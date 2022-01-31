/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the "northbound" code, implementing a VFS-facing
 * superblock and other hooks needed to create a file system instance.
 * This file registers a number of common inode hooks as well.
 */

#include <linux/kernel.h>
#include <linux/namei.h>
#include <linux/slab.h>
#include <linux/buffer_head.h>
#include <linux/parser.h>
#include <linux/list_sort.h>
#include <linux/writeback.h>
#include <linux/path.h>
#include <linux/kallsyms.h>
#include <linux/sched.h>
#include <linux/xattr.h>
#include <linux/blkdev.h>
#include <linux/delay.h>
#include <linux/hashtable.h>
#include <linux/backing-dev.h>
#include <linux/pagemap.h>
#include <linux/gfp.h>
#include <linux/list.h>
#include <linux/bug.h>
#include <linux/bitmap.h>
#include <linux/swap.h>
#include <linux/version.h>
#include <linux/kallsyms.h>

#include "sb_malloc.h"
#include "sb_stat.h"
#include "ftfs.h"
#include "ftfs_northbound.h"
#include "nb_proc_toku_engine_status.h"
#include "nb_proc_toku_checkpoint.h"
#include "nb_proc_toku_flusher.h"
#include "nb_proc_toku_memleak_detect.h"
#include "nb_proc_toku_dump_node.h"
#include "nb_proc_pfn.h"
#include "ftfs_indirect.h"

typedef int (*isolate_lru_page_t)(struct page *);
DECLARE_SYMBOL_FTFS(isolate_lru_page);
int resolve_nb_super_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(isolate_lru_page);
	return 0;
}

static struct ftfs_sb_info *g_sbi;
static inline bool nb_is_rotational(void)
{
	BUG_ON(!g_sbi);
	return g_sbi->is_rotational;
}

bool toku_need_compression(void)
{
	return nb_is_rotational();
}

/*
 * Use to debug page allocation and free when page sharing
 * is enabled and memleak debug is enabled.
 */
#if defined (FT_INDIRECT) && defined(TOKU_MEMLEAK_DETECT)
void debug_ftfs_free_page(void *p);
#else
#define debug_ftfs_free_page(p) do {} while(0)
#endif

/*
 * We use private flag to denote the page is
 * shared between page cache and ft layer
 */
static bool nb_is_shared(struct page *page)
{
#ifdef FT_INDIRECT
	return PagePrivate(page);
#else
	return false;
#endif
}

struct ftfs_indirect_val *ftfs_alloc_msg_val(void) {
	struct ftfs_indirect_val *ptr;
	int size= sizeof(struct ftfs_indirect_val);

	ptr = sb_malloc(size);
	return ptr;
}

void ftfs_fill_msg_val(struct ftfs_indirect_val *msg_val,
		       unsigned long pfn, unsigned int size)
{
	msg_val->pfn = pfn;
	msg_val->size = size;
}

void ftfs_free_msg_val(void *ptr)
{
	sb_free(ptr);
}
/* For page sharing end */

static char root_meta_key[] = "";

static struct kmem_cache *ftfs_inode_cachep;

/* For mounting/umounting purposes */
extern int toku_ydb_init(void);
long txn_count = 0;
long seq_count = 0;
long non_seq_count = 0;
extern void printf_count_blindwrite(void);

/* What device the filesystem is mounted on */
static bool is_hdd;
bool ftfs_is_hdd(void)
{
	return is_hdd;
}

/*
 * ftfs_nb_i_init_once is passed to kmem_cache_create
 * Once an inode is allocated, this function is called to init that inode
 */
static void ftfs_nb_i_init_once(void *inode)
{
	struct ftfs_inode *ftfs_inode = inode;

	dbt_init(&ftfs_inode->meta_dbt);

	inode_init_once(&ftfs_inode->vfs_inode);
}

static void
ftfs_nb_setup_metadata(struct ftfs_metadata *meta, umode_t mode,
		       loff_t size, dev_t rdev, ino_t ino)
{
	struct timespec now_tspec;
	time_t now;

	now_tspec = current_kernel_time();
	TIMESPEC_TO_TIME_T(now, now_tspec);

	meta->st.st_dev = ftfs_vfs->mnt_sb->s_dev;
	meta->st.st_ino = ino;
	meta->st.st_mode = mode;
	meta->st.st_nlink = 1;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99) || defined(CONFIG_UIDGID_STRICT_TYPE_CHECKS)
	meta->st.st_uid = current_uid().val;
	meta->st.st_gid = current_gid().val;
#else /* LINUX_VERSION_CODE >= 4.19.99 || CONFIG_UIDGID_STRICT_TYPE_CHECKS */
	meta->st.st_uid = current_uid();
	meta->st.st_gid = current_gid();
#endif /* LINUX_VERSION_CODE >= 4.19.99 || CONFIG_UIDGID_STRICT_TYPE_CHECKS */
	meta->st.st_rdev = rdev;
	meta->st.st_size = size;
	meta->st.st_blksize = FTFS_BSTORE_BLOCKSIZE;
	meta->st.st_blocks = nb_get_block_num_by_size(size);
	meta->st.st_atime = now;
	meta->st.st_mtime = now;
	meta->st.st_ctime = now;
}

static void
nb_copy_metadata_from_inode(struct ftfs_metadata *meta, struct inode *inode)
{
	meta->st.st_dev = inode->i_sb->s_dev;
	meta->st.st_ino = inode->i_ino;
	meta->st.st_mode = inode->i_mode;
	meta->st.st_nlink = inode->i_nlink;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99) || defined(CONFIG_UIDGID_STRICT_TYPE_CHECKS)
	meta->st.st_uid = inode->i_uid.val;
	meta->st.st_gid = inode->i_gid.val;
#else /* LINUX_VERSION_CODE >= 4.19.99 || CONFIG_UIDGID_STRICT_TYPE_CHECKS */
	meta->st.st_uid = inode->i_uid;
	meta->st.st_gid = inode->i_gid;
#endif /* LINUX_VERSION_CODE >= 4.19.99 || CONFIG_UIDGID_STRICT_TYPE_CHECKS */

	meta->st.st_rdev = inode->i_rdev;
	meta->st.st_size = i_size_read(inode);
	meta->st.st_blksize = FTFS_BSTORE_BLOCKSIZE;
	meta->st.st_blocks = nb_get_block_num_by_size(meta->st.st_size);
	TIMESPEC_TO_TIME_T(meta->st.st_atime, inode->i_atime);
	TIMESPEC_TO_TIME_T(meta->st.st_mtime, inode->i_mtime);
	TIMESPEC_TO_TIME_T(meta->st.st_ctime, inode->i_ctime);
}

// get the next available (unused ino)
// we alloc some ino to each cpu, if more are needed, we will do update_ino
static int nb_next_ino(struct ftfs_sb_info *sbi, ino_t *ino)
{
	int ret = 0;
	unsigned int cpu;
	ino_t new_max;
	DB_TXN *txn;

	new_max = 0;
	cpu = get_cpu();
	*ino = per_cpu_ptr(sbi->s_ftfs_info, cpu)->next_ino;
	if (*ino >= per_cpu_ptr(sbi->s_ftfs_info, cpu)->max_ino) {
		// we expand for all cpus here, it is lavish
		// we can't do txn while holding cpu
		new_max = per_cpu_ptr(sbi->s_ftfs_info, cpu)->max_ino +
		          sbi->s_nr_cpus * FTFS_INO_INC;
		per_cpu_ptr(sbi->s_ftfs_info, cpu)->max_ino = new_max;
	}
	per_cpu_ptr(sbi->s_ftfs_info, cpu)->next_ino += sbi->s_nr_cpus;
	put_cpu();

	if (new_max) {
		TXN_GOTO_LABEL(retry);
		nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
		ret = nb_bstore_update_ino(sbi->meta_db, txn, new_max);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			nb_bstore_txn_abort(txn);
			// we already updated max_cpu, if we get error here
			//  it is hard to go back
			BUG();
		}
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	return ret;
}

int alloc_child_meta_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt, const char *name)
{
	char *parent_key = parent_dbt->data;
	char *meta_key;
	size_t size;
	char *last_slash;

	if ((nb_key_path(parent_key))[0] == '\x00')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	meta_key = sb_malloc(size);
	if (meta_key == NULL)
		return -ENOMEM;
	if ((nb_key_path(parent_key))[0] == '\x00') {
		sprintf(nb_key_path(meta_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(nb_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(nb_key_path(meta_key), nb_key_path(parent_key),
		       last_slash - nb_key_path(parent_key));
		sprintf(nb_key_path(meta_key) + (last_slash - nb_key_path(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
	}

	dbt_setup(dbt, meta_key, size);
	return 0;
}

int alloc_meta_dbt_prefix(DBT *prefix_dbt, DBT *meta_dbt)
{
	char *meta_key = meta_dbt->data;
	char *prefix_key;
	size_t size;
	char *last_slash;

	if ((nb_key_path(meta_key))[0] == '\x00')
		size = meta_dbt->size;
	else
		size = meta_dbt->size - 1;
	prefix_key = sb_malloc(size);
	if (prefix_key == NULL)
		return -ENOMEM;
	if ((nb_key_path(meta_key))[0] == '\x00') {
		(nb_key_path(prefix_key))[0] = '\x00';
	} else {
		last_slash = strrchr(nb_key_path(meta_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(nb_key_path(prefix_key), nb_key_path(meta_key),
		       last_slash - nb_key_path(meta_key));
		strcpy(nb_key_path(prefix_key) + (last_slash - nb_key_path(meta_key)),
		       last_slash + 1);
	}

	dbt_setup(prefix_dbt, prefix_key, size);
	return 0;
}

static int alloc_meta_dbt_movdir(DBT *old_prefix_dbt, DBT *new_prefix_dbt,
                                 DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	new_key = sb_malloc(size);
	if (new_key == NULL)
		return -ENOMEM;
	sprintf(nb_key_path(new_key), "%s%s", nb_key_path(new_prefix_key),
	        old_key + old_prefix_dbt->size - 1);

	dbt_setup(new_dbt, new_key, size);
	return 0;
}

static int
ftfs_nb_do_unlink(DBT *meta_dbt, DB_TXN *txn, struct inode *inode,
		  struct ftfs_sb_info *sbi)
{
	int ret;
	ret = nb_bstore_meta_del(sbi->meta_db, meta_dbt, txn);
	if (!ret && i_size_read(inode) > 0)
		ret = nb_bstore_trunc(sbi->data_db, meta_dbt, txn, 0, 0);
#ifdef FTFS_XATTR
	if (!ret)
		ret = nb_bstore_xattr_del(sbi->meta_db, meta_dbt, txn);
#endif /* End of XATTR */
	return ret;
}

/*
 * we are not just renaming to files (old->new), we are renaming
 * entire subtrees of files.
 *
 * we lock the whole subtree before rename for exclusive access. for
 * either success or fail, you have to call unlock or else you are
 * hosed
 *
 * only the children are locked not the parent
 */
static int prelock_children_for_rename(struct dentry *object, struct list_head *locked)
{
	struct dentry *this_parent;
	struct list_head *next;
	struct inode *inode;

	this_parent = object;
start:
	if (this_parent->d_sb != object->d_sb)
		goto end;
	inode = this_parent->d_inode;
	if (inode == NULL)
		goto repeat;
	if (this_parent != object) {
		nb_get_write_lock(FTFS_I(inode));
		list_add(&FTFS_I(inode)->rename_locked, locked);
	}
repeat:
	next = this_parent->d_subdirs.next;
resume:
	while (next != &this_parent->d_subdirs) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		this_parent = list_entry(next, struct dentry, d_u.d_child);
#else /* LINUX_VERSION_CODE */
		this_parent = list_entry(next, struct dentry, d_child);
#endif /* LINUX_VERSION_CODE */
		goto start;
	}
end:
	if (this_parent != object) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		next = this_parent->d_u.d_child.next;
#else /* LINUX_VERSION_CODE */
		next = this_parent->d_child.next;
#endif /* LINUX_VERSION_CODE */
		this_parent = this_parent->d_parent;
		goto resume;
	}
	return 0;
}

static int unlock_children_after_rename(struct list_head *locked)
{
	struct ftfs_inode *f_inode, *tmp;

	list_for_each_entry_safe(f_inode, tmp, locked, rename_locked) {
		nb_put_write_lock(f_inode);
		list_del_init(&f_inode->rename_locked);
	}
	return 0;
}

static void
nb_update_ftfs_inode_keys(struct list_head *locked,
                            DBT *old_meta_dbt, DBT *new_meta_dbt)
{
	int ret;
	struct ftfs_inode *f_inode, *tmp;
	DBT tmp_dbt, old_prefix_dbt, new_prefix_dbt;

	if (list_empty(locked))
		return;

	ret = alloc_meta_dbt_prefix(&old_prefix_dbt, old_meta_dbt);
	BUG_ON(ret);
	alloc_meta_dbt_prefix(&new_prefix_dbt, new_meta_dbt);
	BUG_ON(ret);

	list_for_each_entry_safe(f_inode, tmp, locked, rename_locked) {
		dbt_copy(&tmp_dbt, &f_inode->meta_dbt);
		ret = alloc_meta_dbt_movdir(&old_prefix_dbt, &new_prefix_dbt,
		                            &tmp_dbt, &f_inode->meta_dbt);
		BUG_ON(ret);
		dbt_destroy(&tmp_dbt);
	}

	dbt_destroy(&old_prefix_dbt);
	dbt_destroy(&new_prefix_dbt);
}

static int nb_readpage(struct file *file, struct page *page)
{
	int ret;
	struct inode *inode = page->mapping->host;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	DBT *meta_dbt;
	DB_TXN *txn;

	meta_dbt = nb_get_read_lock(FTFS_I(inode));

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = nb_bstore_scan_one_page(sbi->data_db, meta_dbt, txn, page);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	nb_put_read_lock(FTFS_I(inode));

	flush_dcache_page(page);
	if (!ret) {
		SetPageUptodate(page);
	} else {
		ClearPageUptodate(page);
		SetPageError(page);
	}

	unlock_page(page);
	return ret;
}

static int nb_readpages(struct file *filp, struct address_space *mapping,
			struct list_head *pages, unsigned nr_pages)
{
	int ret;
	struct ftfs_sb_info *sbi = mapping->host->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(mapping->host);
	struct ftio *ftio;
	DBT *meta_dbt;
	DB_TXN *txn;

	ftio = ftio_alloc(nr_pages);
	if (!ftio)
		return -ENOMEM;
	ftio_setup(ftio, pages, nr_pages, mapping);

	meta_dbt = nb_get_read_lock(ftfs_inode);

	ftio->last_read_cnt = ftfs_inode->last_nr_pages;
	ftio->last_offset = ftfs_inode->last_offset;

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);

	ret = nb_bstore_scan_pages(sbi->data_db, meta_dbt, txn, ftio);

	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	ftfs_inode->last_nr_pages = nr_pages;
	ftfs_inode->last_offset = ftio_last_page(ftio)->index;

	nb_put_read_lock(ftfs_inode);

	if (ret)
		ftio_set_pages_error(ftio);
	else
		ftio_set_pages_uptodate(ftio);
	ftio_unlock_pages(ftio);
	ftio_free(ftio);

	return ret;
}

static int
__nb_updatepage(struct ftfs_sb_info *sbi, struct inode *inode, DBT *meta_dbt,
		struct page *page, size_t len, loff_t offset, DB_TXN *txn)
{
	int ret;
	char *buf;
	size_t off;
	DBT data_dbt;

	// now data_db keys start from 1
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt,
	                                   PAGE_TO_BLOCK_NUM(page));
	if (ret)
		return ret;
	buf = kmap(page);
	buf = buf + (offset & ~PAGE_MASK);
	off = block_get_off_by_position(offset);
	ret = nb_bstore_update(sbi->data_db, &data_dbt, txn, buf, len, off);
	kunmap(page);
	dbt_destroy(&data_dbt);

	return ret;
}

#ifndef FT_INDIRECT
static int
__nb_writepage(struct ftfs_sb_info *sbi, struct inode *inode, DBT *meta_dbt,
                 struct page *page, size_t len, DB_TXN *txn)
{
	int ret;
	char *buf;
	DBT data_dbt;

	// now data_db keys start from 1
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt,
	                                   PAGE_TO_BLOCK_NUM(page));
	if (ret)
		return ret;
	buf = kmap(page);
	ret = nb_bstore_put(sbi->data_db, &data_dbt, txn, buf, len, 0);
	kunmap(page);
	dbt_destroy(&data_dbt);

	return ret;
}

static int
nb_writepage(struct page *page, struct writeback_control *wbc)
{
	int ret;
	DBT *meta_dbt;
	struct inode *inode = page->mapping->host;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	loff_t i_size;
	pgoff_t end_index;
	unsigned offset;
	DB_TXN *txn;

	meta_dbt = nb_get_read_lock(FTFS_I(inode));
	set_page_writeback(page);
	i_size = i_size_read(inode);
	end_index = i_size >> PAGE_SHIFT;

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	if (page->index < end_index)
		ret = __nb_writepage(sbi, inode, meta_dbt, page, PAGE_SIZE, txn);
	else {
		offset = i_size & (~PAGE_MASK);
		if (page->index == end_index && offset != 0)
			ret = __nb_writepage(sbi, inode, meta_dbt, page, offset, txn);
		else
			ret = 0;
	}
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		if (ret == -EAGAIN) {
			redirty_page_for_writepage(wbc, page);
			ret = 0;
		} else {
			SetPageError(page);
			mapping_set_error(page->mapping, ret);
		}
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}
	end_page_writeback(page);

	nb_put_read_lock(FTFS_I(inode));
	unlock_page(page);

	return ret;
}

#else
/**
 * It is called when the system decides to free dirty pages by writing them back.
 * Since in ftfs, dirty page will not be on LRU list,
 * ftfs_writepage just do nothing. But in the future, we may need to change it.
 */
static int
nb_writepage(struct page *page, struct writeback_control *wbc)
{
	(void)page;
	(void)wbc;
	BUG();
	return 0;
}
#endif

static inline int nb_is_seq(pgoff_t max_index, pgoff_t min_index, int nr_list_pages)
{
	return (max_index - min_index + 1 <= nr_list_pages * 2) ? 1 : 0;
}

/*
 * We implement the this wrapper to keep track the flag of pages.
 * It is to ease debugging. We assume page is locked.
 */
static void nb_set_page_writeback(struct page *page)
{
	if (page->mapping != NULL) {
		BUG_ON(PageWriteback(page));
		BUG_ON(!PageLocked(page));
	}
	set_page_writeback(page);
}

/*
 * We implement the this wrapper to keep track _count of pages.
 * It is to ease debugging. We assume page is locked.
 */
static void nb_pagevec_release(struct pagevec *pvec)
{
	int i;
	struct page* page;
	struct ftfs_page_private *priv;
	bool is_locked = false;

	/*
	 * If we have the last reference to the page
	 * we need to free the private data
	 */
	for (i = 0; i < pagevec_count(pvec); i++) {
		page = pvec->pages[i];
		is_locked =PageLocked(page);
		if (!is_locked) {
			lock_page(page);
		}
		if (ftfs_read_page_count(page) == 1) {
			priv = FTFS_PAGE_PRIV(page);
			BUG_ON(!priv);
			debug_ftfs_free_page(page);
			kfree(priv);
			page->private = 0;
		}
		if (!is_locked) {
			unlock_page(page);
		}
		put_page(page);
	}
	pagevec_reinit(pvec);
}

struct ftfs_wp_node {
	struct page *page;
	struct ftfs_wp_node *next;
};
#define FTFS_WRITEPAGES_LIST_SIZE 4096

static struct kmem_cache *ftfs_writepages_cachep;

/* If we encounter a mmapped page in the writeback code
 * we need to copy this page and insert the new page
 * to ft layer.
 */
static struct page *ftfs_copy_mapped_page(struct page *page)
{
	struct address_space *mapping = page->mapping;
	struct page *copy_page = __ftfs_page_cache_alloc(mapping_gfp_mask(mapping));
	char *src_buf;
	char *dest_buf;
	preempt_disable();
	src_buf = kmap_atomic(page);
	dest_buf = kmap_atomic(copy_page);
	memcpy(dest_buf, src_buf, PAGE_SIZE);
	kunmap_atomic(src_buf);
	kunmap_atomic(dest_buf);
	preempt_enable();
	return copy_page;
}

/*
 * Uses a transaction to write back a list of dirty pages.
 * Each for loop iterates through the list and writes it page by page.
 */
static int
__nb_writepages_write_pages(struct ftfs_wp_node *list, int nr_pages,
                              struct writeback_control *wbc,
                              struct inode *inode, struct ftfs_sb_info *sbi,
                              DBT *data_dbt, int is_seq)
{
	int i, ret = 0;
	loff_t i_size;
	pgoff_t end_index;
	unsigned offset;
	char *buf;
	struct ftfs_wp_node *it;
	struct page *page;
	DBT *meta_dbt;
	char *data_key;
	DB_TXN *txn;
	struct ftfs_indirect_val *msg_val;
	struct page *copy_page;
#ifndef FT_INDIRECT
	bool has_page_sharing = false;
#else
	bool has_page_sharing = true;
#endif
	meta_dbt = nb_get_read_lock(FTFS_I(inode));
	data_key = data_dbt->data;
	if (unlikely(!key_is_same_of_key((char *)meta_dbt->data, data_key)))
		copy_data_dbt_from_meta_dbt(data_dbt, meta_dbt, 0);
retry:
	i_size = i_size_read(inode);
	end_index = i_size >> PAGE_SHIFT;
	offset = i_size & (PAGE_SIZE - 1);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	// we did a lazy approach about the list, so we need an additional i here
	for (i = 0, it = list->next; i < nr_pages; i++, it = it->next) {
		page = it->page;
		/* Case 1: If page_sharing is not enabled or we are not doing seq write
		 * use the old code to write back the page to ft layer
		 */
		if (!has_page_sharing || !is_seq) {
			nb_data_key_set_blocknum(data_key, data_dbt->size,
						 PAGE_TO_BLOCK_NUM(page));
			buf = kmap(page);
			if (page->index < end_index)
				ret = nb_bstore_put(sbi->data_db, data_dbt, txn, buf,
						    PAGE_SIZE, is_seq);
			else if (page->index == end_index && offset != 0)
				ret = nb_bstore_put(sbi->data_db, data_dbt, txn, buf,
						    offset, is_seq);
			else
				ret = 0;
			kunmap(page);
			goto txn_end;
		}
		/* Case 2: page sharing is enabled and seq write is detected */

		/* LRU page is inserted again. It happens for OLTP benchmark */
		if (PageLRU(page)) {
			/* isolate the page from LRU list */
			ftfs_isolate_lru_page(page);
			/* attach private data to page->private */
			if (!FTFS_PAGE_PRIV(page)) {
				struct ftfs_page_private *priv;
				priv = kzalloc(sizeof(*priv), GFP_KERNEL);
				BUG_ON(priv == NULL);
				page->private = (unsigned long)priv;
				debug_ftfs_alloc_page(page, PAGE_MAGIC);
			}
		}
		if (!PageLocked(page) || !PageWriteback(page)) {
			printk("%s: Wrong page state\n", __func__);
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}

		if (page->index == end_index && offset != 0 && offset <= FTFS_COPY_VAL_THRESHOLD) {
			char *buf;
			nb_data_key_set_blocknum(data_key, data_dbt->size, page->index + 1);
			buf = kmap(page);
			ret = nb_bstore_put(sbi->data_db, data_dbt, txn, buf, offset, false);
			kunmap(page);
			end_page_writeback(page);
			unlock_page(page);
		} else if (page->index > end_index || (page->index == end_index && offset == 0)) {
			/* When is page is out-of-range, we just ignore this page
			 * `page->index > end_index` means the page's offset is larger than  the file
			 * size; `page->index == end_index && offset == 0` means the last page has nothing
			 * to write and can be ignored as well
			 */
			end_page_writeback(page);
			unlock_page(page);
			ret = 0;
		} else {
			unsigned long insert_pfn;
			if (page_mapped(page)) {
				copy_page = ftfs_copy_mapped_page(page);
				ftfs_set_page_private(page_to_pfn(copy_page), FT_MSG_VAL_NB_WRITE);
				ftfs_inc_page_private_count(copy_page);
				insert_pfn = page_to_pfn(copy_page);
			} else {
				SetPagePrivate(page);
				get_page(page);
				ftfs_set_page_private(page_to_pfn(page), FT_MSG_VAL_NB_WRITE);
				insert_pfn = page_to_pfn(page);
#ifdef FT_PAGE_DEBUG
				just_inserted_pfn = insert_pfn;
#endif
			}
			/* Prepare the value */
			msg_val = ftfs_alloc_msg_val();
			BUG_ON(msg_val == NULL);
			if (page->index < end_index) {
				/* full page */
				ftfs_fill_msg_val(msg_val, insert_pfn, PAGE_SIZE);
			} else {
				/* odded-size page --- offset is the length of the page */
				ftfs_fill_msg_val(msg_val, insert_pfn, offset);
			}
#ifdef FT_INDIRECT_DEBUG
			{
				char *buf = kmap(page);
				memcpy(buf, &page->index, sizeof(page->index));
				kunmap(page);
			}
#endif
			/* Prepare the key */
			nb_data_key_set_blocknum(
					data_key,
					data_dbt->size,
					page->index + 1);
			/* Insert key-value pair */
			ret = nb_bstore_put_indirect_page(
					sbi->data_db,
					data_dbt,
					txn,
					msg_val);
			ftfs_free_msg_val(msg_val);
			if (page_mapped(page)) {
				end_page_writeback(page);
				unlock_page(page);
			} else {
				if (PageWriteback(page)) {
					/* We definitely clear up wb flag.
					 * Ft code also locks page.
					 * We do not test lock flag
					 */
					ftfs_print_page(page_to_pfn(page));
					BUG();
				}
			}
		}
txn_end:
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			nb_bstore_txn_abort(txn);
			goto out;
		}
	}
	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
out:
	if (!has_page_sharing || !is_seq) {
		/* Case 1: unlock all pages and change writeback flag */
		nb_put_read_lock(FTFS_I(inode));
		for (i = 0, it = list->next; i < nr_pages; i++, it = it->next) {
			page = it->page;
			end_page_writeback(page);
			if (ret)
				redirty_page_for_writepage(wbc, page);
			unlock_page(page);
		}
	} else {
		/* Case 2: page is unlocked and writeback flag is changed within
		 * the txn. Only do some sanity check here.
		 */
		if (i != nr_pages || ret != 0) {
			printk("i=%d, nr_pages=%d, ret=%d\n", i, nr_pages, ret);
			BUG();
		}
		nb_put_read_lock(FTFS_I(inode));
	}
	return ret;
}

/**
 * (mostly) copied from write_cache_pages
 *
 * however, instead of calling mm/page-writeback.c:__writepage, we
 * detect large I/Os and potentially issue a special seq_put to our
 * B^e tree
 */
static int nb_writepages(struct address_space *mapping,
			struct writeback_control *wbc)
{
	int i, ret = 0;
	int done = 0;
	struct pagevec pvec;
	int nr_pages;
	pgoff_t uninitialized_var(writeback_index);
	pgoff_t index;
	pgoff_t end;		/* Inclusive */
	pgoff_t done_index, txn_done_index;
	int cycled;
	int range_whole = 0;
	int tag;
	int is_seq = 0;
	struct inode *inode;
	struct ftfs_sb_info *sbi;
	DBT *meta_dbt, data_dbt;
	int nr_list_pages;
	struct ftfs_wp_node list, *tail, *it;
	pgoff_t max_index;
	pgoff_t min_index;

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	pagevec_init(&pvec, 0);
#else /* LINUX_VERSION_CODE */
	pagevec_init(&pvec);
#endif /* LINUX_VERSION_CODE */
	if (wbc->range_cyclic) {
		writeback_index = mapping->writeback_index; /* prev offset */
		index = writeback_index;
		if (index == 0)
			cycled = 1;
		else
			cycled = 0;
		end = -1;
	} else {
		index = wbc->range_start >> PAGE_SHIFT;
		end = wbc->range_end >> PAGE_SHIFT;
		if (wbc->range_start == 0 && wbc->range_end == LLONG_MAX)
			range_whole = 1;
		cycled = 1; /* ignore range_cyclic tests */
	}
	if (wbc->sync_mode == WB_SYNC_ALL || wbc->tagged_writepages)
		tag = PAGECACHE_TAG_TOWRITE;
	else
		tag = PAGECACHE_TAG_DIRTY;
retry:
	if (wbc->sync_mode == WB_SYNC_ALL || wbc->tagged_writepages)
		tag_pages_for_writeback(mapping, index, end);
	done_index = index;
	txn_done_index = index;

	inode = mapping->host;
	sbi = inode->i_sb->s_fs_info;
	meta_dbt = nb_get_read_lock(FTFS_I(inode));
	ret = dbt_alloc(&data_dbt, meta_dbt->size + DATA_META_KEY_SIZE_DIFF);
	if (ret) {
		nb_put_read_lock(FTFS_I(inode));
		goto out;
	}
	copy_data_dbt_from_meta_dbt(&data_dbt, meta_dbt, 0);
	nb_put_read_lock(FTFS_I(inode));

	nr_list_pages = 0;
	max_index = 0;
	min_index = LLONG_MAX;
	list.next = NULL;
	tail = &list;
	while (!done && (index <= end)) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		nr_pages = pagevec_lookup_tag(&pvec, mapping, &index, tag,
			      min(end - index, (pgoff_t)PAGEVEC_SIZE-1) + 1);
#else /* LINUX_VERSION_CODE */
		nr_pages = pagevec_lookup_range_tag(&pvec, mapping, &index, end, tag);
#endif /* LINUX_VERSION_CODE */
		if (nr_pages == 0)
			break;

		for (i = 0; i < nr_pages; i++) {
			struct page *page = pvec.pages[i];

			if (page->index > end) {
				done = 1;
				break;
			}

			txn_done_index = page->index;
			lock_page(page);
			if (unlikely(page->mapping != mapping) || nb_is_shared(page)) {
continue_unlock:
				unlock_page(page);
				continue;
			}

			if (!PageDirty(page)) {
				/* someone wrote it for us */
				goto continue_unlock;
			}

			if (PageWriteback(page)) {
				if (wbc->sync_mode != WB_SYNC_NONE)
					wait_on_page_writeback(page);
				else
					goto continue_unlock;
			}
#ifdef DEBUG_READ_WRITE
			{
				char *buf;
				preempt_disable();
				buf = kmap_atomic(page);
				memcpy(buf, &page->index, sizeof(page->index));
				kunmap_atomic(buf);
				preempt_enable();
			}
#endif
			BUG_ON(PageWriteback(page));
			if (!clear_page_dirty_for_io(page))
				goto continue_unlock;
			nb_set_page_writeback(page);
			if (tail->next == NULL) {
				tail->next = kmem_cache_alloc(
					ftfs_writepages_cachep, GFP_KERNEL);
				tail->next->next = NULL;
			}
			tail = tail->next;
			tail->page = page;
			++nr_list_pages;
			max_index = max(max_index, page->index);
			min_index = min(min_index, page->index);
			if (nr_list_pages >= FTFS_WRITEPAGES_LIST_SIZE) {
				is_seq = nb_is_seq(max_index, min_index, nr_list_pages);
				ret = __nb_writepages_write_pages(&list,
					nr_list_pages, wbc, inode, sbi,
					&data_dbt, is_seq);
				if (ret)
					goto free_dkey_out;
				done_index = txn_done_index;
				nr_list_pages = 0;
				max_index = 0;
				min_index = LLONG_MAX;
				tail = &list;
			}

			if (--wbc->nr_to_write <= 0 &&
			    wbc->sync_mode == WB_SYNC_NONE) {
				done = 1;
				break;
			}
		}
		nb_pagevec_release(&pvec);
		cond_resched();
	}

	if (nr_list_pages > 0) {
		is_seq = nb_is_seq(max_index, min_index, nr_list_pages);
		ret = __nb_writepages_write_pages(&list, nr_list_pages, wbc,
						  inode, sbi, &data_dbt, is_seq);
		if (!ret)
			done_index = txn_done_index;
	}
free_dkey_out:
	dbt_destroy(&data_dbt);
	tail = list.next;
	while (tail != NULL) {
		it = tail->next;
		kmem_cache_free(ftfs_writepages_cachep, tail);
		tail = it;
	}
out:
	if (!cycled && !done) {
		cycled = 1;
		index = 0;
		end = writeback_index - 1;
		goto retry;
	}
	if (wbc->range_cyclic || (range_whole && wbc->nr_to_write > 0))
		mapping->writeback_index = done_index;

	return ret;
}

#ifdef FT_INDIRECT
/* We are trying to find a page, if shared, we copy
 * so that we can start writing right away
 * (while old version is still in writeback)
 */
struct page *ftfs_grab_cache_page_write_begin(
		struct address_space *mapping,
		pgoff_t index, unsigned flags)
{
	int status;
	struct page *page;
	struct page *cow_page;
	gfp_t gfp_mask;
	gfp_t gfp_notmask = 0;

	gfp_mask = mapping_gfp_mask(mapping);
	if (mapping_cap_account_dirty(mapping))
		gfp_mask |= __GFP_WRITE;

	page = find_lock_page(mapping, index);
	if (page && nb_is_shared(page)) {
		/* Found shared page. We do copy on write
		 * to break the sharing.
		 */
		if (page_mapped(page)) {
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
		cow_page = ftfs_cow_page(page, mapping,
					 index, gfp_mask,
					 gfp_notmask);
		/* unlock the old page and dec its refcount */
		unlock_page(page);
		put_page(page);
		BUG_ON(nb_is_shared(cow_page));
		/* Issue a RCU read here */
		page = find_get_page(mapping, index);
		/* The output of RCU read is the same with copy-on-write output */
		BUG_ON(cow_page != page);
		/* After write_begin function page->_count should be 2 */
		put_page(cow_page);
	} else if (page && !nb_is_shared(page)) {
		/* Do we need to wait for shared page undergoing IO?
		 * In our case, writeback flag is protected by lock_page.
		 * Since we get the lock here, page cannot have writeback
		 * flag. Since it is not shared, it cannot happen for page
		 * read from ft code. Therefore, this page is just created
		 * by a write and writeback hasn't happen yet.
                 */
		BUG_ON(PageReserved(page));
		BUG_ON(PageWriteback(page));
	} else {
		/* Page cache miss, we need to allocate a page */
		page = __ftfs_page_cache_alloc(gfp_mask & ~gfp_notmask);
		if (!page) {
			printk("alloc page cache page failed\n");
			BUG();
		}

		status = add_to_page_cache(page, mapping, index,
				   GFP_KERNEL & ~gfp_notmask);
		if (unlikely(status)) {
			printk("insert page to page cache failed\n");
			BUG();
		}
		/* post-checking the refcount */
		BUG_ON(ftfs_read_page_count(page) != 2);
	}
	return page;
}
#endif

static int
nb_write_begin(struct file *file, struct address_space *mapping,
                 loff_t pos, unsigned len, unsigned flags,
                 struct page **pagep, void **fsdata)
{
	int ret = 0;
	struct page *page;
	pgoff_t index = pos >> PAGE_SHIFT;
#ifdef FT_INDIRECT
	page = ftfs_grab_cache_page_write_begin(mapping, index, flags);
#else
	page = grab_cache_page_write_begin(mapping, index, flags);
#endif
	if (!page)
		ret = -ENOMEM;
	/* don't read page if not uptodate */

	*pagep = page;
	return ret;
}

#define FTFS_SHORT_BLOCK_SIZE 256

static int
nb_write_end(struct file *file, struct address_space *mapping,
               loff_t pos, unsigned len, unsigned copied,
               struct page *page, void *fsdata)
{
	/* make sure that ftfs can't guarantee uptodate page */
	loff_t last_pos = pos + copied;
	struct inode *inode = page->mapping->host;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	loff_t i_size = i_size_read(inode);
	DBT *meta_dbt;
	char *buf;
	int ret;
	DB_TXN *txn;

	/*
	 * 1. if page is uptodate/writesize=PAGE_SIZE (we have new content),
	 *    write to page cache and wait for generic_writepage to write
	 *    to disk (generic aio style);
	 * 2. if not, only write to disk so that we avoid read-before-write.
	 */
	if (PageDirty(page) || copied == PAGE_SIZE) {
		goto postpone_to_writepage;
	} else if (page_offset(page) >= i_size) {
		preempt_disable();
		buf = kmap_atomic(page);
		if (pos & ~PAGE_MASK)
			memset(buf, 0, pos & ~PAGE_MASK);
		if (last_pos & ~PAGE_MASK)
			memset(buf + (last_pos & ~PAGE_MASK), 0,
			       PAGE_SIZE - (last_pos & ~PAGE_MASK));
		kunmap_atomic(buf);
		preempt_enable();
postpone_to_writepage:
		SetPageUptodate(page);
		if (!PageDirty(page))
			__set_page_dirty_nobuffers(page);
	} else {
		meta_dbt = nb_get_read_lock(FTFS_I(inode));
		TXN_GOTO_LABEL(retry);
		nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
		ret = __nb_updatepage(sbi, inode, meta_dbt, page, copied, pos,
				      txn);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			nb_bstore_txn_abort(txn);
		} else {
			ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
			COMMIT_JUMP_ON_CONFLICT(ret, retry);
		}

		nb_put_read_lock(FTFS_I(inode));
		BUG_ON(ret);
		clear_page_dirty_for_io(page);
	}
	unlock_page(page);
	put_page(page);

	/* holding i_mutconfigex */
	if (last_pos > i_size_read(inode)) {
		i_size_write(inode, last_pos);
		mark_inode_dirty(inode);
	}

	return copied;
}

/* Called before freeing a page - it writes back the dirty page.
 *
 * To prevent redirtying the page, it is kept locked during the whole
 * operation.
 */
static int nb_launder_page(struct page *page)
{
	printk(KERN_CRIT "laundering page.\n");
	BUG();
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
static int nb_rename(struct inode *old_dir, struct dentry *old_dentry,
                       struct inode *new_dir, struct dentry *new_dentry)
#else /* LINUX_VERSION_CODE */
static int nb_rename(struct inode *old_dir, struct dentry *old_dentry,
                       struct inode *new_dir, struct dentry *new_dentry,
		       unsigned int flags)
#endif /* LINUX_VERSION_CODE */
{
	int ret, drop_newdir_count;
#ifdef FTFS_EMPTY_DIR_VERIFY
	int err, r;
#endif
	struct inode *old_inode, *new_inode;
	struct ftfs_sb_info *sbi = old_dir->i_sb->s_fs_info;
	DBT *old_meta_dbt, new_meta_dbt, *old_dir_meta_dbt;
	DBT *new_dir_meta_dbt, *new_inode_meta_dbt;
	struct ftfs_metadata old_meta;
	LIST_HEAD(locked_children);
	DB_TXN *txn;

	// to prevent any other move from happening, we grab sem of parents
	old_dir_meta_dbt = nb_get_read_lock(FTFS_I(old_dir));
	new_dir_meta_dbt = nb_get_read_lock(FTFS_I(new_dir));

	old_inode = old_dentry->d_inode;
	old_meta_dbt = nb_get_write_lock(FTFS_I(old_inode));
	new_inode = new_dentry->d_inode;
	new_inode_meta_dbt = new_inode ?
		nb_get_write_lock(FTFS_I(new_inode)) : NULL;
	prelock_children_for_rename(old_dentry, &locked_children);

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	drop_newdir_count=0;
	if (new_inode) {
		if (S_ISDIR(old_inode->i_mode)) {
			if (!S_ISDIR(new_inode->i_mode)) {
				ret = -ENOTDIR;
				goto abort;
			}

			ret = new_inode->i_nlink-2;
#ifdef FTFS_EMPTY_DIR_VERIFY
			err =  nb_dir_is_empty(sbi->meta_db, new_inode_meta_dbt, txn, &r);
			if (err) {
				DBOP_JUMP_ON_CONFLICT(err, retry);
				ret = err;
				goto abort;
			}
			if (!(ret ==0 && r==1 || ret>0 && r==0)) { //r is: dir is isempty?
				//dir inode has invalid count
				BUG();
			}
#endif

			if (ret) {  //enter if not empty
				ret = -ENOTEMPTY;
				goto abort;
			}

			drop_newdir_count=1;
			// there will be a put later, so we don't need to
			// delete meta here. but if it is a circle root,
			// we need to perform delete and avoid future update
			// to that inode.
			// For non-circle case, there will be no dir circle
			// root;
		} else {
			if (S_ISDIR(new_inode->i_mode)) {
				ret = -ENOTDIR;
				goto abort;
			}
			// if this file is a circle root, evict_inode can decide
			//   whether it should be deleted
			// otherwise, we cannot let this inode touch the path
			//   any more because another inode owns that path. So
			//   we need to delete here
			ret = ftfs_nb_do_unlink(new_inode_meta_dbt, txn,
			                     new_inode, sbi);

			drop_newdir_count=1;

			if (ret) {
				DBOP_JUMP_ON_CONFLICT(ret, retry);
				goto abort;
			}
		}
	}

	ret = alloc_child_meta_dbt_from_meta_dbt(&new_meta_dbt,
						 new_dir_meta_dbt, new_dentry->d_name.name);
	if (ret)
		goto abort;

	nb_copy_metadata_from_inode(&old_meta, old_inode);
	ret = nb_bstore_meta_del(sbi->meta_db, old_meta_dbt, txn);
	if (!ret)
		ret = nb_bstore_meta_put(sbi->meta_db, &new_meta_dbt, txn, &old_meta);
	if (!ret)
		ret = nb_bstore_move(sbi->meta_db, sbi->data_db,
				     old_meta_dbt, &new_meta_dbt, txn,
				     nb_bstore_get_move_type(&old_meta));
#ifdef FTFS_XATTR
	if (!ret) {
		ret = nb_bstore_xattr_move(sbi->meta_db, old_meta_dbt, &new_meta_dbt, txn);
	}
#endif /* End of FTFS_XATTR */
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort1;
	}

	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	nb_update_ftfs_inode_keys(&locked_children, old_meta_dbt,
				  &new_meta_dbt);
	dbt_destroy(&FTFS_I(old_inode)->meta_dbt);
	dbt_copy(&FTFS_I(old_inode)->meta_dbt, &new_meta_dbt);

	unlock_children_after_rename(&locked_children);

	drop_nlink(old_dir);
	inc_nlink(new_dir);

	if (drop_newdir_count == 1) {
		drop_nlink(new_dir);
	}

	mark_inode_dirty(old_dir);
	mark_inode_dirty(new_dir);

	if (new_inode) {
		clear_nlink(new_inode);
		mark_inode_dirty(new_inode);
		// avoid future updates from write_inode and evict_inode
		FTFS_I(new_inode)->ftfs_flags |= FTFS_FLAG_DELETED;
		nb_put_write_lock(FTFS_I(new_inode));
	}
	nb_put_write_lock(FTFS_I(old_inode));
	nb_put_read_lock(FTFS_I(old_dir));
	nb_put_read_lock(FTFS_I(new_dir));

	return 0;

abort1:
	dbt_destroy(&new_meta_dbt);
abort:
	nb_bstore_txn_abort(txn);
	unlock_children_after_rename(&locked_children);
	nb_put_write_lock(FTFS_I(old_inode));
	if (new_inode)
		nb_put_write_lock(FTFS_I(new_inode));
	nb_put_read_lock(FTFS_I(old_dir));
	nb_put_read_lock(FTFS_I(new_dir));

	return ret;
}

/*
 * nb_readdir: ctx->pos (vfs get from f_pos)
 *   ctx->pos == 0, readdir just starts
 *   ctx->pos == 1/2, readdir has emit dots, used by dir_emit_dots
 *   ctx->pos == 3, readdir has emit all entries
 *   ctx->pos == ?, ctx->pos stores a pointer to the position of last readdir
 */
static int nb_readdir(struct file *file, struct dir_context *ctx)
{
	int ret;
	struct inode *inode = file_inode(file);
	struct dentry *parent = file->f_path.dentry;
	struct super_block *sb = inode->i_sb;
	struct ftfs_sb_info *sbi = sb->s_fs_info;
	DBT *meta_dbt;
	DB_TXN *txn;

	// done
	if (ctx->pos == 3)
		return 0;

	if (ctx->pos == 0) {
		if (!dir_emit_dots(file, ctx))
			return -ENOMEM;
		ctx->pos = 2;
	}

	meta_dbt = nb_get_read_lock(FTFS_I(inode));

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = nb_bstore_meta_readdir(sb, parent, sbi->meta_db, meta_dbt, txn, ctx);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	nb_put_read_lock(FTFS_I(inode));

	return ret;
}

static int
nb_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	int ret;
	struct ftfs_sb_info *sbi = file_inode(file)->i_sb->s_fs_info;
	ret = generic_file_fsync(file, start, end, datasync);
	if (!ret)
		ret = nb_bstore_flush_log(sbi->db_env);

	return ret;
}

static int
nb_mknod(struct inode *dir, struct dentry *dentry, umode_t mode, dev_t rdev)
{
	int ret;
	struct inode *inode = NULL;
	struct ftfs_metadata meta;
	struct ftfs_sb_info *sbi = dir->i_sb->s_fs_info;
	DBT *dir_meta_dbt, meta_dbt;
	ino_t ino;
	DB_TXN *txn;

	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));
	ret = alloc_child_meta_dbt_from_meta_dbt(&meta_dbt, dir_meta_dbt,
	                                         dentry->d_name.name);
	if (ret)
		goto out;

	ret = nb_next_ino(sbi, &ino);
	if (ret) {
err_free_dbt:
		dbt_destroy(&meta_dbt);
		goto out;
	}

	ftfs_nb_setup_metadata(&meta, mode, 0, rdev, ino);
	inode = nb_setup_inode(dir->i_sb, &meta_dbt, &meta);
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		goto err_free_dbt;
	}
	if ((mode | S_IFDIR ) == mode) {
		inc_nlink(inode);
	}

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = nb_bstore_meta_put(sbi->meta_db, &meta_dbt, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		set_nlink(inode, 0);
		FTFS_I(inode)->ftfs_flags |= FTFS_FLAG_DELETED;
		dbt_destroy(&FTFS_I(inode)->meta_dbt);
		iput(inode);
		goto out;
	}
	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	inc_nlink(dir);
	mark_inode_dirty(dir);

	d_instantiate(dentry, inode);
	mark_inode_dirty(inode);

out:
	nb_put_read_lock(FTFS_I(dir));
	return ret;
}

static int
nb_create(struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
	return nb_mknod(dir, dentry, mode | S_IFREG, 0);
}

static int nb_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode)
{
	return nb_mknod(dir, dentry, mode | S_IFDIR, 0);
}

static int ftfs_dir_delete(struct ftfs_sb_info *sbi, DBT *meta_dbt)
{
	DB_TXN *txn;
	int ret;

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	// Issue a range delete for pacman
	ret = dir_delete(meta_dbt, sbi->data_db, txn);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		goto out;
	}
	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
out:
	return ret;
}

static int nb_rmdir(struct inode *dir, struct dentry *dentry)
{
	int r2;
	int r, ret, x;
	int i_nlink;

	struct inode *inode = dentry->d_inode;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	DBT *meta_dbt, *dir_meta_dbt;

#ifdef FTFS_EMPTY_DIR_VERIFY
	DB_TXN *txn;
#endif
	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));
	meta_dbt = nb_get_read_lock(ftfs_inode);

	if (meta_dbt->data == &root_meta_key) {
		ret = -EINVAL;
		goto out;
	}
#ifdef FTFS_EMPTY_DIR_VERIFY
	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = nb_dir_is_empty(sbi->meta_db, meta_dbt, txn, &r);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		goto out;
	}

	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	ret = inode->i_nlink-2;
	if (!(ret ==0 && r==1 || ret>0 && r==0)) { //r is: dir is isempty?
		BUG();
	}
#endif

#ifdef RMDIR_RANGE_DELETE
	// Issue a range delete for pacman
	// Using a new transaction for it.
	// Otherwise, nb_dir_is_empty's row lock
	// can not be released earlier and causes deadlock
	// for multi-thead programs, like xfs 074
	r2 = ftfs_dir_delete(sbi, meta_dbt);
	BUG_ON(r2);
#endif

	i_nlink = inode->i_nlink;

	if (i_nlink != 2) {
		ret = -ENOTEMPTY;
	}
	else {
		r = inode->i_nlink;
		x = dir->i_nlink;
		clear_nlink(inode);
		ret = dir->i_nlink;
		drop_nlink(dir);
		mark_inode_dirty(inode);
		mark_inode_dirty(dir);
		ret = 0;
	}


out:
	nb_put_read_lock(ftfs_inode);
	nb_put_read_lock(FTFS_I(dir));
	return ret;
}

static int
nb_symlink(struct inode *dir, struct dentry *dentry, const char *symname)
{
	int ret;
	struct inode *inode;
	struct ftfs_sb_info *sbi = dir->i_sb->s_fs_info;
	struct ftfs_metadata meta;
	DBT *dir_meta_dbt, meta_dbt;
	DBT data_dbt;
	size_t len = strlen(symname);
	ino_t ino;
	DB_TXN *txn;

	if (len > FTFS_BSTORE_BLOCKSIZE)
		return -ENAMETOOLONG;

	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));
	ret = alloc_child_meta_dbt_from_meta_dbt(&meta_dbt,
			dir_meta_dbt, dentry->d_name.name);
	if (ret)
		goto out;

	// now we start from 1
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, &meta_dbt, 1);
	if (ret) {
free_meta_out:
		dbt_destroy(&meta_dbt);
		goto out;
	}

	ret = nb_next_ino(sbi, &ino);
	if (ret) {
free_data_out:
		dbt_destroy(&data_dbt);
		goto free_meta_out;
	}
	ftfs_nb_setup_metadata(&meta, S_IFLNK | S_IRWXUGO, len, 0, ino);
	inode = nb_setup_inode(dir->i_sb, &meta_dbt, &meta);
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		goto free_data_out;
	}

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = nb_bstore_meta_put(sbi->meta_db, &meta_dbt, txn, &meta);
	if (ret) {
abort:
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		set_nlink(inode, 0);
		FTFS_I(inode)->ftfs_flags |= FTFS_FLAG_DELETED;
		dbt_destroy(&FTFS_I(inode)->meta_dbt);
		iput(inode);
		dbt_destroy(&data_dbt);
		goto out;
	}
	ret = nb_bstore_put(sbi->data_db, &data_dbt, txn, symname, len, 0);
	if (ret)
		goto abort;

	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	inc_nlink(dir);
	mark_inode_dirty(dir);


	d_instantiate(dentry, inode);
	dbt_destroy(&data_dbt);
out:
	nb_put_read_lock(FTFS_I(dir));
	return ret;
}

/* XXX: This is for hard link. It seems the code is no longer valid */
static int nb_link(struct dentry *old_dentry,
		   struct inode *dir, struct dentry *dentry)
{
	int ret;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct inode *inode = old_dentry->d_inode;
	DBT *meta_dbt, *dir_meta_dbt, new_meta_dbt;
	struct ftfs_metadata meta;
	DB_TXN *txn;

	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));
	meta_dbt = nb_get_read_lock(FTFS_I(inode));
	ret = alloc_child_meta_dbt_from_meta_dbt(&new_meta_dbt,
			dir_meta_dbt, dentry->d_name.name);
	if (ret)
		goto out;
	inc_nlink(inode);
	meta.st.st_nlink = inode->i_nlink;
	meta.st.st_ino = inode->i_ino;
	meta.st.st_mode = inode->i_mode;

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = nb_bstore_meta_put(sbi->meta_db, &new_meta_dbt, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	if (!ret) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		inode->i_ctime = current_kernel_time();
#else
		inode->i_ctime = current_time(inode);
#endif
		inc_nlink(dir);
		mark_inode_dirty(dir);
		mark_inode_dirty(inode);
		ihold(inode);
		d_instantiate(dentry, inode);
	} else {
		drop_nlink(inode);
	}

	dbt_destroy(&new_meta_dbt);
	nb_put_read_lock(FTFS_I(inode));
	nb_put_read_lock(FTFS_I(dir));

out:
	return ret;
}

/* inode->i_mutex is called when enter into this function.
 * We assume i_sb->s_fs_info is not changed after mounting
 * the file system. When file system is umounted, the inode
 * lock should prevent i_sb->s_fs_info from being changed to
 * NULL before unlink is finished.
 */
static int nb_unlink(struct inode *dir, struct dentry *dentry)
{
	int ret = 0;
	struct inode *inode = dentry->d_inode;
	DBT *dir_meta_dbt, *meta_dbt;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	DBT indirect_dbt;
	DB_TXN *txn;

	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));
	meta_dbt = nb_get_read_lock(FTFS_I(inode));

	ret = alloc_child_meta_dbt_from_meta_dbt(&indirect_dbt, dir_meta_dbt, dentry->d_name.name);
	if (ret)
		goto out;
	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = nb_bstore_meta_del(sbi->meta_db, &indirect_dbt, txn);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		drop_nlink (dir);
		mark_inode_dirty(dir);
		if (dir->i_nlink  < 2) {
			printk(KERN_ERR "Warning: nlink < 2 for parent of unlinked file. If parent is root, ignore warning");
			BUG_ON(dir->i_sb->s_root->d_inode != dir);
		}
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}
	dbt_destroy(&indirect_dbt);
	/*
	 * There are places where delete message is issued
	 * for the VFS layer to delete the metadata of a file.
	 * One is from nb_unlink() which is called by vfs_unlink in do_unlinkat(),
	 * the other is from nb_evict_inode() which is called by iput() in do_unlink().
	 * Use the FTFS_FLAG_DELETED flag to tell nb_evict_inode that the metadata
	 * of the file has been deleted.
	 */
	FTFS_I(inode)->ftfs_flags |= FTFS_FLAG_DELETED;
out:
	nb_put_read_lock(FTFS_I(inode));
	nb_put_read_lock(FTFS_I(dir));

	if (ret)
		return ret;
	drop_nlink(inode);
	mark_inode_dirty(inode);

	return ret;
}

static struct dentry *
nb_lookup(struct inode *dir, struct dentry *dentry, unsigned int flags)
{
	int r, err;
	struct dentry *ret;
	struct inode *inode;
	struct ftfs_sb_info *sbi = dir->i_sb->s_fs_info;
	DBT *dir_meta_dbt, meta_dbt;
	DB_TXN *txn;
	struct ftfs_metadata meta;

	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));

	r = alloc_child_meta_dbt_from_meta_dbt(&meta_dbt,
			dir_meta_dbt, dentry->d_name.name);
	if (r) {
		inode = ERR_PTR(r);
		goto out;
	}

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);

	r = nb_bstore_meta_get(sbi->meta_db, &meta_dbt, txn, &meta);
	if (r == -ENOENT) {
		inode = NULL;
		dbt_destroy(&meta_dbt);
	} else if (r) {
		inode = ERR_PTR(r);
		nb_bstore_txn_abort(txn);
		dbt_destroy(&meta_dbt);
		goto out;
	}

	err = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(err, retry);

	// r == -ENOENT, inode == 0
	// r == 0, get meta, need to setup inode
	// r == err, error, will not execute this code
	if (r == 0) {
		inode = nb_setup_inode(dir->i_sb, &meta_dbt, &meta);
		if (IS_ERR(inode)) {
			dbt_destroy(&meta_dbt);
		}
	}

out:
	nb_put_read_lock(FTFS_I(dir));
	ret = d_splice_alias(inode, dentry);
	return ret;
}

/* copied from truncate_pagecache in truncate.c */
void ftfs_truncate_pagecache(struct inode *inode, loff_t oldsize, loff_t newsize)
{
	struct address_space *mapping = inode->i_mapping;
	loff_t holebegin = round_up(newsize, PAGE_SIZE);
	unmap_mapping_range(mapping, holebegin, 0, 1);
	truncate_inode_pages(mapping, newsize);
	unmap_mapping_range(mapping, holebegin, 0, 1);
}

static int nb_setattr(struct dentry *dentry, struct iattr *iattr)
{
	int ret;
	struct inode *inode = dentry->d_inode;
	loff_t size;
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	ret = inode_change_ok(inode, iattr);
#else /* LINUX_VERSION_CODE */
	ret = setattr_prepare(dentry, iattr);
#endif /* LINUX_VERSION_CODE */
	if (ret)
		return ret;

	size = i_size_read(inode);
	if ((iattr->ia_valid & ATTR_SIZE) && (iattr->ia_size != size)) {
		if (iattr->ia_size < size) {
			uint64_t block_num;
			size_t block_off;
			loff_t size;
			struct ftfs_inode *ftfs_inode = FTFS_I(inode);
			struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
			DBT *meta_dbt;
			DB_TXN *txn;

			size = i_size_read(inode);
			block_num = block_get_num_by_position(iattr->ia_size);
			block_off = block_get_off_by_position(iattr->ia_size);

			meta_dbt = nb_get_read_lock(ftfs_inode);

			i_size_write(inode, iattr->ia_size);
			ftfs_truncate_pagecache(inode, size, iattr->ia_size);

			TXN_GOTO_LABEL(retry);
			nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
			ret = nb_bstore_trunc(sbi->data_db, meta_dbt, txn,
			                        block_num, block_off);
			if (ret) {
				DBOP_JUMP_ON_CONFLICT(ret, retry);
				nb_bstore_txn_abort(txn);
				nb_put_read_lock(ftfs_inode);
				goto err;
			}
			ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
			COMMIT_JUMP_ON_CONFLICT(ret, retry);
			nb_put_read_lock(ftfs_inode);
		}
		else {
			i_size_write(inode, iattr->ia_size);
		}
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		inode->i_mtime = inode->i_ctime = CURRENT_TIME_SEC;
#else /* LINUX_VERSION_CODE */
		inode->i_mtime = inode->i_ctime = current_time(inode);
#endif /* LINUX_VERSION_CODE */
	}

	setattr_copy(inode, iattr);
	mark_inode_dirty(inode);

err:
	return ret;
}

#ifdef FTFS_XATTR
static int nb_setxattr(const struct xattr_handler *handler, struct dentry *dentry,
		       struct inode * inode, const char *name,
		       const void *value, size_t size, int flags)
{
	int ret = 0;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dentry->d_inode);
	DBT *meta_dbt;
	DBT xattr_dbt;
	DB_TXN *txn;
	ssize_t r = 0;
	// If this is a removal, it is now encoded as a flag to setxattr,
	// plus a value of null.
	bool is_removal = (flags & XATTR_REPLACE) && value == NULL;

	meta_dbt = nb_get_write_lock(ftfs_inode);
	ret = copy_xattr_dbt_from_meta_dbt(&xattr_dbt, meta_dbt, name, true);
	if (ret)
		goto cleanup;

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	if (is_removal) {
		ret = nb_bstore_meta_del(sbi->meta_db, &xattr_dbt, txn);
		/* Here, we don't actually care about the results, just whether
		 * the offset in r is non-zero.  There may be a more efficient way to do this,
		 * like having xattr_exists be a count instead of a boolean.
		 */
		if (!ret) {
			ret = nb_bstore_xattr_list(sbi->meta_db, meta_dbt, txn, NULL, 0, &r);
		}
	} else {
		// Default case
		ret = nb_bstore_xattr_put(sbi->meta_db, &xattr_dbt, txn, value, size);
	}

	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		goto cleanup;
	}
	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	if (ret) {
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
		goto cleanup;
	}
	if (!is_removal) {
		if (!ftfs_inode->xattr_exists) {
			ftfs_inode->xattr_exists = 1;
		}
	} else if (!ret && !r) {
		// Set this boolean to false if this was the last xattr
		ftfs_inode->xattr_exists = 0;
	}

cleanup:
	nb_put_write_lock(ftfs_inode);
	dbt_destroy(&xattr_dbt);
	return ret;
}

static int nb_getxattr(const struct xattr_handler *handler, struct dentry *dentry,
		       struct inode *inode, const char *name,
		       void *buffer, size_t size)
{
	ssize_t ret;
	int r = 0;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dentry->d_inode);
	DBT *meta_dbt, xattr_dbt;
	DB_TXN *txn;

	meta_dbt = nb_get_read_lock(ftfs_inode);
	r = copy_xattr_dbt_from_meta_dbt(&xattr_dbt, meta_dbt, name, true);
	if (r) {
		ret = r;
		goto cleanup;
	}

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = nb_bstore_xattr_get(sbi->meta_db, &xattr_dbt, txn, buffer, size);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		goto cleanup;
	}
	r = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

cleanup:
	nb_put_read_lock(ftfs_inode);
	dbt_destroy(&xattr_dbt);
	return ret;
}

static ssize_t nb_listxattr(struct dentry *dentry, char *buffer, size_t size)
{
	ssize_t ret = 0;
	int r;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dentry->d_inode);
	DBT *meta_dbt;
	DB_TXN *txn;

	meta_dbt = nb_get_read_lock(ftfs_inode);

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	r = nb_bstore_xattr_list(sbi->meta_db, meta_dbt, txn, buffer, size, &ret);
	if (r) {
		DBOP_JUMP_ON_CONFLICT(r, retry);
		nb_bstore_txn_abort(txn);
		ret = r;
		goto cleanup;
	}
	r = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);
	if (r) {
		ret = r;
	}

cleanup:
	nb_put_read_lock(ftfs_inode);
	return ret;
}

static const struct xattr_handler ftfs_xattr_handler = {
	.prefix                 = "", /* Match any name */
	.get                    = nb_getxattr,
	.set                    = nb_setxattr,
};

const struct xattr_handler *ftfs_xattr_handlers[] = {
	&ftfs_xattr_handler,
	NULL
};

#endif /* End of FTFS_XATTR */

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)

static void *nb_follow_link(struct dentry *dentry, struct nameidata *nd)
{
	int r;
	void *ret;
	void *buf;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dentry->d_inode);
	DBT *meta_dbt, data_dbt;
	DB_TXN *txn;

	buf = sb_malloc(FTFS_BSTORE_BLOCKSIZE);
	if (!buf) {
		ret = ERR_PTR(-ENOMEM);
		goto err1;
	}
	meta_dbt = nb_get_read_lock(ftfs_inode);
	// now block start from 1
	r = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt, 1);
	if (r) {
		ret = ERR_PTR(r);
		goto err2;
	}

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	r = nb_bstore_get(sbi->data_db, &data_dbt, txn, buf);
	if (r) {
		DBOP_JUMP_ON_CONFLICT(r, retry);
		nb_bstore_txn_abort(txn);
		ret = ERR_PTR(r);
		goto err3;
	}
	r = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

	nd_set_link(nd, buf);

	ret = buf;
err3:
	dbt_destroy(&data_dbt);
err2:
	nb_put_read_lock(ftfs_inode);
	if (ret != buf)
		sb_free(buf);
err1:
	return ret;
}

static void nb_put_link(struct dentry *dentry, struct nameidata *nd,
                          void *cookie)
{
	if (IS_ERR(cookie))
		return;
	sb_free(cookie);
}
#endif /* LINUX_VERSION_CODE */
static struct inode *nb_alloc_inode(struct super_block *sb)
{
	struct ftfs_inode *ftfs_inode;

	ftfs_inode = kmem_cache_alloc(ftfs_inode_cachep, GFP_KERNEL);
	// initialization in ftfs_nb_i_init_once

	return ftfs_inode ? &ftfs_inode->vfs_inode : NULL;
}

static void nb_destroy_inode(struct inode *inode)
{
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	nb_get_write_lock(ftfs_inode);
	if (ftfs_inode->meta_dbt.data &&
	    ftfs_inode->meta_dbt.data != &root_meta_key)
		dbt_destroy(&ftfs_inode->meta_dbt);

	kmem_cache_free(ftfs_inode_cachep, ftfs_inode);
}

static int
nb_write_inode(struct inode *inode, struct writeback_control *wbc)
{
	int ret = 0;
	DB_TXN *txn;
	DBT *meta_dbt;
	struct ftfs_metadata meta;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;

	if (inode->i_nlink == 0)
		goto no_write;

	meta_dbt = nb_get_read_lock(FTFS_I(inode));

	nb_copy_metadata_from_inode(&meta, inode);

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = nb_bstore_meta_put(sbi->meta_db, meta_dbt, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	nb_put_read_lock(FTFS_I(inode));

no_write:
	return ret;
}

static void nb_evict_inode(struct inode *inode)
{
	int ret;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	DBT *meta_dbt;
	DB_TXN *txn;

	if (inode->i_nlink || (FTFS_I(inode)->ftfs_flags & FTFS_FLAG_DELETED))
		goto no_delete;

	meta_dbt = nb_get_read_lock(FTFS_I(inode));

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_nb_do_unlink(meta_dbt, txn, inode, sbi);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	nb_put_read_lock(FTFS_I(inode));

no_delete:
	truncate_inode_pages(&inode->i_data, 0);
	invalidate_inode_buffers(inode);
	clear_inode(inode);
}

int ftfs_cache_init(void)
{
	int ret;

	ftfs_inode_cachep =
		kmem_cache_create("ftfs_i",
		                  sizeof(struct ftfs_inode), 0,
		                  SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD,
		                  ftfs_nb_i_init_once);
	if (!ftfs_inode_cachep) {
		printk(KERN_ERR "FTFS ERROR: Failed to initialize inode cache.\n");
		ret = -ENOMEM;
		goto out;
	}

	ftfs_writepages_cachep =
		kmem_cache_create("ftfs_wp",
		                  sizeof(struct ftfs_wp_node), 0,
		                  SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD,
		                  NULL);
	if (!ftfs_writepages_cachep) {
		printk(KERN_ERR "FTFS ERROR: Failed to initialize write page vec cache.\n");
		ret = -ENOMEM;
		goto out_free_inode_cachep;
	}

	return 0;

out_free_inode_cachep:
	kmem_cache_destroy(ftfs_inode_cachep);
out:
	return ret;
}

void ftfs_cache_destroy(void)
{
	if (ftfs_writepages_cachep)
		kmem_cache_destroy(ftfs_writepages_cachep);

	if (ftfs_inode_cachep)
		kmem_cache_destroy(ftfs_inode_cachep);

	ftfs_writepages_cachep = NULL;
	ftfs_inode_cachep = NULL;
}

// called when VFS wishes to free sb (unmount), sync southbound here
static void ftfs_nb_put_super(struct super_block *sb)
{
	struct ftfs_sb_info *sbi = sb->s_fs_info;
	sync_filesystem(sb);
	sb->s_fs_info = NULL;

	nb_bstore_checkpoint(sbi->db_env);
	nb_bstore_env_close(sbi);

	free_percpu(sbi->s_ftfs_info);
	g_sbi = NULL;
	sb_free(sbi);

	/* Begin old module exit code */
	destroy_ft_index();
	destroy_sb_vmalloc_cache();
	ftfs_cache_destroy();

	put_ftfs_southbound();

	TOKU_MEMLEAK_EXIT;
	nb_proc_toku_engine_status_exit();
	nb_proc_toku_checkpoint_exit();
	nb_proc_toku_flusher_exit();
	nb_proc_toku_dump_node_exit();
	printf_count_blindwrite();
#if defined(FT_INDIRECT) && defined(FTFS_MEM_DEBUG)
	ftfs_print_free_page_counter();
#endif
	printk(KERN_INFO "seq count = %ld, non seq count = %ld, txn count = %ld\n", seq_count, non_seq_count, txn_count);
}

static int ftfs_nb_sync_fs(struct super_block *sb, int wait)
{
	struct ftfs_sb_info *sbi = sb->s_fs_info;

	// DEP 8/19/19: In some mount failure cases, sbi->db_env
	// may not be initialized yet.
	if (sbi) {
		return nb_bstore_flush_log(sbi->db_env);
	} else {
		return 0;
	}
}

#ifdef FT_INDIRECT
void ftfs_invalidatepage(struct page *page, unsigned int offset,
		       unsigned int length)
{
	int count;
	struct ftfs_page_private *priv = FTFS_PAGE_PRIV(page);

	BUG_ON(!PageLocked(page));
	BUG_ON(page_mapped(page));

	if (nb_is_shared(page)) {
		ClearPagePrivate(page);
	}

	count = ftfs_read_page_count(page);
	/* If _count == 2, only page cache and pvec reference
	 * this page, we can confidently free the private data.
	 */
	if (count == 2) {
		BUG_ON(!priv);
		debug_ftfs_free_page(page);
		kfree(priv);
		page->private = 0;
	}
}

/**
 * Hook of delete_from_page_cache(). Assume private flag has been cleared up.
 * If priv == NULL, we do nothing to this page, else if the page cache is
 * the last one to reference this page, we need to free the private data
 *
 * If is a non-shared page (private flag is not set),
 * the page will be handled by remove_mapping() during truncate_inode_pages().
 * remove_mapping returns page with refcount == 0.
 */
void ftfs_freepage(struct page *page)
{
	struct ftfs_page_private *priv = FTFS_PAGE_PRIV(page);
	int count;

	BUG_ON(nb_is_shared(page));

	/* Page has been added to LRU list.
	 * This means ft layer releases this page first.
	 */
	if (PageLRU(page)) {
		return;
	}

	count = ftfs_read_page_count(page);
	/*
	 * Page cache is the only one to reference the page
	 * or remove_mapping is called.
	 */
	if ((count == 1 || count == 0) && priv != NULL) {
		debug_ftfs_free_page(page);
		kfree(priv);
		page->private = 0;
	}
}

/**
 * echo 3 > /proc/sys/vm/drop_caches should not touch shared pages
 * Page should be locked already. Called from invalidate_complete_page()
 */
int ftfs_releasepage(struct page *page, gfp_t mask)
{
	BUG_ON(!PageLocked(page));
	if (nb_is_shared(page))
		return 0;
	/**
	 * Just return 1 here and let ftfs_freepage() to free
	 * private data when it is deleted from page cache
	 */
	return 1;
}

/* for mmap(), we need to re-implement this hook for page sharing */
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
extern int ftfs_filemap_fault(struct vm_area_struct *vma,
			      struct vm_fault *vmf);
#else /* LINUX_VERSION_CODE */
extern int ftfs_filemap_fault(struct vm_fault *vmf);
#endif /* LINUX_VERSION_CODE */

static const struct vm_operations_struct ftfs_file_vm_ops = {
	.fault		= ftfs_filemap_fault,
	.page_mkwrite   = filemap_page_mkwrite,
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	.remap_pages	= generic_file_remap_pages,
#endif /* LINUX_VERSION_CODE */
};

static int ftfs_file_mmap(struct file *file, struct vm_area_struct *vma)
{
	file_accessed(file);
	vma->vm_ops = &ftfs_file_vm_ops;
	return 0;
}
#endif

static const struct address_space_operations ftfs_aops = {
	.readpage		= nb_readpage,
	.readpages		= nb_readpages,
	.writepage		= nb_writepage,
	.writepages		= nb_writepages,
#ifdef FT_INDIRECT
	.freepage		= ftfs_freepage,
	.releasepage		= ftfs_releasepage,
	.invalidatepage		= ftfs_invalidatepage,
#endif
	.write_begin		= nb_write_begin,
	.write_end		= nb_write_end,
	.launder_page		= nb_launder_page,
};

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
extern ssize_t ftfs_file_aio_read(struct kiocb *iocb,
				  const struct iovec *iov,
				  unsigned long nr_segs,
				  loff_t pos);
#endif /* LINUX_VERSION_CODE */
/**
 *  nb_data_exists_in_datadb()
 *  check whether data exists in data_db using nb_bstore_get()
 *  return 1 if data exists, 0 otherwise.
 */
int nb_data_exists_in_datadb(struct inode *inode, loff_t offset)
{
	int ret = 0;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	DBT *meta_dbt, data_dbt;
	DB_TXN *txn;
	void *buf;

	buf = sb_malloc(FTFS_BSTORE_BLOCKSIZE);
	if (!buf) {
		return 0;
	}
	meta_dbt = nb_get_read_lock(ftfs_inode);
	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt, block_get_num_by_position(offset));
	if (ret) {
		goto out;
	}
	ret = nb_bstore_get(sbi->data_db, &data_dbt, txn, buf);
	dbt_destroy(&data_dbt);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}
out:
	nb_put_read_lock(ftfs_inode);
	sb_free(buf);
	return (ret == 0) ? 1 : 0;
}


/**
 *  nb_llseek_data_or_hole() retrieves the offset for SEEK_DATA or SEEK_HOLE.
 *  First check the radix_tree. If not in page cache, then check if it exists in data_db.
 */
off_t nb_llseek_data_or_hole(struct file *file, loff_t offset, int whence)
{
	int ret = 0;
	struct inode *inode = file->f_mapping->host;
	uint64_t page_start, page_end, i;
	loff_t eof = i_size_read(inode);
	void * pi;

	if (offset >= eof) {
		return -ENXIO;
	}
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	mutex_lock(&inode->i_mutex);
#else /* LINUX_VERSION_CODE */
	inode_lock(inode);
#endif /* LINUX_VERSION_CODE */
	page_start = offset >> PAGE_SHIFT;
	page_end = (eof-1) >> PAGE_SHIFT;
	for (i = page_start; i <= page_end; i++) {
		rcu_read_lock();
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		pi = radix_tree_lookup(&inode->i_mapping->page_tree, i);
#else /* LINUX_VERSION_CODE */
		pi = radix_tree_lookup(&inode->i_mapping->i_pages, i);
#endif /* LINUX_VERSION_CODE */
		rcu_read_unlock();
		if ( (!pi) && (!nb_data_exists_in_datadb(inode, offset))) {
			// no data: done for SEEK_HOLE, retry for SEEK_DATA if not yet reach eof
			if (whence == SEEK_DATA) {
				offset = (i + 1) << PAGE_SHIFT;
				if (offset >= eof ){
					ret = -ENXIO; // reach eof and no data, set to -ENXIO
					break;
				}
			}
			else {
				break;
			}
		}
		else {
			// got data : done for SEEK_DATA, retry for SEEK_HOLE if not yet reach eof
			if (whence == SEEK_DATA) {
				break;
			}
			else {
				offset = (i + 1) << PAGE_SHIFT;
				if (offset >= eof ){
					offset = eof;
					break;
				}
			}
		}
	}
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	mutex_unlock(&inode->i_mutex);
#else /* LINUX_VERSION_CODE */
	inode_unlock(inode);
#endif /* LINUX_VERSION_CODE */
	return (ret == 0) ? vfs_setpos(file, offset, inode->i_sb->s_maxbytes) : ret;
}

static loff_t nb_llseek(struct file *file, loff_t offset, int whence)
{
	switch (whence) {
		case SEEK_SET:
		case SEEK_CUR:
		case SEEK_END:
		return generic_file_llseek(file, offset, whence);
		case SEEK_HOLE:
		case SEEK_DATA:
		default:
		return nb_llseek_data_or_hole(file,offset, whence);
	}
	return -EINVAL;
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
extern ssize_t ftfs_file_read_iter(struct kiocb *iocb, struct iov_iter *iter);
#endif /* LINUX_KERNEL_VERION */

static const struct file_operations ftfs_file_file_operations = {
	.llseek			= nb_llseek, //generic_file_llseek,
	.fsync			= nb_fsync,
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	.read			= do_sync_read,
	.write			= do_sync_write,
#ifdef FT_INDIRECT
	.aio_read		= ftfs_file_aio_read,
	.mmap			= ftfs_file_mmap,
#else
	.aio_read		= generic_file_aio_read,
	.mmap			= generic_file_mmap,
#endif
	.aio_write		= generic_file_aio_write,
#else /* LINUX_VERSION_CODE */
	.write_iter		= generic_file_write_iter,
#ifdef FT_INDIRECT
	.read_iter		= ftfs_file_read_iter,
	.mmap			= ftfs_file_mmap,
#else
	.read_iter		= generic_file_read_iter,
	.mmap			= generic_file_mmap,
#endif
#endif /* LINUX_VERSION_CODE */
};

static const struct file_operations ftfs_dir_file_operations = {
	.read			= generic_read_dir,
	.iterate		= nb_readdir,
	.fsync			= nb_fsync,
};

static const struct inode_operations ftfs_file_inode_operations = {
	.setattr		= nb_setattr,
#ifdef FTFS_XATTR
	.listxattr		= nb_listxattr,
#endif /* End of FTFS_XATTR */
};

static const struct inode_operations ftfs_dir_inode_operations = {
	.create			= nb_create,
	.lookup			= nb_lookup,
	.link			= nb_link,
	.unlink			= nb_unlink,
	.symlink		= nb_symlink,
	.mkdir			= nb_mkdir,
	.rmdir			= nb_rmdir,
	.mknod			= nb_mknod,
	.rename			= nb_rename,
	.setattr		= nb_setattr,
#ifdef FTFS_XATTR
	.listxattr		= nb_listxattr,
#endif /* End of FTFS_XATTR */
};

static const struct inode_operations ftfs_symlink_inode_operations = {
	.setattr		= nb_setattr,
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	.readlink		= generic_readlink,
	.follow_link		= nb_follow_link,
	.put_link		= nb_put_link,
#else /* LINUX_VERSION_CODE */
	.get_link		= page_get_link,
#endif
};

static const struct inode_operations ftfs_special_inode_operations = {
	.setattr		= nb_setattr,
};

static const struct super_operations ftfs_super_ops = {
	.alloc_inode		= nb_alloc_inode,
	.destroy_inode		= nb_destroy_inode,
	.write_inode		= nb_write_inode,
	.evict_inode		= nb_evict_inode,
	.put_super		= ftfs_nb_put_super,
	.sync_fs		= ftfs_nb_sync_fs,
	// XXX: Should really do a proper NB statfs
	.statfs			= sb_super_statfs,
};

/*
 * fill inode with meta_key, metadata from database and inode number
 */
struct inode *nb_setup_inode(struct super_block *sb,
			     DBT *meta_dbt,
			     struct ftfs_metadata *meta)
{
	struct inode *i;
	struct ftfs_inode *ftfs_inode;

	/* inode cached in searched by ino */
	if ((i = iget_locked(sb, meta->st.st_ino)) == NULL)
		return ERR_PTR(-ENOMEM);

	ftfs_inode = FTFS_I(i);
	if (!(i->i_state & I_NEW)) {
		DBT *old_dbt = nb_get_write_lock(ftfs_inode);
		dbt_destroy(old_dbt);
		dbt_copy(old_dbt, meta_dbt);
		nb_put_write_lock(ftfs_inode);
		return i;
	}

	BUG_ON(ftfs_inode->meta_dbt.data != NULL);
	dbt_copy(&ftfs_inode->meta_dbt, meta_dbt);
	init_rwsem(&ftfs_inode->key_lock);
	INIT_LIST_HEAD(&ftfs_inode->rename_locked);
	ftfs_inode->ftfs_flags = 0;
#ifdef FTFS_XATTR
	ftfs_inode->xattr_exists = 0;
#endif /* End of FTFS_XATTR */
	ftfs_inode->last_nr_pages = 0;
	ftfs_inode->last_offset = ULONG_MAX;

	i->i_rdev = meta->st.st_rdev;
	i->i_mode = meta->st.st_mode;
	set_nlink(i, meta->st.st_nlink);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99) || defined(CONFIG_UIDGID_STRICT_TYPE_CHECKS)
	i->i_uid.val = meta->st.st_uid;
	i->i_gid.val = meta->st.st_gid;
#else /* LINUX_VERSION_CODE >= 4.19.99 || CONFIG_UIDGID_STRICT_TYPE_CHECKS */
	i->i_uid = meta->st.st_uid;
	i->i_gid = meta->st.st_gid;
#endif /* LINUX_VERSION_CODE >= 4.19.99 || CONFIG_UIDGID_STRICT_TYPE_CHECKS */
	i->i_size = meta->st.st_size;
	i->i_blocks = meta->st.st_blocks;
	TIME_T_TO_TIMESPEC(i->i_atime, meta->st.st_atime);
	TIME_T_TO_TIMESPEC(i->i_mtime, meta->st.st_mtime);
	TIME_T_TO_TIMESPEC(i->i_ctime, meta->st.st_ctime);

	if (S_ISREG(i->i_mode)) {
		/* Regular file */
		i->i_op = &ftfs_file_inode_operations;
		i->i_fop = &ftfs_file_file_operations;
		i->i_data.a_ops = &ftfs_aops;
	} else if (S_ISDIR(i->i_mode)) {
		/* Directory */
		i->i_op = &ftfs_dir_inode_operations;
		i->i_fop = &ftfs_dir_file_operations;
	} else if (S_ISLNK(i->i_mode)) {
		/* Sym link */
		i->i_op = &ftfs_symlink_inode_operations;
		i->i_data.a_ops = &ftfs_aops;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
		inode_nohighmem(i);
#endif /* LINUX_VERSION_CODE */
	} else if (S_ISCHR(i->i_mode) || S_ISBLK(i->i_mode) ||
	           S_ISFIFO(i->i_mode) || S_ISSOCK(i->i_mode)) {
		i->i_op = &ftfs_special_inode_operations;
		init_special_inode(i, i->i_mode, i->i_rdev); // duplicates work
	} else {
		BUG();
	}

	unlock_new_inode(i);
	return i;
}

enum {
	Opt_circle_size,
	Opt_sb_fstype,
	Opt_sb_dev,
	Opt_d_dev,
	Opt_is_rotational,
	dummy_null
};

static const match_table_t tokens = {
	{Opt_circle_size, "max=%u"},
	{Opt_sb_fstype, "sb_fstype=%s"},
	{Opt_sb_dev, "sb_dev=%s"},
	{Opt_d_dev, "d_dev=%s"},
	{Opt_is_rotational, "is_rotational=%s"},
	{dummy_null, NULL}
};

#define MAX_FS_LEN 20

static int parse_options(char *options, struct ftfs_sb_info *sbi, char *sb_fstype, char **sb_dev)
{
	char *p;
	substring_t args[MAX_OPT_ARGS];
	int len;


	if (!options) {
		ftfs_error(__func__, "No options passed at mount time");
		return -EINVAL;
	}

	while ((p = strsep(&options, ",")) != NULL) {
		int token;

		if (!*p)
			continue;

		token = match_token(p, tokens, args);
		switch (token) {
		case Opt_circle_size:
			// Go ahead and accept the circle size argument (easier scripting)
			// But don't do anything
			printk(KERN_WARNING "FTFS: Zones are disabled in this build, but zone size option passed to mount.  This option will be ignored\n");
			break;

		case Opt_sb_fstype:
			len = strnlen(args[0].from, MAX_FS_LEN);
			if (MAX_FS_LEN > len) {
				memcpy(sb_fstype, args[0].from, len);
			} else {
				ftfs_error(__func__, "Name of southbound fs is too long");
				return -EINVAL;
			}
		case Opt_sb_dev:
			*sb_dev = args[0].from;
			break;
		case Opt_is_rotational:
			if (0 == strcmp(args[0].from, "0")) {
				sbi->is_rotational = false;
			} else if (0 == strcmp(args[0].from, "1")) {
				sbi->is_rotational = true;
			} else {
				ftfs_error(__func__, "is_rotational option has to be true or false");
				return -EINVAL;
			}
			is_hdd = sbi->is_rotational;
			break;
		case Opt_d_dev:
			// This is a real option but we don't need to use it here
			break;
		default:
			ftfs_error(__func__, "Unrecognized option passed at mount time");
		}
	}
	return 0;
}

static char *parse_d_dev(char *options)
{
	char *p;
	char *original;
	char *d_dev = NULL;
	substring_t args[MAX_OPT_ARGS];

	if (!options) {
		ftfs_error(__func__, "No dummy block device specified at mount time");
		return NULL;
	}

	original = options;

	while ((p = strsep(&options, ",")) != NULL) {
		int token;

		if (!*p)
			continue;

		token = match_token(p, tokens, args);
		if (token == Opt_d_dev) {
			// This is silly and unsafe, maybe we should enforce a maximum length?
			d_dev = sb_malloc(strlen(args[0].from) + 1);
			memcpy(d_dev, args[0].from, strlen(args[0].from) + 1);
		}

		/* strsep is destructive and we would like to parse this
		string again in ftfs_fill_super so we repair the string */
		if (options != original && options != NULL)
			options[-1] = ',';
	}
	options = original;
	return d_dev;
}


static int init_southbound_fs(const char *sb_dev, char * sb_fstype) {
	int ret;
	void *data = NULL;

	if (!sb_dev || strlen(sb_dev) == 0) {
		ftfs_error(__func__, "no mount device for ftfs_southbound!");
		return -EINVAL;
	}

	if (!sb_fstype || strnlen(sb_fstype, MAX_FS_LEN) == 0) {
		printk(KERN_INFO "No fstype specified, using default (ext4)");
		sb_fstype = "ext4";
	}

	/*
	 * Now we create a disconnected mount for our southbound file
	 * system. It will not be inserted into any mount trees, but
	 * we pin a global struct vfsmount that we use for all path
	 * resolution.
	 */

	ret = sb_private_mount(sb_dev, sb_fstype, data);
	if (ret) {
		ftfs_error(__func__, "can't mount southbound");
		return ret;
	}

	BUG_ON(ftfs_fs);
	BUG_ON(ftfs_files);

	/*
	 * The southbound "file system context" needs to be created to
	 * force all fractal tree worker threads to "see" our file
	 * system as if they were running in user space.
	 */

	ret = init_ftfs_southbound();
	if (ret) {
		ftfs_error(__func__, "can't init southbound_fs");
		return ret;
	}

	TOKU_MEMLEAK_INIT;

	ret = nb_proc_toku_engine_status_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku engine proc");
		return ret;
	}

	ret = nb_proc_toku_checkpoint_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku checkpoint proc");
		return ret;
	}

	ret = nb_proc_toku_flusher_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku flusher proc");
		return ret;
	}

	ret = nb_proc_toku_dump_node_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku dump node proc");
		return ret;
	}

	ret = init_sb_vmalloc_cache();
	if (ret) {
		ftfs_error(__func__, "can't init vmalloc caches");
		return ret;
	}

	return init_ft_index();
}


/*
 * fill in the superblock
 */
static int ftfs_nb_fill_super(struct super_block *sb, void *data, int silent)
{
	int ret;
	int cpu;
	ino_t ino;
	struct inode *root;
	struct ftfs_metadata meta;
	struct ftfs_sb_info *sbi;
	DBT root_dbt;
	DB_TXN *txn;
	char *sb_dev;
	char sb_fstype[MAX_FS_LEN];

	// FTFS specific info
	ret = -ENOMEM;
	sbi = sb_malloc(sizeof(struct ftfs_sb_info));
	if (!sbi)
		goto err;
	g_sbi = sbi;
	memset(sbi, 0, sizeof(*sbi));

	// Setup southbound
	sb_dev = NULL;
	memset(sb_fstype, 0, MAX_FS_LEN);
	ret = parse_options(data, sbi, sb_fstype, &sb_dev);
	if (ret) {
		printk(KERN_ERR "ftfs_fill_super: parse_options failed\n");
		goto err;
	}

	ret = init_southbound_fs(sb_dev, sb_fstype);
	if (ret) {
		printk(KERN_ERR "ftfs_fill_super: init_southboud_fs failed\n");
		goto err;
	}
	ret = -ENOMEM;

	sb->s_dev = ftfs_vfs->mnt_sb->s_dev;

	sbi->s_ftfs_info = alloc_percpu(struct ftfs_info);

	if (!sbi->s_ftfs_info)
		goto err;

	// DEP 8/19/19: In case subsequent steps fail during mount,
	// set this to NULL for graceful cleanup
	sbi->db_env = NULL;

	sb->s_fs_info = sbi;

	sb_set_blocksize(sb, FTFS_BSTORE_BLOCKSIZE);

	sb->s_op = &ftfs_super_ops;
#ifdef FTFS_XATTR
	sb->s_xattr = ftfs_xattr_handlers;
#endif /* End of FTFS_XATTR */
	sb->s_maxbytes = MAX_LFS_FILESIZE;

	ret = nb_bstore_env_open(sbi);
	if (ret) {
		printk(KERN_ERR "ftfs_fill_super: nb_bstore_env_open failed\n");
		goto err;
	}

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	dbt_setup(&root_dbt, &root_meta_key, SIZEOF_META_KEY(root_meta_key));
	ret = nb_bstore_meta_get(sbi->meta_db, &root_dbt, txn, &meta);

	if (ret) {
		if (ret == -ENOENT) {
			printk(KERN_ERR "ftfs_fill_super: ftfs_bstore_meta_get failed with ENOENT\n");
			ftfs_nb_setup_metadata(&meta, 0755 | S_IFDIR, 0, 0,
					       FTFS_ROOT_INO);
			ret = nb_bstore_meta_put(sbi->meta_db,
						 &root_dbt,
						 txn, &meta);
		}
		if (ret) {
			printk(KERN_ERR "ftfs_fill_super: db op error\n");
			goto db_op_err;
		}
	}
	ret = nb_bstore_get_ino(sbi->meta_db, txn, &ino);
	if (ret) {
		printk(KERN_ERR "ftfs_fill_super: nb_bstore_get_ino failed\n");
db_op_err:
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
		goto err;
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_SYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	sbi->s_nr_cpus = 0;
	for_each_possible_cpu(cpu) {
		(per_cpu_ptr(sbi->s_ftfs_info, cpu))->next_ino = ino + cpu;
		(per_cpu_ptr(sbi->s_ftfs_info, cpu))->max_ino = ino;
		sbi->s_nr_cpus++;
	}

	root = nb_setup_inode(sb, &root_dbt, &meta);
	if (IS_ERR(root)) {
		ret = PTR_ERR(root);
		printk(KERN_ERR "ftfs_fill_super: ftfs_setup_inode failed\n");
		goto err_close;
	}

	sb->s_root = d_make_root(root);
	if (!sb->s_root) {
		ret = -EINVAL;
		printk(KERN_ERR "ftfs_fill_super: d_make_root failed\n");
		goto err_close;
	}

	return 0;

err_close:
	nb_bstore_env_close(sbi);
err:
	if (sbi) {
		if (sbi->s_ftfs_info)
			free_percpu(sbi->s_ftfs_info);
		sb_free(sbi);
	}
	sb->s_fs_info = NULL;
	if (ret)
		printk(KERN_ERR "ftfs_fill_super: failed %d\n", ret);
	return ret;
}

/*
 * mount ftfs, call kernel util mount_bdev
 * actual work of ftfs is done in ftfs_fill_super
 */
static struct dentry *ftfs_mount(struct file_system_type *fs_type, int flags,
                                 const char *sb_dev, void *data)
{
	char *all_opts;
	char *dummy;
	size_t size;
	struct dentry *ret;
	int rv;

	if (ftfs_vfs != NULL) {
		/* This is the case where we get a second mount attempt without
		 * an rmmod first. */
		ftfs_cache_destroy();
		rv = sb_private_umount();
		if (rv) {
			ftfs_error(__func__, "unable to umount ftfs southbound");
			ret = ERR_PTR(rv);
			goto out;
		}

		ftfs_vfs = NULL;
		ftfs_files = NULL;
		ftfs_cred = NULL;
	}

	ftfs_cache_init();

	// No options passed
	if (data == NULL) {
		ftfs_error(__func__, "You must pass a dummy block device to ftfs at mount time");
		ret = ERR_PTR(-EINVAL);
		goto out;
	}

	size = strlen(data) + 8 + strlen(sb_dev) + 1;
	all_opts = sb_malloc(size);
	snprintf(all_opts, size, "%s,sb_dev=%s", (char *)data, sb_dev);

	dummy = parse_d_dev(data);
	if (!dummy) {
		ret = ERR_PTR(-EINVAL);
		goto out_free;
	}

	ret = mount_bdev(fs_type, flags, dummy, all_opts, ftfs_nb_fill_super);

	sb_free(dummy);
 out_free:
	sb_free(all_opts);

 out:
	if (IS_ERR(ret))
		printk(KERN_ERR "ftfs_mount: failed with %ld\n", PTR_ERR(ret));
	return ret;
}

static void ftfs_nb_kill_sb(struct super_block *sb)
{
	sync_filesystem(sb);
	kill_block_super(sb);
}

static struct file_system_type ftfs_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "ftfs",
	.mount		= ftfs_mount,
	.kill_sb	= ftfs_nb_kill_sb,
	.fs_flags	= FS_REQUIRES_DEV,
};

int init_ftfs_fs(void)
{
	int ret;

	ret = proc_pfn_init();
	if (ret) {
		printk(KERN_ERR "FTFS ERROR: Failed to create proc file\n");
		return ret;
	}

	ret = register_filesystem(&ftfs_fs_type);
	if (ret) {
		printk(KERN_ERR "FTFS ERROR: Failed to register filesystem\n");
	}
	return ret;
}

void exit_ftfs_fs(void)
{
	unregister_filesystem(&ftfs_fs_type);
	proc_pfn_deinit();
}
