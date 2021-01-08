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

#include <linux/blkdev.h>

#include "sb_malloc.h"
#include "sb_stat.h"
#include "ftfs.h"
#include "ftfs_northbound.h"
#include "nb_proc_toku_engine_status.h"
#include "nb_proc_toku_checkpoint.h"
#include "nb_proc_toku_flusher.h"
#include "nb_proc_toku_memleak_detect.h"
#include "nb_proc_toku_dump_node.h"


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

	meta->st.st_dev = 0;
	meta->st.st_ino = ino;
	meta->st.st_mode = mode;
	meta->st.st_nlink = 1;
	meta->st.st_uid = __kuid_val(current_uid());
	meta->st.st_gid = __kgid_val(current_gid());
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
	meta->st.st_uid = __kuid_val(inode->i_uid);
	meta->st.st_gid = __kgid_val(inode->i_gid);
	meta->st.st_rdev = inode->i_rdev;
	meta->st.st_size = i_size_read(inode);
	meta->st.st_blksize = FTFS_BSTORE_BLOCKSIZE;
	meta->st.st_blocks = nb_get_block_num_by_size(meta->st.st_size);
	TIMESPEC_TO_TIME_T(meta->st.st_atime, inode->i_atime);
	TIMESPEC_TO_TIME_T(meta->st.st_mtime, inode->i_mtime);
	TIMESPEC_TO_TIME_T(meta->st.st_ctime, inode->i_ctime);
}

static inline DBT *nb_get_read_lock(struct ftfs_inode *f_inode)
{
	down_read(&f_inode->key_lock);
	return &f_inode->meta_dbt;
}

static inline void nb_put_read_lock(struct ftfs_inode *f_inode)
{
	up_read(&f_inode->key_lock);
}

static inline DBT *nb_get_write_lock(struct ftfs_inode *f_inode)
{
	down_write(&f_inode->key_lock);
	return &f_inode->meta_dbt;
}

static inline void nb_put_write_lock(struct ftfs_inode *f_inode)
{
	up_write(&f_inode->key_lock);
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

static struct inode *
nb_setup_inode(struct super_block *sb, DBT *meta_dbt,
                 struct ftfs_metadata *meta);

static int
ftfs_nb_do_unlink(DBT *meta_dbt, DB_TXN *txn, struct inode *inode,
		  struct ftfs_sb_info *sbi)
{
	int ret;

	ret = nb_bstore_meta_del(sbi->meta_db, meta_dbt, txn);
	if (!ret && i_size_read(inode) > 0)
		ret = nb_bstore_trunc(sbi->data_db, meta_dbt, txn, 0, 0);

	return ret;
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

	ret = dbt_alloc_data_from_meta(&data_dbt, meta_dbt,
	                               PAGE_TO_BLOCK_NUM(page));
	if (ret)
		return ret;
	buf = kmap(page);
	buf = buf + (offset & ~PAGE_CACHE_MASK);
	off = block_get_off_by_position(offset);
	ret = nb_bstore_update(sbi->data_db, &data_dbt, txn, buf, len, off);
	kunmap(page);
	dbt_destroy(&data_dbt);

	return ret;
}

static int
__nb_writepage(struct ftfs_sb_info *sbi, struct inode *inode, DBT *meta_dbt,
                 struct page *page, size_t len, DB_TXN *txn)
{
	int ret;
	char *buf;
	DBT data_dbt;

	ret = dbt_alloc_data_from_meta(&data_dbt, meta_dbt,
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
	end_index = i_size >> PAGE_CACHE_SHIFT;

	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	if (page->index < end_index)
		ret = __nb_writepage(sbi, inode, meta_dbt, page, PAGE_CACHE_SIZE, txn);
	else {
		offset = i_size & (~PAGE_CACHE_MASK);
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

struct ftfs_wp_node {
	struct page *page;
	struct ftfs_wp_node *next;
};
#define FTFS_WRITEPAGES_LIST_SIZE 4096

static struct kmem_cache *ftfs_writepages_cachep;

static int
__nb_writepages_write_pages(struct ftfs_wp_node *list, int nr_pages,
                              struct writeback_control *wbc,
                              struct inode *inode, struct ftfs_sb_info *sbi,
                              DBT *data_dbt, int is_seq)
{
	int i, ret;
	loff_t i_size;
	pgoff_t end_index;
	unsigned offset;
	char *buf;
	struct ftfs_wp_node *it;
	struct page *page;
	DBT *meta_dbt;
	char *data_key;
	DB_TXN *txn;

	meta_dbt = nb_get_read_lock(FTFS_I(inode));
	data_key = data_dbt->data;
	if (unlikely(!key_is_same_of_key((char *)meta_dbt->data, data_key))) {
		dbt_copy_data_from_meta(data_dbt, meta_dbt, 0);
	}
retry:
	i_size = i_size_read(inode);
	end_index = i_size >> PAGE_CACHE_SHIFT;
	offset = i_size & (PAGE_CACHE_SIZE - 1);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	// we did a lazy approach about the list, so we need an additional i here
	for (i = 0, it = list->next; i < nr_pages; i++, it = it->next) {
		page = it->page;
		nb_data_key_set_blocknum(data_key, data_dbt->size,
					 PAGE_TO_BLOCK_NUM(page));
		buf = kmap(page);
		if (page->index < end_index)
			ret = nb_bstore_put(sbi->data_db, data_dbt, txn, buf,
					    PAGE_CACHE_SIZE, is_seq);
		else if (page->index == end_index && offset != 0)
			ret = nb_bstore_put(sbi->data_db, data_dbt, txn, buf,
					    offset, is_seq);
		else
			ret = 0;
		kunmap(page);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			nb_bstore_txn_abort(txn);
			goto out;
		}
	}
	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
out:
	nb_put_read_lock(FTFS_I(inode));
	for (i = 0, it = list->next; i < nr_pages; i++, it = it->next) {
		page = it->page;
		end_page_writeback(page);
		if (ret)
			redirty_page_for_writepage(wbc, page);
		unlock_page(page);
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

	pagevec_init(&pvec, 0);
	if (wbc->range_cyclic) {
		writeback_index = mapping->writeback_index; /* prev offset */
		index = writeback_index;
		if (index == 0)
			cycled = 1;
		else
			cycled = 0;
		end = -1;
	} else {
		index = wbc->range_start >> PAGE_CACHE_SHIFT;
		end = wbc->range_end >> PAGE_CACHE_SHIFT;
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
	dbt_copy_data_from_meta(&data_dbt, meta_dbt, 0);
	nb_put_read_lock(FTFS_I(inode));

	nr_list_pages = 0;
	list.next = NULL;
	tail = &list;
	while (!done && (index <= end)) {
		nr_pages = pagevec_lookup_tag(&pvec, mapping, &index, tag,
			      min(end - index, (pgoff_t)PAGEVEC_SIZE-1) + 1);
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

			if (unlikely(page->mapping != mapping)) {
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

			BUG_ON(PageWriteback(page));
			if (!clear_page_dirty_for_io(page))
				goto continue_unlock;

			set_page_writeback(page);
			if (tail->next == NULL) {
				tail->next = kmem_cache_alloc(
					ftfs_writepages_cachep, GFP_KERNEL);
				tail->next->next = NULL;
			}
			tail = tail->next;
			tail->page = page;
			++nr_list_pages;
			if (nr_list_pages >= FTFS_WRITEPAGES_LIST_SIZE) {
				// we have many pages, use seq_put
				is_seq = 1;
				ret = __nb_writepages_write_pages(&list,
					nr_list_pages, wbc, inode, sbi,
					&data_dbt, is_seq);
				if (ret)
					goto free_dkey_out;
				done_index = txn_done_index;
				nr_list_pages = 0;
				tail = &list;
			}

			if (--wbc->nr_to_write <= 0 &&
			    wbc->sync_mode == WB_SYNC_NONE) {
				done = 1;
				break;
			}
		}
		pagevec_release(&pvec);
		cond_resched();
	}

	if (nr_list_pages > 0) {
		if (nr_list_pages > LARGE_IO_THRESHOLD)
			is_seq = 1;
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

static int
nb_write_begin(struct file *file, struct address_space *mapping,
                 loff_t pos, unsigned len, unsigned flags,
                 struct page **pagep, void **fsdata)
{
	int ret = 0;
	struct page *page;
	pgoff_t index = pos >> PAGE_CACHE_SHIFT;

	page = grab_cache_page_write_begin(mapping, index, flags);
	if (!page)
		ret = -ENOMEM;
	/* don't read page if not uptodate */

	*pagep = page;
	return ret;
}

static int
nb_write_end(struct file *file, struct address_space *mapping,
               loff_t pos, unsigned len, unsigned copied,
               struct page *page, void *fsdata)
{
	/* make sure that ftfs can't guarantee uptodate page */
	loff_t last_pos = pos + copied;
	struct inode *inode = page->mapping->host;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
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
	if (PageUptodate(page) || copied == PAGE_CACHE_SIZE) {
postpone_to_writepage:
		SetPageUptodate(page);
		set_page_dirty(page);
	} else if (page_offset(page) >= i_size_read(inode)) {
		buf = kmap(page);
		if (pos & ~PAGE_CACHE_MASK)
			memset(buf, 0, pos & ~PAGE_CACHE_MASK);
		if (last_pos & ~PAGE_CACHE_MASK)
			memset(buf + (last_pos & ~PAGE_CACHE_MASK), 0,
			       PAGE_CACHE_SIZE - (last_pos & ~PAGE_CACHE_MASK));
		kunmap(page);
		goto postpone_to_writepage;
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
	page_cache_release(page);

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

/*
 * we lock the whole subtree before clone for exclusive access. for
 * either success or fail, you have to call unlock or else you are
 * hosed
 *
 * for clones, we need to sync all dirty pages
 * the root is not locked by this function
 */
static void
lock_descendents_for_clone(struct dentry *dentry, struct list_head *lock_list,
                           bool flush)
{
	struct dentry *this_parent;
	struct list_head *next;
	struct inode *inode;

	BUG_ON(!dentry);
	this_parent = dentry;
start:
	if (this_parent->d_sb != dentry->d_sb)
		goto end;
	inode = this_parent->d_inode;
	if (inode == NULL)
		goto repeat;
	if (this_parent != dentry) {
		if (flush) {
			// from generic_file_fsync
			int r;
			r = filemap_write_and_wait_range(inode->i_mapping, 0, -1);
			BUG_ON(r);
			mutex_lock(&inode->i_mutex);
			r = sync_mapping_buffers(inode->i_mapping);
			if (r || !(inode->i_state & I_DIRTY)) {
				goto unlock_inode;
			}
			r = sync_inode_metadata(inode, 1);
unlock_inode:
			mutex_unlock(&inode->i_mutex);
			BUG_ON(r);
		}
		nb_get_write_lock(FTFS_I(inode));
		list_add(&FTFS_I(inode)->rename_locked, lock_list);
	}
repeat:
	next = this_parent->d_subdirs.next;
resume:
	while (next != &this_parent->d_subdirs) {
		this_parent = list_entry(next, struct dentry, d_u.d_child);
		goto start;
	}
end:
	if (this_parent != dentry) {
		next = this_parent->d_u.d_child.next;
		this_parent = this_parent->d_parent;
		goto resume;
	}
}

static int unlock_descendents_after_clone(struct list_head *lock_list)
{
	struct ftfs_inode *ftfs_inode, *tmp;

	list_for_each_entry_safe(ftfs_inode, tmp, lock_list, rename_locked) {
		nb_put_write_lock(ftfs_inode);
		list_del(&ftfs_inode->rename_locked);
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

	ret = dbt_alloc_prefix_from_meta(&old_prefix_dbt, old_meta_dbt);
	BUG_ON(ret);
	ret = dbt_alloc_prefix_from_meta(&new_prefix_dbt, new_meta_dbt);
	BUG_ON(ret);

	list_for_each_entry_safe(f_inode, tmp, locked, rename_locked) {
		dbt_copy(&tmp_dbt, &f_inode->meta_dbt);
		ret = dbt_alloc_meta_movdir(&old_prefix_dbt, &new_prefix_dbt,
		                            &tmp_dbt, &f_inode->meta_dbt);
		BUG_ON(ret);
		dbt_destroy(&tmp_dbt);
	}

	dbt_destroy(&old_prefix_dbt);
	dbt_destroy(&new_prefix_dbt);
}

static int nb_rename(struct inode *old_dir, struct dentry *old_dentry,
                       struct inode *new_dir, struct dentry *new_dentry)
{
	int ret, drop_newdir_count;

#ifdef FTFS_EMPTY_DIR_VERIFY
	int err, r;
#endif

	struct inode *old_inode, *new_inode;
	struct ftfs_sb_info *sbi = old_dir->i_sb->s_fs_info;
	DBT *old_meta_dbt, new_meta_dbt, *old_dir_meta_dbt, *new_dir_meta_dbt,
	    *new_inode_meta_dbt;
	struct ftfs_metadata old_meta;
	LIST_HEAD(lock_list);
	DB_TXN *txn;

	// to prevent any other move from happening, we grab sem of parents
	old_dir_meta_dbt = nb_get_read_lock(FTFS_I(old_dir));
	new_dir_meta_dbt = nb_get_read_lock(FTFS_I(new_dir));

	old_inode = old_dentry->d_inode;
	old_meta_dbt = nb_get_write_lock(FTFS_I(old_inode));
	new_inode = new_dentry->d_inode;
	new_inode_meta_dbt = new_inode ?
		nb_get_write_lock(FTFS_I(new_inode)) : NULL;
	lock_descendents_for_clone(old_dentry, &lock_list, false);

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

	ret = dbt_alloc_child_meta_from_meta(&new_meta_dbt, new_dir_meta_dbt,
	                                     new_dentry->d_name.name);
	if (ret)
		goto abort;

	nb_copy_metadata_from_inode(&old_meta, old_inode);
	ret = nb_bstore_meta_del(sbi->meta_db, old_meta_dbt, txn);
	if (!ret)
		ret = nb_bstore_meta_put(sbi->meta_db, &new_meta_dbt, txn, &old_meta);
	if (S_ISDIR(old_meta.st.st_mode)) {
		ret = nb_bstore_dir_clone(sbi->meta_db, sbi->data_db,
		                            old_meta_dbt, &new_meta_dbt,
		                            txn, true);
	} else if (old_meta.st.st_size < FTFS_BSTORE_MOVE_LARGE_THRESHOLD) {
		ret = nb_bstore_file_clone_copy(sbi->data_db,
		                                  old_meta_dbt, &new_meta_dbt,
		                                  txn, true);
	} else {
		ret = nb_bstore_file_clone(sbi->data_db,
		                             old_meta_dbt, &new_meta_dbt,
		                             txn, true);
	}

	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort1;
	}

	ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	nb_update_ftfs_inode_keys(&lock_list, old_meta_dbt,
				  &new_meta_dbt);
	dbt_destroy(&FTFS_I(old_inode)->meta_dbt);
	dbt_copy(&FTFS_I(old_inode)->meta_dbt, &new_meta_dbt);

	unlock_descendents_after_clone(&lock_list);

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
	unlock_descendents_after_clone(&lock_list);
	nb_put_write_lock(FTFS_I(old_inode));
	if (new_inode)
		nb_put_write_lock(FTFS_I(new_inode));
	nb_put_read_lock(FTFS_I(old_dir));
	nb_put_read_lock(FTFS_I(new_dir));

	return ret;
}

// checking from btrfs_ioctl_clone
static long nb_file_clone(struct file *file, unsigned long srcfd)
{
	int r;
	struct fd src_file;
	struct inode *src, *dst;
	loff_t size;
	DBT *src_key, *dst_key;
	DB_TXN *txn;
	struct ftfs_sb_info *sbi;

	// the destination must be opened for writing
	if (!(file->f_mode & FMODE_WRITE) || (file->f_flags & O_APPEND)) {
		return -EINVAL;
	}

	r = mnt_want_write_file(file);
	if (r) {
		return r;
	}

	src_file = fdget(srcfd);
	if (!src_file.file) {
		r = -EBADF;
		goto out_drop_write;
	}

	if (src_file.file->f_path.mnt != file->f_path.mnt) {
		r = -EXDEV;
		goto out_fput;
	}

	src = file_inode(src_file.file);
	dst = file_inode(file);
	if (src == dst) {
		r = -EINVAL;
		goto out_fput;
	}

	if (!(src_file.file->f_mode & FMODE_READ)) {
		r = -EINVAL;
		goto out_fput;
	}

	if (S_ISDIR(src->i_mode) || S_ISDIR(dst->i_mode)) {
		r = -EISDIR;
		goto out_fput;
	}

	// remove all pages of dst
	truncate_inode_pages_range(&dst->i_data, 0, -1);

	// sync src, generic_file_fsync but we are not going to write inode
	r = filemap_write_and_wait_range(src->i_mapping, 0, -1);
	if (r) {
		goto out_unlock;
	}
	r = sync_mapping_buffers(src->i_mapping);
	if (r) {
		goto out_unlock;
	}

	size = i_size_read(src);
	// we are going to write dirty pages for src
	src_key = nb_get_write_lock(FTFS_I(src));
	dst_key = nb_get_write_lock(FTFS_I(dst));

	// clone txn
	sbi = src->i_sb->s_fs_info;
	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	if (size < FTFS_BSTORE_MOVE_LARGE_THRESHOLD) {
		r = nb_bstore_file_clone_copy(sbi->data_db,
		                                src_key, dst_key,
					        txn, false);
	} else {
		r = nb_bstore_file_clone(sbi->data_db,
		                           src_key, dst_key,
					   txn, false);
	}
	if (r) {
		DBOP_JUMP_ON_CONFLICT(r, retry);
		nb_bstore_txn_abort(txn);
		goto out_unlock;
	}

	r = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

out_unlock:
	nb_put_write_lock(FTFS_I(dst));
	nb_put_write_lock(FTFS_I(src));
out_fput:
	fdput(src_file);
out_drop_write:
	if (!r) {
		i_size_write(dst, size);
		mark_inode_dirty(dst);
	}
	mnt_drop_write_file(file);
	return r;
}

static long
nb_file_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	switch (cmd) {
	case FTFS_IOC_CLONE:
		return nb_file_clone(file, arg);
	}

	return -EINVAL;
}

static long
nb_dir_clone(struct file *file, unsigned long srcfd)
{	struct fd src_file;
	struct inode *src, *dst;
	DBT *src_key, *dst_key;
	DB_TXN *txn;
	struct ftfs_sb_info *sbi;
	LIST_HEAD(lock_list);
	int r, dir_empty;

	// check dst permision
	src_file = fdget(srcfd);
	if (!src_file.file) {
		return -EBADF;
	}
	if (src_file.file->f_path.mnt != file->f_path.mnt) {
		r = -EXDEV;
		goto out_fput;
	}
	src = file_inode(src_file.file);
	dst = file_inode(file);

	if (!S_ISDIR(src->i_mode) || !S_ISDIR(dst->i_mode)) {
		r = -EISDIR;
		goto out_fput;
	}

	// may_create in fs/namei.c
	if (IS_DEADDIR(dst) || IS_DEADDIR(src)) {
		r = -ENOENT;
		goto out_fput;
	}
	r = inode_permission(dst, MAY_WRITE | MAY_EXEC);
	if (r) {
		goto out_fput;
	}
	if (!(src_file.file->f_mode & FMODE_READ)) {
		r = -EINVAL;
		goto out_fput;
	}

	// lock and sync src
	dst_key = nb_get_write_lock(FTFS_I(dst));
	src_key = nb_get_write_lock(FTFS_I(src));
	lock_descendents_for_clone(d_find_alias(src), &lock_list, true);

	// clone txn
	sbi = src->i_sb->s_fs_info;
	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	// check if the dst is empty
	r = nb_dir_is_empty(sbi->meta_db, dst_key, txn, &dir_empty);
	if (r) {
		goto abort;
	}
	if (!dir_empty) {
		r = -ENOTEMPTY;
		goto abort;
	}

	// do move
	r = nb_bstore_dir_clone(sbi->meta_db, sbi->data_db,
	                          src_key, dst_key, txn, false);
	if (r) {
abort:
		DBOP_JUMP_ON_CONFLICT(r, retry);
		nb_bstore_txn_abort(txn);
		goto out_unlock;
	}

	r = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

out_unlock:
	// unlock
	unlock_descendents_after_clone(&lock_list);
	nb_put_write_lock(FTFS_I(src));
	nb_put_write_lock(FTFS_I(dst));
out_fput:
	fdput(src_file);

	return r;	
}

static long
nb_dir_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	switch (cmd) {
	case FTFS_IOC_CLONE:
		return nb_dir_clone(file, arg);
	}

	return -EINVAL;
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
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
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
	ret = nb_bstore_meta_readdir(sbi->meta_db, meta_dbt, txn, ctx);
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

	if (rdev && !new_valid_dev(rdev))
		return -EINVAL;

	dir_meta_dbt = nb_get_read_lock(FTFS_I(dir));
	ret = dbt_alloc_child_meta_from_meta(&meta_dbt, dir_meta_dbt,
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
	ret = dbt_alloc_child_meta_from_meta(&meta_dbt, dir_meta_dbt, dentry->d_name.name);
	if (ret)
		goto out;

	ret = dbt_alloc_data_from_meta(&data_dbt, &meta_dbt, 1);
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
	ret = dbt_alloc_child_meta_from_meta(&new_meta_dbt, dir_meta_dbt, 
										 dentry->d_name.name);
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
		inode->i_ctime = current_kernel_time();
		inc_nlink(dir);
		mark_inode_dirty(dir);
		mark_inode_dirty(inode);
		ihold(inode);
		d_instantiate(dentry, inode);
	}
	else {
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

	ret = dbt_alloc_child_meta_from_meta(&indirect_dbt,dir_meta_dbt, dentry->d_name.name);
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
		if (dir->i_nlink  < 2)
			printk(KERN_ERR "Warning: nlink < 2 for parent of unlinked file. If parent is root, ignore warning");
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}
	dbt_destroy(&indirect_dbt);
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
	r = dbt_alloc_child_meta_from_meta(&meta_dbt, dir_meta_dbt, dentry->d_name.name);

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
		if (IS_ERR(inode))
			dbt_destroy(&meta_dbt);
	}

out:
	nb_put_read_lock(FTFS_I(dir));
	ret = d_splice_alias(inode, dentry);

	return ret;
}

static int nb_setattr(struct dentry *dentry, struct iattr *iattr)
{
	int ret;
	struct inode *inode = dentry->d_inode;
	loff_t size;

	ret = inode_change_ok(inode, iattr);
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
			truncate_pagecache(inode, size, iattr->ia_size);

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
		inode->i_mtime = inode->i_ctime = CURRENT_TIME_SEC;
	}

	setattr_copy(inode, iattr);
	mark_inode_dirty(inode);

err:
	return ret;
}

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
	r = dbt_alloc_data_from_meta(&data_dbt, meta_dbt, 1);
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

	sb->s_fs_info = NULL;

	nb_bstore_checkpoint(sbi->db_env);
	nb_bstore_env_close(sbi);

	free_percpu(sbi->s_ftfs_info);
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

static int nb_dir_release(struct inode *inode, struct file *filp)
{
	if (filp->f_pos != 0 && filp->f_pos != 1)
		sb_free((char *)filp->f_pos);

	return 0;
}

static const struct address_space_operations ftfs_aops = {
	.readpage		= nb_readpage,
	.readpages		= nb_readpages,
	.writepage		= nb_writepage,
	.writepages		= nb_writepages,
	.write_begin	= nb_write_begin,
	.write_end		= nb_write_end,
	.launder_page	= nb_launder_page,
};


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
	ret = dbt_alloc_data_from_meta(&data_dbt, meta_dbt, block_get_num_by_position(offset));
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
	mutex_lock(&inode->i_mutex);
	page_start = offset >> PAGE_CACHE_SHIFT;
	page_end = (eof-1) >> PAGE_CACHE_SHIFT;
	for (i = page_start; i <= page_end; i++) {
		rcu_read_lock();
		pi = radix_tree_lookup(&inode->i_mapping->page_tree, i);
		rcu_read_unlock();
		if ( (!pi) && (!nb_data_exists_in_datadb(inode, offset))) {
			// no data: done for SEEK_HOLE, retry for SEEK_DATA if not yet reach eof
			if (whence == SEEK_DATA) {
				offset = (i + 1) << PAGE_CACHE_SHIFT;
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
				offset = (i + 1) << PAGE_CACHE_SHIFT;
				if (offset >= eof ){
					offset = eof;
					break;
				}
			}
		}
	}
	mutex_unlock(&inode->i_mutex);
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

static const struct file_operations ftfs_file_file_operations = {
	.llseek			= nb_llseek, //generic_file_llseek,
	.fsync			= nb_fsync,
	.read			= do_sync_read,
	.write			= do_sync_write,
	.aio_read		= generic_file_aio_read,
	.aio_write		= generic_file_aio_write,
	.mmap			= generic_file_mmap,
	.unlocked_ioctl = nb_file_ioctl,
#ifdef CONFIG_COMPAT
	.compat_ioctl   = nb_file_ioctl,
#endif
};

static const struct file_operations ftfs_dir_file_operations = {
	.read			= generic_read_dir,
	.iterate		= nb_readdir,
	.fsync			= nb_fsync,
	.release		= nb_dir_release,
	.unlocked_ioctl = nb_dir_ioctl,
#ifdef CONFIG_COMPAT
	.compat_ioctl   = nb_dir_ioctl,
#endif
};

static const struct inode_operations ftfs_file_inode_operations = {
	.setattr		= nb_setattr
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
};

static const struct inode_operations ftfs_symlink_inode_operations = {
	.setattr		= nb_setattr,
	.readlink		= generic_readlink,
	.follow_link		= nb_follow_link,
	.put_link		= nb_put_link,
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
static struct inode *
nb_setup_inode(struct super_block *sb, DBT *meta_dbt,
                 struct ftfs_metadata *meta)
{
	struct inode *i;
	struct ftfs_inode *ftfs_inode;

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
	ftfs_inode->last_nr_pages = 0;
	ftfs_inode->last_offset = ULONG_MAX;

	i->i_rdev = meta->st.st_dev;
	i->i_mode = meta->st.st_mode;
	set_nlink(i, meta->st.st_nlink);
	i->i_uid = KUIDT_INIT(meta->st.st_uid);
	i->i_gid = KGIDT_INIT(meta->st.st_gid);
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


	sbi->s_ftfs_info = alloc_percpu(struct ftfs_info);

	if (!sbi->s_ftfs_info)
		goto err;

	// DEP 8/19/19: In case subsequent steps fail during mount,
	// set this to NULL for graceful cleanup
	sbi->db_env = NULL;

	sb->s_fs_info = sbi;

	sb_set_blocksize(sb, FTFS_BSTORE_BLOCKSIZE);

	sb->s_op = &ftfs_super_ops;
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
	for_each_present_cpu(cpu) {
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

	ret = register_filesystem(&ftfs_fs_type);
	if (ret) {
		printk(KERN_ERR "FTFS ERROR: Failed to register filesystem\n");
	}

	return ret;
}

void exit_ftfs_fs(void)
{
	unregister_filesystem(&ftfs_fs_type);
}
