/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
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

#include "ftfs_fs.h"
#include "ftfs_profile.h"

#define DCACHE_FTFS_FLAG 0x04000000

struct ftfs_meta_key root_dir_meta_key = {
	META_KEY_MAGIC,
	0,
	""
};

static struct kmem_cache *ftfs_inode_cachep;

static inline struct ftfs_meta_key *
meta_key_move_to_target_directory(struct ftfs_meta_key *source,
                                  struct ftfs_meta_key *target,
                                  struct ftfs_meta_key *entry)
{
	struct ftfs_meta_key *newkey;
	int source_len = strlen(source->path);
	int target_len = strlen(target->path);
	int entry_len = strlen(entry->path);
	int path_size = target_len + (entry_len - source_len) + 1;
	newkey = kmalloc(sizeof(struct ftfs_meta_key) + path_size, GFP_KERNEL);
	if (newkey == NULL)
		return NULL;
	newkey->magic = META_KEY_MAGIC;
	newkey->circle_id = target->circle_id;
	snprintf(newkey->path, path_size, "%s%s", target->path, entry->path + source_len);
	return newkey;
}

static inline struct ftio *ftio_alloc(int nr_iovecs)
{
	struct ftio *ftio;

	ftio = kmalloc(sizeof(*ftio) + nr_iovecs * sizeof(struct ftio_vec),
	               GFP_KERNEL);
	if (unlikely(!ftio))
		return NULL;
	ftio->ft_max_vecs = nr_iovecs;
	ftio->ft_vcnt = 0;
	ftio->ft_bvidx = 0;

	return ftio;
}

static inline void ftio_free(struct ftio *ftio)
{
	kfree(ftio);
}

static int
ftfs_page_cmp(void *priv, struct list_head *a, struct list_head *b)
{
	struct page *a_pg = container_of(a, struct page, lru);
	struct page *b_pg = container_of(b, struct page, lru);

	if (a_pg->index < b_pg->index)
		return 1;
	else if (a_pg->index > b_pg->index)
		return -1;
	else
		return 0;
}

static inline void ftio_add_page(struct ftio *ftio, struct page *page)
{
	struct ftio_vec *fv;
	BUG_ON(ftio->ft_vcnt >= ftio->ft_max_vecs);
	fv = ftio->ft_io_vec + ftio->ft_vcnt++;
	fv->fv_page = page;
#if (FTFS_BSTORE_BLOCKSIZE != PAGE_CACHE_SIZE)
	fv->fv_offset = 0;
#endif
}

static inline void ftio_setup(struct ftio *ftio, struct list_head *pages,
                              int nr_pages, struct address_space *mapping)
{
	unsigned page_idx;

	list_sort(NULL, pages, ftfs_page_cmp);

	for (page_idx = 0; page_idx < nr_pages; page_idx++) {
		struct page *page = list_entry(pages->prev, struct page, lru);
		prefetchw(&page->flags);
		list_del(&page->lru);
		if (!add_to_page_cache_lru(page, mapping, page->index,
		                           GFP_KERNEL))
			ftio_add_page(ftio, page);
		page_cache_release(page);
	}
	BUG_ON(!list_empty(pages));
}

static inline void ftio_set_pages_uptodate(struct ftio *ftio)
{
	unsigned i;
	for (i = 0; i < ftio->ft_vcnt; i++)
		SetPageUptodate((ftio->ft_io_vec + i)->fv_page);
}

static inline void ftio_set_pages_error(struct ftio *ftio)
{
	unsigned i;
	for (i = 0; i < ftio->ft_vcnt; i++) {
		struct page *page = (ftio->ft_io_vec + i)->fv_page;
		ClearPageUptodate(page);
		SetPageError(page);
	}
}

static inline void ftio_unlock_pages(struct ftio *ftio)
{
	unsigned i;
	for (i = 0; i < ftio->ft_vcnt; i++)
		unlock_page((ftio->ft_io_vec + i)->fv_page);
}

/*
 * ftfs_i_init_once is passed to kmem_cache_create
 * Once an inode is allocated, this function is called to init that inode
 */
static void ftfs_i_init_once(void *inode)
{
	struct ftfs_inode *ftfs_inode = inode;

	ftfs_inode->key = NULL;

	inode_init_once(&ftfs_inode->vfs_inode);
}

static struct inode *
ftfs_setup_inode(struct super_block *sb, struct ftfs_meta_key *meta_key,
                 struct ftfs_metadata *meta);

static inline void
ftfs_setup_metadata(struct ftfs_metadata *meta, umode_t mode,
                    loff_t size, dev_t rdev, ino_t ino)
{
	struct timespec now_tspec;
	time_t now;

	now_tspec = current_kernel_time();
	TIMESPEC_TO_TIME_T(now, now_tspec);
	memset(meta, 0, sizeof(*meta));
	meta->u.st.st_ino = ino;
	meta->u.st.st_mode = mode;
	meta->u.st.st_size = size;
	meta->u.st.st_dev = rdev;
	meta->u.st.st_nlink = 1;
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	meta->u.st.st_uid = current_uid().val;
	meta->u.st.st_gid = current_gid().val;
#else
	meta->u.st.st_uid = current_uid();
	meta->u.st.st_gid = current_gid();
#endif
	meta->u.st.st_blocks = ((size + FTFS_BSTORE_BLOCKSIZE - 1) /
	                        FTFS_BSTORE_BLOCKSIZE);
	meta->u.st.st_blksize = FTFS_BSTORE_BLOCKSIZE;
	meta->u.st.st_atime = now;
	meta->u.st.st_mtime = now;
	meta->u.st.st_ctime = now;

	meta->nr_meta = 1;
	meta->nr_data = meta->u.st.st_blocks;
}

static inline void
ftfs_copy_metadata_from_inode(struct ftfs_metadata *meta, struct inode *inode)
{
	meta->type = FTFS_METADATA_TYPE_NORMAL;
	meta->nr_meta = FTFS_I(inode)->nr_meta;
	meta->nr_data = FTFS_I(inode)->nr_data;
	meta->u.st.st_ino = inode->i_ino;
	meta->u.st.st_dev = inode->i_rdev;
	meta->u.st.st_mode = inode->i_mode;
	meta->u.st.st_nlink = inode->i_nlink;
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	meta->u.st.st_uid = inode->i_uid.val;
	meta->u.st.st_gid = inode->i_gid.val;
#else
	meta->u.st.st_uid = inode->i_uid;
	meta->u.st.st_gid = inode->i_gid;
#endif
	meta->u.st.st_size = i_size_read(inode);
	meta->u.st.st_blocks = ((meta->u.st.st_size + FTFS_BSTORE_BLOCKSIZE - 1) /
	                        FTFS_BSTORE_BLOCKSIZE);
	TIMESPEC_TO_TIME_T(meta->u.st.st_atime, inode->i_atime);
	TIMESPEC_TO_TIME_T(meta->u.st.st_mtime, inode->i_mtime);
	TIMESPEC_TO_TIME_T(meta->u.st.st_ctime, inode->i_ctime);
}

static inline struct ftfs_meta_key *ftfs_get_read_lock(struct ftfs_inode *f_inode)
{
	down_read(&f_inode->key_lock);
	return f_inode->key;
}

static inline int ftfs_get_read_trylock(struct ftfs_inode *f_inode)
{
	return down_read_trylock(&f_inode->key_lock);
}


static inline void ftfs_put_read_lock(struct ftfs_inode *f_inode)
{
	up_read(&f_inode->key_lock);
}

static inline struct ftfs_meta_key *ftfs_get_write_lock(struct ftfs_inode *f_inode)
{
	down_write(&f_inode->key_lock);
	return f_inode->key;
}

static inline void ftfs_put_write_lock(struct ftfs_inode *f_inode)
{
	up_write(&f_inode->key_lock);
}

// if the txn eventually aborts, db ino value would be changed in retrial
// this is dangerous;
static int ftfs_next_ino(struct ftfs_sb_info *sbi, DB_TXN *txn, ino_t *ino)
{
	unsigned int cpu;
	ino_t need_update;

	cpu = get_cpu();
	*ino = per_cpu_ptr(sbi->s_ftfs_info, cpu)->next_ino;
	per_cpu_ptr(sbi->s_ftfs_info, cpu)->next_ino += sbi->s_nr_cpus;
	if (*ino >= per_cpu_ptr(sbi->s_ftfs_info, cpu)->max_ino) {
		need_update = per_cpu_ptr(sbi->s_ftfs_info, cpu)->max_ino +
		              sbi->s_nr_cpus * FTFS_INO_INC;
		per_cpu_ptr(sbi->s_ftfs_info, cpu)->max_ino = need_update;
	} else
		need_update = 0;
	put_cpu();

	if (need_update)
		ftfs_bstore_update_ino(sbi, need_update);

	return 0;
}

// iget must free 'meta_key' if it is not passed to inode
static struct inode *ftfs_iget(struct super_block *sb,
                               struct ftfs_meta_key *meta_key, DB_TXN *txn)
{
	int ret;
	struct inode *inode;
	struct ftfs_sb_info *sbi = sb->s_fs_info;
	struct ftfs_metadata meta;

	ret = ftfs_bstore_meta_get(sbi->meta_db, meta_key, txn, &meta);
	if (ret) {
#ifdef TXN_ON
		IF_DBOP_CONFLICT(ret)
			return ERR_PTR(ret);
#endif
		meta_key_free(meta_key);
		return ERR_PTR(-ENOENT);
	}
	if (meta.type == FTFS_METADATA_TYPE_REDIRECT) {
		struct ftfs_meta_key *real_meta_key;

		meta_key_free(meta_key);
		real_meta_key = alloc_meta_key_from_circle_id(meta.u.circle_id);
		if (!real_meta_key)
			return ERR_PTR(-ENOMEM);
		ret = ftfs_bstore_meta_get(sbi->meta_db, real_meta_key,
		                           txn, &meta);
		if (ret) {
			BUG_ON(ret == -ENOENT);
			meta_key_free(real_meta_key);
#ifdef TXN_ON
			IF_DBOP_CONFLICT(ret)
				return ERR_PTR(ret);
#endif
			return ERR_PTR(ret);
		}
		BUG_ON(meta.type == FTFS_METADATA_TYPE_REDIRECT);

		inode = ftfs_setup_inode(sb, real_meta_key, &meta);
		if (IS_ERR(inode))
			meta_key_free(real_meta_key);

		return inode;
	}

	inode = ftfs_setup_inode(sb, meta_key, &meta);
	if (IS_ERR(inode))
		meta_key_free(meta_key);

	return inode;
}

static inline int
ftfs_do_unlink(struct ftfs_meta_key *meta_key, DB_TXN *txn,
               struct inode *inode, struct ftfs_sb_info *sbi)
{
	int ret;
	loff_t size;

	ret = ftfs_bstore_meta_del(sbi->meta_db, meta_key, txn);
	if (ret)
		return ret;

	size = i_size_read(inode);
	if (size > 0)
		ret = ftfs_bstore_truncate(sbi->data_db, meta_key, txn,
		                           0, FTFS_UINT64_MAX);

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
	struct ftfs_meta_key *meta_key;
	uint64_t object_circleid;
	uint64_t current_circleid;
	this_parent = object;
	object_circleid = meta_key_get_circle_id(FTFS_I(object->d_inode)->key);

start:
	if (this_parent->d_sb != object->d_sb)
		goto end;
	inode = this_parent->d_inode;
	if (inode == NULL)
		goto repeat;
	if (this_parent != object) {
		meta_key = ftfs_get_write_lock(FTFS_I(inode));
		current_circleid = meta_key_get_circle_id(meta_key);
		if (current_circleid != object_circleid) {
			ftfs_put_write_lock(FTFS_I(inode));
			goto end;
		}
		list_add(&FTFS_I(inode)->rename_locked, locked);
	}
repeat:
	next = this_parent->d_subdirs.next;
resume:
	while (next != &this_parent->d_subdirs) {
		this_parent = list_entry(next, struct dentry, d_u.d_child);
		goto start;
	}
end:
	if (this_parent != object) {
		next = this_parent->d_u.d_child.next;
		this_parent = this_parent->d_parent;
		goto resume;
	}
	return 0;
}

static int unlock_children_after_rename(struct list_head *locked)
{
	struct ftfs_inode *f_inode, *tmp;

	list_for_each_entry_safe(f_inode, tmp, locked, rename_locked) {
		ftfs_put_write_lock(f_inode);
		list_del_init(&f_inode->rename_locked);
	}
	return 0;
}

static int
ftfs_update_ftfs_inode_keys(struct list_head *locked,
                            struct ftfs_meta_key *old_meta_key,
                            struct ftfs_meta_key *new_meta_key)
{
	struct ftfs_inode *f_inode, *tmp;
	struct ftfs_meta_key *old_mkey, *new_mkey;
	list_for_each_entry_safe(f_inode, tmp, locked, rename_locked) {
		old_mkey = f_inode->key;
		new_mkey = meta_key_move_to_target_directory(old_meta_key, new_meta_key, old_mkey);
		if (new_mkey == NULL)
			BUG();
		f_inode->key = new_mkey;
		meta_key_free(old_mkey);
	}
	return 0;
}

static int split_circle(struct dentry *dentry);

#define FTFS_UPDATE_NR_MAY_SPLIT 0x1

static void inline
ftfs_update_nr(struct dentry *dentry, int meta_change, int data_change, unsigned flag)
{
	struct inode *inode;
	struct ftfs_meta_key *meta_key;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	int end;

	if (unlikely(meta_change == 0 && data_change == 0))
		return;

	do {
		inode = dentry->d_inode;
		meta_key = ftfs_get_read_lock(FTFS_I(inode));
		end = meta_key_is_circle_root(meta_key);
		ftfs_put_read_lock(FTFS_I(inode));
		if (!end && (flag & FTFS_UPDATE_NR_MAY_SPLIT) &&
		    (FTFS_I(inode)->nr_meta + meta_change >= sbi->max_circle_size ||
		     FTFS_I(inode)->nr_data + data_change >= sbi->max_circle_size))
			end = !split_circle(dentry);
		FTFS_I(inode)->nr_meta += meta_change;
		FTFS_I(inode)->nr_data += data_change;
		mark_inode_dirty(inode);
		if (end)
			break;
		dentry = dentry->d_parent;
	} while (1);
}

#define ftfs_update_nr_inode(inode, meta_change, data_change, flag) do { \
		struct dentry *de = d_find_alias(inode); \
		BUG_ON(de == NULL); \
		spin_lock(&de->d_lock); \
		de->d_flags |= DCACHE_FTFS_FLAG; \
		spin_unlock(&de->d_lock); \
		ftfs_update_nr(de, meta_change, data_change, flag); \
		dput(de); \
		spin_lock(&de->d_lock); \
		de->d_flags &= ~DCACHE_FTFS_FLAG; \
		spin_unlock(&de->d_lock); \
	} while (0)

static int split_circle(struct dentry *dentry)
{
	int ret;
	uint64_t circle_id;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_meta_key *old_meta_key, *new_meta_key;
	struct inode *inode = dentry->d_inode;
	struct ftfs_metadata meta;
	LIST_HEAD(locked_children);
	DB_TXN *txn;

	trace_printk("split_circle is called\n");
	old_meta_key = ftfs_get_write_lock(FTFS_I(inode));
	if (meta_key_is_circle_root(old_meta_key)) {
		ftfs_put_write_lock(FTFS_I(inode));
		return 0;
	}
	circle_id = inode->i_ino;
	new_meta_key = alloc_meta_key_from_circle_id(circle_id);
	if (!new_meta_key) {
		ftfs_put_write_lock(FTFS_I(inode));
		return -ENOMEM;
	}
	prelock_children_for_rename(dentry, &locked_children);
	ftfs_copy_metadata_from_inode(&meta, inode);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_move(sbi->meta_db, sbi->data_db,
	                       old_meta_key, new_meta_key, txn,
	                       0, &meta);
	if (ret)
		goto abort;
	meta.type = FTFS_METADATA_TYPE_REDIRECT;
	meta.u.circle_id = circle_id;
	ret = ftfs_bstore_meta_put(sbi->meta_db, old_meta_key, txn, &meta);
	if (ret)
		goto abort;
	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	if (!ret) {
		ftfs_update_ftfs_inode_keys(&locked_children, old_meta_key, new_meta_key);
		FTFS_I(inode)->key = new_meta_key;
		meta_key_free(old_meta_key);
	}

unlock_out:
	unlock_children_after_rename(&locked_children);
	ftfs_put_write_lock(FTFS_I(inode));

	if (!ret) {
		ftfs_update_nr(dentry->d_parent,
		               -(FTFS_I(inode)->nr_meta - 1),
		               -FTFS_I(inode)->nr_data,
		               0);
	}

	return ret;

abort:
	ftfs_bstore_txn_abort(txn);
	meta_key_free(new_meta_key);
	goto unlock_out;
}

static int merge_circle(struct dentry *dentry)
{
	int ret;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_meta_key *old_meta_key, *new_meta_key, *parent_meta_key;
	struct inode *inode = dentry->d_inode;
	struct ftfs_metadata meta;
	struct inode *parent_inode = dentry->d_parent->d_inode;
	LIST_HEAD(locked_children);
	DB_TXN *txn;

	BUG_ON(dentry->d_parent == dentry);
	parent_meta_key = ftfs_get_read_lock(FTFS_I(parent_inode));
	old_meta_key = ftfs_get_write_lock(FTFS_I(inode));
	if (!meta_key_is_circle_root(old_meta_key)) {
		ret = -1;
		goto out;
	}
	new_meta_key = alloc_child_meta_key_from_meta_key(parent_meta_key, dentry->d_name.name);
	if (!new_meta_key) {
		ret = -ENOMEM;
		goto out;
	}
	prelock_children_for_rename(dentry, &locked_children);
	ftfs_copy_metadata_from_inode(&meta, inode);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_move(sbi->meta_db, sbi->data_db,
	                       old_meta_key, new_meta_key, txn,
	                       0, &meta);

	if (ret)
		goto abort;
	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	if (!ret) {
		ftfs_update_ftfs_inode_keys(&locked_children, old_meta_key, new_meta_key);
		FTFS_I(inode)->key = new_meta_key;
		meta_key_free(old_meta_key);
	} else {
		meta_key_free(new_meta_key);
	}

unlock_out:
	unlock_children_after_rename(&locked_children);
out:
	ftfs_put_write_lock(FTFS_I(inode));
	ftfs_put_read_lock(FTFS_I(parent_inode));

	if (!ret) {
		ftfs_update_nr(dentry->d_parent,
		               FTFS_I(inode)->nr_meta - 1,
		               FTFS_I(inode)->nr_data,
		               0);
	}

	return ret;

abort:
	ftfs_bstore_txn_abort(txn);
	meta_key_free(new_meta_key);
	goto unlock_out;
}

// change from d_find_alias
static struct dentry *ftfs_find_dentry(struct inode *inode)
{
	struct dentry *de = NULL;

	if (!hlist_empty(&inode->i_dentry)) {
		spin_lock(&inode->i_lock);
		hlist_for_each_entry(de, &inode->i_dentry, d_alias) {
			spin_lock(&de->d_lock);
			if (!IS_ROOT(de) && !d_unhashed(de)) {
				//__dget_dlock(de);
				de->d_lockref.count++;
				de->d_flags |= DCACHE_FTFS_FLAG;
				spin_unlock(&de->d_lock);
				goto out;
			}
			spin_unlock(&de->d_lock);
		}
out:
		spin_unlock(&inode->i_lock);
	}

	return de;
}

#if 0
static struct dentry *ftfs_find_dentry_lock(struct inode *inode)
{
	struct dentry *de = NULL;

	if (!hlist_empty(&inode->i_dentry)) {
		spin_lock(&inode->i_lock);
		hlist_for_each_entry(de, &inode->i_dentry, d_alias) {
			spin_lock(&de->d_lock);
			if (!IS_ROOT(de) && !d_unhashed(de)) {
				de->d_lockref.count++;
				goto out;
			}
			spin_unlock(&de->d_lock);
		}
out:
		spin_unlock(&inode->i_lock);
	}

	return de;
}
#endif

static void ftfs_dput(struct dentry *de)
{
	if (de == NULL)
		return;
	spin_lock(&de->d_lock);
	de->d_lockref.count--;
	de->d_flags &= (~DCACHE_FTFS_FLAG);
	spin_unlock(&de->d_lock);
}

static int inline maybe_merge_circle(struct inode *inode)
{
	int ret;
	struct dentry *it_dentry, *dentry;
	struct ftfs_sb_info *sbi;
	struct inode *it_inode;
	struct ftfs_meta_key *meta_key;

	dentry = ftfs_find_dentry(inode);
	ret = -1;
	if (dentry == NULL)
		goto out;
	it_dentry = dentry->d_parent;
	sbi = dentry->d_sb->s_fs_info;
	do {
		it_inode = it_dentry->d_inode;
		meta_key = ftfs_get_read_lock(FTFS_I(it_inode));
		ret = !meta_key_is_circle_root(meta_key);
		ftfs_put_read_lock(FTFS_I(it_inode));
		if (!ret ||
		    FTFS_I(it_inode)->nr_meta + FTFS_I(inode)->nr_meta - 1 >= sbi->max_circle_size ||
		    FTFS_I(it_inode)->nr_data + FTFS_I(inode)->nr_data >= sbi->max_circle_size)
			break;
		it_dentry = it_dentry->d_parent;
	} while (1);

	if (!ret) {
		ret = merge_circle(dentry);
	}

	ftfs_dput(dentry);
out:
	return ret;
}

static int ftfs_readpage(struct file *file, struct page *page)
{
	int ret;
	struct inode *inode = page->mapping->host;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	struct ftfs_meta_key *meta_key;
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	DB_TXN *txn;

	meta_key = ftfs_get_read_lock(ftfs_inode);

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = ftfs_bstore_scan_one_page(sbi->data_db, meta_key, txn, page);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
	} else {
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	ftfs_put_read_lock(ftfs_inode);

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

static int ftfs_readpages(struct file *filp, struct address_space *mapping,
                          struct list_head *pages, unsigned nr_pages)
{
	int ret;
	struct ftfs_sb_info *sbi = mapping->host->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(mapping->host);
	struct ftfs_meta_key *meta_key;
	struct ftio *ftio;
	DB_TXN *txn;

	ftio = ftio_alloc(nr_pages);
	if (!ftio)
		return -ENOMEM;
	ftio_setup(ftio, pages, nr_pages, mapping);

	meta_key = ftfs_get_read_lock(ftfs_inode);

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);

	ret = ftfs_bstore_scan_pages(sbi->data_db, meta_key, txn, ftio);

	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
	} else {
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	ftfs_put_read_lock(ftfs_inode);

	if (ret)
		ftio_set_pages_error(ftio);
	else
		ftio_set_pages_uptodate(ftio);
	ftio_unlock_pages(ftio);
	ftio_free(ftio);

	return ret;
}

static int inline
__ftfs_writen(struct ftfs_meta_key *meta_key, struct page *page,
              size_t len, DB_TXN *txn, int is_seq)
{
#if (PAGE_CACHE_SIZE == FTFS_BSTORE_BLOCKSIZE)
	int ret;
	char *buf;
	struct inode *inode;
	uint64_t block_num;
	struct ftfs_sb_info *sbi;
	struct ftfs_data_key *data_key;

	inode = page->mapping->host;
	sbi = inode->i_sb->s_fs_info;
	buf = kmap(page);
	block_num = page->index;
	data_key = alloc_data_key_from_meta_key(meta_key, block_num);
	if (!data_key) {
		ret = -ENOMEM;
		goto out;
	}
	ret = ftfs_bstore_put(sbi->data_db, data_key, txn, buf, len, is_seq);

	data_key_free(data_key);
out:
	kunmap(page);

	return ret;
#else
	BUG_ON();
#endif
}

static int inline
__ftfs_updaten(struct ftfs_meta_key *meta_key, struct page *page,
               size_t len, loff_t offset, DB_TXN *txn)
{
#if (PAGE_CACHE_SIZE == FTFS_BSTORE_BLOCKSIZE)
	int ret;
	char *buf;
	struct inode *inode;
	uint64_t block_num;
	size_t block_offset;
	struct ftfs_sb_info *sbi;
	struct ftfs_data_key *data_key;

	inode = page->mapping->host;
	sbi = inode->i_sb->s_fs_info;
	buf = kmap(page);
	buf = &buf[offset & ~PAGE_CACHE_MASK];
	block_num = block_get_num_by_position(offset);
	block_offset = block_get_offset_by_position(offset);
	data_key = alloc_data_key_from_meta_key(meta_key, block_num);
	if (!data_key) {
		ret = -ENOMEM;
		goto out;
	}
	ret = ftfs_bstore_update(sbi->data_db, data_key, txn, buf,
	                         len, block_offset);

	data_key_free(data_key);
out:
	kunmap(page);

	return ret;
#else
	BUG_ON();
#endif
}

static int inline
__ftfs_writepage(struct page *page, struct writeback_control *wbc,
                 DB_TXN *txn, struct ftfs_meta_key *meta_key, int is_seq)
{
	int ret;
	struct inode *inode = page->mapping->host;
	loff_t i_size;
	pgoff_t end_index;
	unsigned offset;

	i_size = i_size_read(inode);
	end_index = i_size >> PAGE_CACHE_SHIFT;
	if (page->index < end_index)
		ret = __ftfs_writen(meta_key, page, PAGE_CACHE_SIZE,
		                    txn, is_seq);
	else {
		offset = i_size & (PAGE_CACHE_SIZE - 1);
		if (page->index >= end_index + 1 || !offset)
			ret = 0;
		else
			ret = __ftfs_writen(meta_key, page, offset,
			                    txn, is_seq);
	}

	if (ret) {
		if (ret == -EAGAIN) {
			redirty_page_for_writepage(wbc, page);
			ret = 0;
		} else {
			SetPageError(page);
			mapping_set_error(page->mapping, ret);
		}
	}

	return ret;
}

static int
ftfs_writepage(struct page *page, struct writeback_control *wbc)
{
	int ret;
	struct ftfs_meta_key *meta_key;
	struct inode *inode = page->mapping->host;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	DB_TXN *txn;

	meta_key = ftfs_get_read_lock(FTFS_I(inode));
	set_page_writeback(page);
	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = __ftfs_writepage(page, wbc, txn, meta_key, 0);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
		goto out;
	}
	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
	end_page_writeback(page);

out:
	ftfs_put_read_lock(FTFS_I(inode));
	unlock_page(page);

	return ret;
}

static inline void *radix_indirect_to_ptr(void *ptr)
{
	return (void *)((unsigned long)ptr & ~RADIX_TREE_INDIRECT_PTR);
}

/**
 * (copied from lib/radix-tree.c:radix_tree_gang_lookup_tagged())
 *	radix_tree_tag_count_exceeds - perform multiple lookup on a radix tree
 *	                             based on a tag
 *	@root:		radix tree root
 *	@results:	where the results of the lookup are placed
 *	@first_index:	start the lookup from this key
 *	@max_items:	place up to this many items at *results
 *	@tag:		the tag index (< RADIX_TREE_MAX_TAGS)
 *
 *	Performs an index-ascending scan of the tree for present items which
 *	have the tag indexed by @tag set.  Places the items at *@results and
 *	returns the number of items which were placed at *@results.
 */
static unsigned int
radix_tree_tag_count_exceeds(struct radix_tree_root *root,
			unsigned long first_index, unsigned int threshold,
			unsigned int tag)
{
	struct radix_tree_iter iter;
	void **slot;
	unsigned int count = 0;

	if (unlikely(!threshold))
		return 0;

	radix_tree_for_each_tagged(slot, root, &iter, first_index, tag) {
		if (!radix_indirect_to_ptr(rcu_dereference_raw(*slot)))
			continue;
		if (++count == threshold)
			return 1;
	}

	return 0;
}

struct ftfs_writepages_vec {
	struct list_head list;
	int nr_pages;
	struct page *pages[];
};
#define FTFS_WRITEPAGES_VEC_SIZE 1024
#define FTFS_WRITEPAGES_VEC_LIST_INIT_SIZE 16
struct ftfs_writepages_vec *ftfs_alloc_writepages_vec(void)
{
	struct ftfs_writepages_vec *ret;
	ret = kmalloc(sizeof(struct ftfs_writepages_vec) +
	              sizeof(struct page *) * FTFS_WRITEPAGES_VEC_SIZE,
	              GFP_KERNEL);
	if (ret != NULL)
		INIT_LIST_HEAD(&ret->list);
	return ret;
}
struct mutex ftfs_writepages_vec_list_mutex;
LIST_HEAD(ftfs_writepages_vec_list);

static int
__ftfs_writepages_write_pages(struct ftfs_writepages_vec *writepages_vec,
                              struct writeback_control *wbc,
                              struct inode *inode, struct ftfs_sb_info *sbi,
                              struct ftfs_data_key *data_key, int is_seq)
{
	int i, ret;
	loff_t i_size;
	pgoff_t end_index;
	unsigned offset;
	char *buf;
	struct page *page;
	struct ftfs_meta_key *meta_key;
	DB_TXN *txn;

	meta_key = ftfs_get_read_lock(FTFS_I(inode));
	if (unlikely(meta_key->circle_id != data_key->circle_id ||
	             strcmp(meta_key->path, data_key->path) != 0))
		copy_data_key_from_meta_key(data_key, meta_key, 0);
retry:
	i_size = i_size_read(inode);
	end_index = i_size >> PAGE_CACHE_SHIFT;
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	for (i = 0; i < writepages_vec->nr_pages; i++) {
		page = writepages_vec->pages[i];
		data_key->block_num = page->index;
		buf = kmap(page);
		if (page->index < end_index)
			ret = ftfs_bstore_put(sbi->data_db, data_key, txn, buf, PAGE_CACHE_SIZE, is_seq);
		else {
			offset = i_size & (PAGE_CACHE_SIZE - 1);
			if (page->index > end_index + 1 || !offset)
				ret = 0;
			else
				ret = ftfs_bstore_put(sbi->data_db, data_key, txn, buf, offset, is_seq);
		}
		kunmap(page);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			ftfs_bstore_txn_abort(txn);
			goto out;
		}
	}
	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
out:
	ftfs_put_read_lock(FTFS_I(inode));
	for (i = 0; i < writepages_vec->nr_pages; i++) {
		page = writepages_vec->pages[i];
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
static int ftfs_writepages(struct address_space *mapping,
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
	struct ftfs_meta_key *meta_key;
	struct ftfs_data_key *data_key;
	struct ftfs_writepages_vec *writepages_vec;

	writepages_vec = NULL;
	mutex_lock(&ftfs_writepages_vec_list_mutex);
	if (!list_empty(&ftfs_writepages_vec_list)) {
		writepages_vec = list_entry(ftfs_writepages_vec_list.next,
		                            struct ftfs_writepages_vec, list);
		list_del_init(&writepages_vec->list);
	}
	mutex_unlock(&ftfs_writepages_vec_list_mutex);
	if (writepages_vec == NULL) {
		writepages_vec = ftfs_alloc_writepages_vec();
		if (writepages_vec == NULL)
			return -ENOMEM;
	}
	writepages_vec->nr_pages = 0;

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

	/* wkj: add count of total pages for writeback: we need to
	 * detect sequential I/Os somehow. */
	if (range_whole || (end - index >= LARGE_IO_THRESHOLD))
		is_seq = radix_tree_tag_count_exceeds(&mapping->page_tree,
		                           	index, LARGE_IO_THRESHOLD, tag);

	inode = mapping->host;
	sbi = inode->i_sb->s_fs_info;
	meta_key = ftfs_get_read_lock(FTFS_I(inode));
	if (i_size_read(inode) >= sbi->max_file_size &&
	    !meta_key_is_circle_root(meta_key)) {
		struct dentry *de;
		de = ftfs_find_dentry(inode);
		if (de) {
			ftfs_put_read_lock(FTFS_I(inode));
			split_circle(de);
			ftfs_dput(de);
			meta_key = ftfs_get_read_lock(FTFS_I(inode));
		}
	}
	data_key = kmalloc(DATA_KEY_MAX_LEN, GFP_KERNEL);
	if (!data_key) {
		ftfs_put_read_lock(FTFS_I(inode));
		ret = -ENOMEM;
		goto out;
	}
	copy_data_key_from_meta_key(data_key, meta_key, 0);
	ftfs_put_read_lock(FTFS_I(inode));
	while (!done && (index <= end)) {
		nr_pages = pagevec_lookup_tag(&pvec, mapping, &index, tag,
			      min(end - index, (pgoff_t)PAGEVEC_SIZE-1) + 1);
		if (nr_pages == 0)
			break;

		for (i = 0; i < nr_pages; i++) {
			struct page *page = pvec.pages[i];

			if (page->index > end) {
				if (writepages_vec->nr_pages > 0) {
					ret = __ftfs_writepages_write_pages(writepages_vec, wbc, inode, sbi, data_key, is_seq);
                    if (ret)
                        goto free_dkey_out;
					done_index = txn_done_index;
					writepages_vec->nr_pages = 0;
				}
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
			writepages_vec->pages[writepages_vec->nr_pages++] = page;
			if (writepages_vec->nr_pages >= FTFS_WRITEPAGES_VEC_SIZE) {
				ret = __ftfs_writepages_write_pages(writepages_vec, wbc, inode, sbi, data_key, is_seq);
				if (ret)
					goto free_dkey_out;
				done_index = txn_done_index;
				writepages_vec->nr_pages = 0;
			}

			if (--wbc->nr_to_write <= 0 &&
			    wbc->sync_mode == WB_SYNC_NONE) {
				if (writepages_vec->nr_pages > 0) {
					ret = __ftfs_writepages_write_pages(writepages_vec, wbc, inode, sbi, data_key, is_seq);
                    if (ret)
                        goto free_dkey_out;
					done_index = txn_done_index;
					writepages_vec->nr_pages = 0;
				}
				done = 1;
				break;
			}
		}
		pagevec_release(&pvec);
		cond_resched();
	}
	if (writepages_vec->nr_pages > 0) {
		ret = __ftfs_writepages_write_pages(writepages_vec, wbc, inode, sbi, data_key, is_seq);
		if (!ret) {
			done_index = txn_done_index;
			writepages_vec->nr_pages = 0;
		}
	}
free_dkey_out:
	kfree(data_key);
out:
	if (!cycled && !done) {
		cycled = 1;
		index = 0;
		end = writeback_index - 1;
		goto retry;
	}
	if (wbc->range_cyclic || (range_whole && wbc->nr_to_write > 0))
		mapping->writeback_index = done_index;

	mutex_lock(&ftfs_writepages_vec_list_mutex);
	list_add(&writepages_vec->list, &ftfs_writepages_vec_list);
	mutex_unlock(&ftfs_writepages_vec_list_mutex);

	return ret;
}

static int
ftfs_write_begin(struct file *file, struct address_space *mapping,
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
ftfs_write_end(struct file *file, struct address_space *mapping,
               loff_t pos, unsigned len, unsigned copied,
               struct page *page, void *fsdata)
{
	/* make sure that ftfs can't guarantee uptodate page */
	loff_t last_pos = pos + copied;
	struct inode *inode = page->mapping->host;
	int ret;

	/*
	 * 1. if page is uptodate/writesize=PAGE_SIZE (we have new content),
	 *    write to page cache and wait for generic_writepage to write
	 *    to disk (generic aio style);
	 * 2. if not, only write to disk so that we avoid read-before-write.
	 */
	if (PageDirty(page) || copied == PAGE_CACHE_SIZE) {
		goto postpone_to_writepage;
	} else if (page_offset(page) >= i_size_read(inode)) {
		char *buf = kmap(page);
		if (pos & ~PAGE_CACHE_MASK)
			memset(buf, 0, pos & ~PAGE_CACHE_MASK);
		if (last_pos & ~PAGE_CACHE_MASK)
			memset(buf + (last_pos & ~PAGE_CACHE_MASK),
			       0, PAGE_CACHE_SIZE - (last_pos & ~PAGE_CACHE_MASK));
		kunmap(page);
postpone_to_writepage:
		SetPageUptodate(page);
		if (!PageDirty(page))
			__set_page_dirty_nobuffers(page);
	} else {
		struct ftfs_meta_key *meta_key =
			ftfs_get_read_lock(FTFS_I(inode));
		DB_TXN *txn;
		TXN_GOTO_LABEL(retry);
		ftfs_bstore_txn_begin(((struct ftfs_sb_info *)inode->i_sb->s_fs_info)->db_env,
		                      NULL, &txn, TXN_MAY_WRITE);
		ret = __ftfs_updaten(meta_key, page, copied, pos, txn);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			ftfs_bstore_txn_abort(txn);
			goto out;
		}
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
out:
		ftfs_put_read_lock(FTFS_I(inode));
		BUG_ON(ret);
		clear_page_dirty_for_io(page);
	}

	unlock_page(page);
	page_cache_release(page);

	/* holding i_mutconfigex */
	if (last_pos > i_size_read(inode)) {
		if (last_pos >= ((struct ftfs_sb_info *)inode->i_sb->s_fs_info)->max_file_size) {
			struct ftfs_meta_key *meta_key = ftfs_get_read_lock(FTFS_I(inode));
			struct dentry *de;
			if (!meta_key_is_circle_root(meta_key)) {
				ftfs_put_read_lock(FTFS_I(inode));
				de = ftfs_find_dentry(inode);
				BUG_ON(!de);
				if (split_circle(de)) {
					ftfs_dput(de);
					goto split_fail;
				}
				ftfs_dput(de);
			} else
				ftfs_put_read_lock(FTFS_I(inode));
		} else {
			int dc;

split_fail:
			dc = ftfs_get_block_num_by_size(last_pos)
			     - FTFS_I(inode)->nr_data;
			// the file doesn't form a circle itself
			if (dc > 0) {
				struct dentry *de = ftfs_find_dentry(inode);
				if (de) {
					ftfs_update_nr(de->d_parent, 0, dc, FTFS_UPDATE_NR_MAY_SPLIT);
					ftfs_dput(de);
				}
			}
		}
		FTFS_I(inode)->nr_data = ftfs_get_block_num_by_size(last_pos);
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
static int ftfs_launder_page(struct page *page)
{
	printk(KERN_CRIT "laundering page.\n");
	BUG();
}

static int ftfs_rename(struct inode *old_dir, struct dentry *old_dentry,
                       struct inode *new_dir, struct dentry *new_dentry)
{
	int ret, err;
	struct inode *old_inode, *new_inode;
	LIST_HEAD(locked_children);
	DB_TXN *txn;
	struct ftfs_sb_info *sbi = old_dir->i_sb->s_fs_info;
	struct ftfs_meta_key *old_meta_key, *new_meta_key,
	                     *old_dir_meta_key, *new_dir_meta_key;
	int old_mc, old_dc, new_mc, new_dc;
	struct ftfs_metadata old_meta;

	// to prevent any other move from happening, we grab sem of parents
	old_dir_meta_key = ftfs_get_read_lock(FTFS_I(old_dir));
	new_dir_meta_key = ftfs_get_read_lock(FTFS_I(new_dir));

	old_inode = old_dentry->d_inode;
	old_meta_key = ftfs_get_write_lock(FTFS_I(old_inode));
	prelock_children_for_rename(old_dentry, &locked_children);
	new_inode = new_dentry->d_inode;
	new_meta_key = new_inode ? ftfs_get_write_lock(FTFS_I(new_inode)) : NULL;

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	if (!meta_key_is_circle_root(old_meta_key)) {
		new_mc = FTFS_I(old_inode)->nr_meta;
		new_dc = FTFS_I(old_inode)->nr_data;
	}
	else {
		new_mc = 1;
		new_dc = 0;
	}
	old_mc = -new_mc;
	old_dc = -new_dc;
	if (new_inode) {
		// we either delete an emptry dir or one file,
		new_mc -= 1;
		if (!meta_key_is_circle_root(new_meta_key))
			new_dc -= FTFS_I(new_inode)->nr_data;
		if (S_ISDIR(old_inode->i_mode)) {
			if (!S_ISDIR(new_inode->i_mode)) {
				ret = -ENOTDIR;
				goto abort;
			}
			err = ftfs_directory_is_empty(sbi->meta_db,
			                              new_meta_key,
			                              txn, &ret);
			if (err) {
				DBOP_JUMP_ON_CONFLICT(err, retry);
				BUG();
			}
			if (!ret) {
				ret = -ENOTEMPTY;
				goto abort;
			}
			// there will be a put later, so we don't need to
			// delete meta here. but if it is a circle root,
			// we need to perform delete and avoid future update
			// to that inode.
			if (meta_key_is_circle_root(new_meta_key)) {
				ret = ftfs_bstore_meta_del(sbi->meta_db,
				                           new_meta_key,
				                           txn);
				if (ret) {
					DBOP_JUMP_ON_CONFLICT(ret, retry);
					goto abort;
				}
			}
		} else {
			if (S_ISDIR(new_inode->i_mode)) {
				ret = -ENOTDIR;
				goto abort;
			}
			// basically the same as dirs, but we also have to
			// take care of data entries and hard links
			if (!meta_key_is_circle_root(new_meta_key) ||
			    new_inode->i_nlink == 1) {
				ret = ftfs_do_unlink(new_meta_key, txn,
				                     new_inode, sbi);
				if (ret) {
					DBOP_JUMP_ON_CONFLICT(ret, retry);
					goto abort;
				}
			}
		}
	}

	new_meta_key = alloc_child_meta_key_from_meta_key(new_dir_meta_key, new_dentry->d_name.name);
	if (!new_meta_key) {
		ret = -ENOMEM;
		goto abort;
	}

	if (meta_key_is_circle_root(old_meta_key)) {
		struct ftfs_meta_key *old_redirect_meta_key =
			alloc_child_meta_key_from_meta_key(old_dir_meta_key,
			                                   old_dentry->d_name.name);

		old_meta.type = FTFS_METADATA_TYPE_REDIRECT;
		old_meta.u.circle_id = old_meta_key->circle_id;

		if (!old_redirect_meta_key) {
			ret = -ENOMEM;
			goto abort1;
		}
		ret = ftfs_bstore_move(sbi->meta_db, sbi->data_db,
		                       old_redirect_meta_key, new_meta_key,
		                       txn, FTFS_BSTORE_MOVE_NO_DATA, &old_meta);
		meta_key_free(old_redirect_meta_key);
	} else {
		ftfs_copy_metadata_from_inode(&old_meta, old_inode);
		ret = ftfs_bstore_move(sbi->meta_db, sbi->data_db,
		                       old_meta_key, new_meta_key, txn,
		                       0, &old_meta);
	}

	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort1;
	}

	if (!meta_key_is_circle_root(old_meta_key)) {
		ret = ftfs_update_ftfs_inode_keys(&locked_children, old_meta_key, new_meta_key);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			goto abort1;
		}
		FTFS_I(old_inode)->key = new_meta_key;
		meta_key_free(old_meta_key);
	}

	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	unlock_children_after_rename(&locked_children);
	ftfs_put_write_lock(FTFS_I(old_inode));
	if (new_inode) {
		if (new_inode->i_nlink > 1) {
			drop_nlink(new_inode);
			mark_inode_dirty(new_inode);
		} else {
			// basically, we are trying to avoid any future updates
			spin_lock(&new_inode->i_lock);
			i_size_write(new_inode, 0);
			new_inode->i_state &= ~I_DIRTY;
			spin_unlock(&new_inode->i_lock);
		}
		ftfs_put_write_lock(FTFS_I(new_inode));
	}
	ftfs_put_read_lock(FTFS_I(old_dir));
	ftfs_put_read_lock(FTFS_I(new_dir));

	ftfs_update_nr_inode(new_dir, new_mc, new_dc, FTFS_UPDATE_NR_MAY_SPLIT);
	ftfs_update_nr_inode(old_dir, old_mc, old_dc, 0);

	return 0;

abort1:
	meta_key_free(new_meta_key);
abort:
	ftfs_bstore_txn_abort(txn);
	unlock_children_after_rename(&locked_children);
	ftfs_put_write_lock(FTFS_I(old_inode));
	if (new_inode)
		ftfs_put_write_lock(FTFS_I(new_inode));
	ftfs_put_read_lock(FTFS_I(old_dir));
	ftfs_put_read_lock(FTFS_I(new_dir));

	return ret;
}

static int ftfs_readdir(struct file *file, struct dir_context *ctx)
{
	int ret;
	struct inode *inode = file_inode(file);
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	struct ftfs_meta_key *meta_key;
	DB_TXN *txn;

	meta_key = ftfs_get_read_lock(ftfs_inode);

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	ret = ftfs_bstore_meta_readdir(sbi->meta_db, meta_key, txn, ctx);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
	} else {
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	ftfs_put_read_lock(ftfs_inode);
	return ret;
}

static int
ftfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	int ret;
	struct ftfs_sb_info *sbi = file_inode(file)->i_sb->s_fs_info;

	ret = generic_file_fsync(file, start, end, datasync);

	if (!ret)
		ret = ftfs_bstore_flush_log(sbi->db_env);

	return ret;
}

static void ftfs_destroy_inode(struct inode *inode);

static int
ftfs_mknod(struct inode *dir, struct dentry *dentry, umode_t mode, dev_t rdev)
{
	int ret;
	struct inode *inode = NULL;
	struct ftfs_metadata meta;
	struct ftfs_sb_info *sbi = dir->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dir);
	struct ftfs_meta_key *dir_meta_key, *meta_key;
	ino_t ino;
	DB_TXN *txn;

	if (rdev && !new_valid_dev(rdev)) {
		ret = -EINVAL;
		goto out;
	}

	dir_meta_key = ftfs_get_read_lock(ftfs_inode);
	meta_key = alloc_child_meta_key_from_meta_key(dir_meta_key,
	                                              dentry->d_name.name);
	if (!meta_key) {
		ret = -ENOMEM;
		goto out1;
	}

	ret = ftfs_next_ino(sbi, txn, &ino);
	if (ret) {
		BUG();
	}
	ftfs_setup_metadata(&meta, mode, 0, rdev, ino);
	inode = ftfs_setup_inode(dir->i_sb, meta_key, &meta);
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		inode = NULL;
		goto out1;
	}

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	ret = ftfs_bstore_meta_put(sbi->meta_db, meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}
	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	d_instantiate(dentry, inode);

	ftfs_put_read_lock(ftfs_inode);

	ftfs_update_nr_inode(dir, 1, 0, FTFS_UPDATE_NR_MAY_SPLIT);

	return 0;

abort:
	ftfs_bstore_txn_abort(txn);
	meta_key_free(meta_key);
	if (inode) {
		iput(inode);
		__destroy_inode(inode);
		ftfs_destroy_inode(inode);
	}
out1:
	ftfs_put_read_lock(ftfs_inode);
out:
	return ret;
}

static int
ftfs_create(struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
	return ftfs_mknod(dir, dentry, mode | S_IFREG, 0);
}

static int ftfs_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode)
{
	return ftfs_mknod(dir, dentry, mode | S_IFDIR, 0);
}

static int ftfs_rmdir(struct inode *dir, struct dentry *dentry)
{
#ifdef FTFS_PROFILE_RMDIR
	ktime_t rmdir_start = ktime_get();
#endif
	int r, ret;
	struct inode *inode = dentry->d_inode;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	struct ftfs_meta_key *meta_key;
	struct ftfs_meta_key *dir_meta_key = ftfs_get_read_lock(FTFS_I(dir));
	DB_TXN *txn;

	meta_key = ftfs_get_read_lock(ftfs_inode);

	if (meta_key == &root_dir_meta_key) {
		ret = -EINVAL;
		goto out;
	}

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	r = ftfs_directory_is_empty(sbi->meta_db, meta_key, txn, &ret);
	if (r) {
		DBOP_JUMP_ON_CONFLICT(r, retry);
		ftfs_bstore_txn_abort(txn);
		ret = r;
		goto out;
	}

	if (ret && meta_key_is_circle_root(meta_key)) {
		struct ftfs_meta_key *redirect_meta_key;
		redirect_meta_key = alloc_child_meta_key_from_meta_key(
		                            dir_meta_key, dentry->d_name.name);
		if (!redirect_meta_key) {
			r = -ENOMEM;
			goto failed;
		}

		r = ftfs_bstore_meta_del(sbi->meta_db, redirect_meta_key, txn);

		kfree(redirect_meta_key);
		if (r) {
			DBOP_JUMP_ON_CONFLICT(r, retry);
failed:
			ftfs_bstore_txn_abort(txn);
			ret = r;
			goto out;
		}
	}

	r = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

	if (r) {
		ret = r;
		goto out;
	}
	if (!ret)
		ret = -ENOTEMPTY;
	else {
		clear_nlink(inode);
		mark_inode_dirty(inode);
		ret = 0;
	}

out:
	ftfs_put_read_lock(FTFS_I(dir));
	ftfs_put_read_lock(ftfs_inode);

	if (!ret)
		ftfs_update_nr_inode(dir, -1, 0, 0);
#ifdef FTFS_PROFILE_RMDIR
	unlink_stat.rmdir = ktime_add(unlink_stat.rmdir,
				      ktime_sub(ktime_get(), rmdir_start));
	unlink_stat.count_rmdir += 1;
#endif
	return ret;
}

static int
ftfs_symlink(struct inode *dir, struct dentry *dentry, const char *symname)
{
	int ret;
	struct inode *inode = NULL;
	struct ftfs_sb_info *sbi = dir->i_sb->s_fs_info;
	struct ftfs_metadata meta;
	struct ftfs_inode *ftfs_inode = FTFS_I(dir);
	struct ftfs_meta_key *meta_key, *dir_meta_key;
	struct ftfs_data_key *data_key;
	size_t len = strlen(symname);
	ino_t ino;
	DB_TXN *txn;

	if (len > dir->i_sb->s_blocksize)
		return -ENAMETOOLONG;

	dir_meta_key = ftfs_get_read_lock(ftfs_inode);
	meta_key = alloc_child_meta_key_from_meta_key(dir_meta_key,
	                                              dentry->d_name.name);
	if (!meta_key) {
		ret = -ENOMEM;
		goto out;
	}

	data_key = alloc_data_key_from_meta_key(meta_key, 0);
	if (!data_key) {
		ret = -ENOMEM;
		goto out1;
	}

    ret = ftfs_next_ino(sbi, txn, &ino);
	if (ret) {
		BUG();
	}

    ftfs_setup_metadata(&meta, S_IFLNK | S_IRWXUGO, len, 0, ino);
	inode = ftfs_setup_inode(dir->i_sb, meta_key, &meta);
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		inode = NULL;
		goto out1;
	}

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

	ret = ftfs_bstore_meta_put(sbi->meta_db, meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}
	ret = ftfs_bstore_update(sbi->data_db, data_key, txn,
	                         symname, len, 0);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret,retry);
		goto abort;
	}

	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	d_instantiate(dentry, inode);
	ftfs_put_read_lock(ftfs_inode);
	data_key_free(data_key);

	ftfs_update_nr_inode(dir, 1, 1, FTFS_UPDATE_NR_MAY_SPLIT);

	return 0;

abort:
	ftfs_bstore_txn_abort(txn);
	if (inode) {
		inode_dec_link_count(inode);
		iput(inode);
	}
	data_key_free(data_key);
out1:
	meta_key_free(meta_key);
out:
	ftfs_put_read_lock(ftfs_inode);

	return ret;
}

static int ftfs_link(struct dentry *old_dentry,
                     struct inode *dir, struct dentry *dentry)
{
	int ret;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct inode *inode = old_dentry->d_inode;
	struct ftfs_meta_key *meta_key, *new_meta_key, *dir_meta_key;
	struct ftfs_metadata meta;
	DB_TXN *txn;

	meta_key = ftfs_get_read_lock(FTFS_I(inode));
	if (!meta_key_is_circle_root(meta_key)) {
		ftfs_put_read_lock(FTFS_I(inode));
		ret = split_circle(old_dentry);
		if (ret)
			goto out;
	} else
		ftfs_put_read_lock(FTFS_I(inode));

	dir_meta_key = ftfs_get_read_lock(FTFS_I(dir));
	meta_key = ftfs_get_read_lock(FTFS_I(inode));

	BUG_ON(!meta_key_is_circle_root(meta_key));

	new_meta_key = alloc_child_meta_key_from_meta_key(dir_meta_key,
	                                                  dentry->d_name.name);
	meta.type = FTFS_METADATA_TYPE_REDIRECT;
	meta.u.circle_id = meta_key_get_circle_id(meta_key);
	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_meta_put(sbi->meta_db, new_meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
	} else {
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	if (!ret) {
		inode->i_ctime = current_kernel_time();
		inc_nlink(inode);
		mark_inode_dirty(inode);
		ihold(inode);
		d_instantiate(dentry, inode);
	}

	meta_key_free(new_meta_key);
	ftfs_put_read_lock(FTFS_I(inode));
	ftfs_put_read_lock(FTFS_I(dir));

	if (!ret)
		ftfs_update_nr_inode(dir, 1, 0, FTFS_UPDATE_NR_MAY_SPLIT);

out:
	return ret;
}

struct ftfs_unlink_stat unlink_stat;

static int ftfs_unlink(struct inode *dir, struct dentry *dentry)
{
	int ret = 0;
	struct inode *inode = dentry->d_inode;
	struct ftfs_meta_key *dir_meta_key, *meta_key;
#ifdef FTFS_PROFILE_UNLINK
	ktime_t stage1_start;
	ktime_t stage2_start;
	stage1_start = ktime_get();
#endif
	dir_meta_key = ftfs_get_read_lock(FTFS_I(dir));
	meta_key = ftfs_get_read_lock(FTFS_I(inode));

	if (meta_key_is_circle_root(meta_key)) {
		struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
		struct ftfs_meta_key *redirect_meta_key;
		DB_TXN *txn;

		redirect_meta_key = alloc_child_meta_key_from_meta_key(
		                            dir_meta_key, dentry->d_name.name);
		if (!redirect_meta_key) {
			ret = -ENOMEM;
		} else {
			TXN_GOTO_LABEL(retry);
			ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
			ret = ftfs_bstore_meta_del(sbi->meta_db, redirect_meta_key, txn);
			if (ret) {
				DBOP_JUMP_ON_CONFLICT(ret, retry);
				ftfs_bstore_txn_abort(txn);
			} else {
				ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
				COMMIT_JUMP_ON_CONFLICT(ret, retry);
			}
			kfree(redirect_meta_key);
		}
		ftfs_put_read_lock(FTFS_I(inode));
		ftfs_put_read_lock(FTFS_I(dir));
		if (!ret)
			ftfs_update_nr_inode(dir, -1, 0, 0);
#ifdef FTFS_PROFILE_UNLINK
		unlink_stat.stage1_if = ktime_add(unlink_stat.stage1_if,
				        ktime_sub(ktime_get(), stage1_start));
		unlink_stat.count_if += 1;
#endif
	} else {
		ftfs_put_read_lock(FTFS_I(inode));
		ftfs_put_read_lock(FTFS_I(dir));
		ftfs_update_nr_inode(dir, -1, -FTFS_I(inode)->nr_data, 0);

#ifdef FTFS_PROFILE_UNLINK
		unlink_stat.stage1_else = ktime_add(unlink_stat.stage1_else,
					       ktime_sub(ktime_get(), stage1_start));
		unlink_stat.count_else += 1;
#endif
	}

	if (ret)
		return ret;
#ifdef FTFS_PROFILE_UNLINK
	stage2_start = ktime_get();
#endif
	drop_nlink(inode);
	mark_inode_dirty(inode);

	/* A dirty hack */
	while (dentry->d_flags & DCACHE_FTFS_FLAG)
		yield();

	if (dentry->d_lockref.count != 1) {
		printk("%s dentry flag %x\n", __func__, dentry->d_flags);
		printk("%s dentry is %s\n", __func__, dentry->d_name.name);
		printk("%s dentry ref count %d\n", __func__, dentry->d_lockref.count);
	}

#ifdef FTFS_PROFILE_UNLINK
	unlink_stat.stage2 = ktime_add(unlink_stat.stage2,
			          ktime_sub(ktime_get(), stage2_start));
#endif
	return ret;
}

static void ftfs_d_prune(struct dentry *dentry)
{
#if 0
	struct inode *inode;

	if (dentry == NULL)
		return;
	inode = dentry->d_inode;

	if (inode == NULL)
		return;

	/* Writeback the inode itself */
	if (inode->i_state & I_DIRTY)
		inode->i_sb->s_op->write_inode(inode, NULL);
	spin_lock(&inode->i_lock);

	/* Writeback inode's pages */
	if (inode->i_state & (I_NEW | I_FREEING | I_WILL_FREE | I_SYNC)) {
		spin_unlock(&inode->i_lock);
		return;
	}

	inode->i_state |= I_SYNC;
	spin_unlock(&inode->i_lock);

	{
		/* Real writeback after some checks */
		struct address_space *mapping = inode->i_mapping;
		int ret;
		struct writeback_control wbc = {
			.sync_mode              = WB_SYNC_NONE,
			.range_cyclic           = 1,
			.range_start            = 0,
			.range_end              = LLONG_MAX,
		};

		WARN_ON(!(inode->i_state & I_SYNC));
		ret = mapping->a_ops->writepages(mapping, &wbc);
		if (ret)
			BUG();
	}
#endif
}

static const struct dentry_operations ftfs_dentry_ops = {
	.d_prune = ftfs_d_prune,
};

void ftfs_init_dentry(struct dentry *dentry)
{
	spin_lock(&dentry->d_lock);
	d_set_d_op(dentry, &ftfs_dentry_ops);
	spin_unlock(&dentry->d_lock);
}

static struct dentry *
ftfs_lookup(struct inode *dir, struct dentry *dentry, unsigned int flags)
{
	int r;
	struct dentry *ret;
	struct inode *inode;
	struct ftfs_sb_info *sbi = dir->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dir);
	struct ftfs_meta_key *dir_meta_key, *meta_key;
	DB_TXN *txn;

	ftfs_init_dentry(dentry);

	dir_meta_key = ftfs_get_read_lock(ftfs_inode);
	meta_key = alloc_child_meta_key_from_meta_key(dir_meta_key,
	                                              dentry->d_name.name);
	if (!meta_key) {
		inode = ERR_PTR(-ENOMEM);
		goto out;
	}

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);

	inode = ftfs_iget(dir->i_sb, meta_key, txn);
	if (IS_ERR(inode)) {
		int err = PTR_ERR(inode);
		if (err == -ENOENT) {
			inode = NULL;
			r = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
			COMMIT_JUMP_ON_CONFLICT(r, retry);
		} else {
			DBOP_JUMP_ON_CONFLICT(err, retry);
			ftfs_bstore_txn_abort(txn);
		}
	} else {
		r = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(r, retry);
	}

    if (IS_ERR(inode))
        WARN_ON(inode != NULL && (flags & LOOKUP_CREATE));
    else {
        WARN_ON(inode != NULL && (flags & LOOKUP_CREATE));
        if (inode != NULL && (flags & LOOKUP_CREATE)) {
            printk("dentry is %s\n", dentry->d_name.name);
            printk("inode i_nlink is %d\n", inode->i_nlink);
            printk("inode is %p\n", inode);
        }
    }

out:
	/* aging bug */
	ret = d_splice_alias(inode, dentry);
	ftfs_put_read_lock(ftfs_inode);
	return ret;
}

static int ftfs_setattr(struct dentry *dentry, struct iattr *iattr)
{
	int ret, circle_root;
	struct inode *inode = dentry->d_inode;
	loff_t size;

	ret = inode_change_ok(inode, iattr);
	if (ret)
		return ret;

	size = i_size_read(inode);
	if ((iattr->ia_valid & ATTR_SIZE) && iattr->ia_size < size) {
		uint64_t block_num, old_block_num;
		struct ftfs_inode *ftfs_inode = FTFS_I(inode);
		struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
		struct ftfs_meta_key *meta_key;
		DB_TXN *txn;

		block_num = ftfs_get_block_num_by_size(iattr->ia_size);
		old_block_num = ftfs_get_block_num_by_size(iattr->ia_size);
		if (block_num == old_block_num)
			goto skip_txn;

		meta_key = ftfs_get_read_lock(ftfs_inode);
		TXN_GOTO_LABEL(retry);
		ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn,
		                      TXN_MAY_WRITE);
		ret = ftfs_bstore_truncate(sbi->data_db, meta_key, txn,
		                           block_num + 1,
		                           old_block_num);
		if (ret) {
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			ftfs_bstore_txn_abort(txn);
			ftfs_put_read_lock(ftfs_inode);
			goto failed;
		}
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
		circle_root = meta_key_is_circle_root(meta_key);
		ftfs_put_read_lock(ftfs_inode);
		if (!ret && !circle_root)
			ftfs_update_nr(dentry->d_parent, 0, block_num - old_block_num, 0);

skip_txn:
		i_size_write(inode, iattr->ia_size);
	}

    /**
     *  ftruncate can call this to increase the file size
     *  http://pubs.opengroup.org/onlinepubs/009695399/functions/ftruncate.html
     */

	if ((iattr->ia_valid & ATTR_SIZE) && iattr->ia_size > size) {
        //printk("%s size:%lld\n", __func__, size);
		i_size_write(inode, iattr->ia_size);
    }

	setattr_copy(inode, iattr);
	mark_inode_dirty(inode);

failed:
	return ret;
}

static void *ftfs_follow_link(struct dentry *dentry, struct nameidata *nd)
{
	int r;
	void *ret;
	void *buf;
	struct ftfs_sb_info *sbi = dentry->d_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(dentry->d_inode);
	struct ftfs_meta_key *meta_key;
	struct ftfs_data_key *data_key;
	DB_TXN *txn;

	buf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	if (!buf) {
		ret = ERR_PTR(-ENOMEM);
		goto err1;
	}
	meta_key = ftfs_get_read_lock(ftfs_inode);
	data_key = alloc_data_key_from_meta_key(meta_key, 0);
	if (!data_key) {
		ret = ERR_PTR(-ENOMEM);
		goto err2;
	}

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);
	r = ftfs_bstore_get(sbi->data_db, data_key, txn, buf);
	if (r) {
		DBOP_JUMP_ON_CONFLICT(r, retry);
		ftfs_bstore_txn_abort(txn);
		ret = ERR_PTR(r);
		goto err3;
	}
	r = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

	nd_set_link(nd, buf);

	ret = buf;

err3:
	data_key_free(data_key);
err2:
	ftfs_put_read_lock(ftfs_inode);
	if (ret != buf)
		kfree(buf);
err1:
	return ret;
}

static void ftfs_put_link(struct dentry *dentry, struct nameidata *nd,
                          void *cookie)
{
	if (IS_ERR(cookie))
		return;
	kfree(cookie);
}

static struct inode *ftfs_alloc_inode(struct super_block *sb)
{
	struct ftfs_inode *ftfs_inode;

	ftfs_inode = kmem_cache_alloc(ftfs_inode_cachep, GFP_KERNEL);
	if (ftfs_inode)
		INIT_LIST_HEAD(&ftfs_inode->rename_locked);

	return ftfs_inode ? &ftfs_inode->vfs_inode : NULL;
}

static void ftfs_destroy_inode(struct inode *inode)
{
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
#ifdef FTFS_PROFILE_DESTROY
	ktime_t destroy_start = ktime_get();
#endif
	ftfs_get_write_lock(ftfs_inode);
	if (ftfs_inode->key != &root_dir_meta_key)
		meta_key_free(ftfs_inode->key);
	ftfs_inode->key = NULL;

	kmem_cache_free(ftfs_inode_cachep, ftfs_inode);

#ifdef FTFS_PROFILE_DESTROY
	unlink_stat.destroy_inode = ktime_add(unlink_stat.destroy_inode,
				        ktime_sub(ktime_get(), destroy_start));
	unlink_stat.count_destroy += 1;
#endif
}

static int
ftfs_write_inode(struct inode *inode, struct writeback_control *wbc)
{
	int ret;
	DB_TXN *txn;
	struct ftfs_meta_key *meta_key;
	struct ftfs_metadata meta;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;

	meta_key = ftfs_get_read_lock(FTFS_I(inode));

	if (meta_key_is_circle_root(meta_key) &&
	    meta_key->circle_id != 0 &&
	    FTFS_I(inode)->nr_meta < sbi->max_circle_size &&
	    FTFS_I(inode)->nr_data < sbi->max_circle_size &&
	    inode->i_nlink == 1) {
		ftfs_put_read_lock(FTFS_I(inode));
		ret = maybe_merge_circle(inode);
		meta_key = ftfs_get_read_lock(FTFS_I(inode));
	}

	ftfs_copy_metadata_from_inode(&meta, inode);

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_meta_put(sbi->meta_db, meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
	} else {
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	ftfs_put_read_lock(FTFS_I(inode));

	return ret;
}

static void ftfs_evict_inode(struct inode *inode)
{
	int ret;
	struct ftfs_sb_info *sbi = inode->i_sb->s_fs_info;
	struct ftfs_meta_key *meta_key;
	struct ftfs_inode *ftfs_inode = FTFS_I(inode);
	DB_TXN *txn;
#ifdef FTFS_PROFILE_EVICT
	ktime_t evict_start = ktime_get();
#endif
	if (inode->i_nlink)
		goto no_delete;

	meta_key = ftfs_get_read_lock(ftfs_inode);

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_do_unlink(meta_key, txn, inode, sbi);
	if (ret) {
		printk("Inode is %p\n", inode);
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		ftfs_bstore_txn_abort(txn);
	} else {
		ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
		/* if (inode->i_size > HOT_FLUSH_THRESHOLD)
			ftfs_bstore_data_hot_flush(sbi->data_db,
				meta_key, 0,
				block_get_num_by_position(
					inode->i_size)); */
	}

	ftfs_put_read_lock(ftfs_inode);

no_delete:
	truncate_inode_pages(&inode->i_data, 0);

	invalidate_inode_buffers(inode);
	clear_inode(inode);
#ifdef FTFS_PROFILE_EVICT
	unlink_stat.evict_inode = ktime_add(unlink_stat.evict_inode,
				        ktime_sub(ktime_get(), evict_start));
	unlink_stat.count_evict += 1;
#endif
}

// called when VFS wishes to free sb (unmount), sync southbound here
static void ftfs_put_super(struct super_block *sb)
{
	struct ftfs_sb_info *sbi = sb->s_fs_info;

	sync_filesystem(sb);

	sb->s_fs_info = NULL;

	ftfs_bstore_checkpoint(sbi->db_env);
	ftfs_bstore_env_close(sbi);

	free_percpu(sbi->s_ftfs_info);
	kfree(sbi);
}

static int ftfs_sync_fs(struct super_block *sb, int wait)
{
	struct ftfs_sb_info *sbi = sb->s_fs_info;

	return ftfs_bstore_flush_log(sbi->db_env);
}

static int ftfs_dir_release(struct inode *inode, struct file *filp)
{
	if (filp->f_pos != 0 && filp->f_pos != 1)
		kfree((char *)filp->f_pos);

	return 0;
}

static const struct address_space_operations ftfs_aops = {
	.readpage		= ftfs_readpage,
	.readpages		= ftfs_readpages,
	.writepage		= ftfs_writepage,
	.writepages		= ftfs_writepages,
	.write_begin		= ftfs_write_begin,
	.write_end		= ftfs_write_end,
	.launder_page		= ftfs_launder_page,
};

static const struct file_operations ftfs_file_file_operations = {
	.llseek			= generic_file_llseek,
	.fsync			= ftfs_fsync,
	.read			= do_sync_read,
	.write			= do_sync_write,
	.aio_read		= generic_file_aio_read,
	.aio_write		= generic_file_aio_write,
	.mmap			= generic_file_mmap,
};

static const struct file_operations ftfs_dir_file_operations = {
	.read			= generic_read_dir,
	.iterate		= ftfs_readdir,
	.fsync			= ftfs_fsync,
	.release		= ftfs_dir_release,
};

static const struct inode_operations ftfs_file_inode_operations = {
	.setattr		= ftfs_setattr
};

static const struct inode_operations ftfs_dir_inode_operations = {
	.create			= ftfs_create,
	.lookup			= ftfs_lookup,
	.link			= ftfs_link,
	.unlink			= ftfs_unlink,
	.symlink		= ftfs_symlink,
	.mkdir			= ftfs_mkdir,
	.rmdir			= ftfs_rmdir,
	.mknod			= ftfs_mknod,
	.rename			= ftfs_rename,
	.setattr		= ftfs_setattr,
};

static const struct inode_operations ftfs_symlink_inode_operations = {
	.setattr		= ftfs_setattr,
	.readlink		= generic_readlink,
	.follow_link		= ftfs_follow_link,
	.put_link		= ftfs_put_link,
};

static const struct inode_operations ftfs_special_inode_operations = {
	.setattr		= ftfs_setattr,
};

static const struct super_operations ftfs_super_ops = {
	.alloc_inode		= ftfs_alloc_inode,
	.destroy_inode		= ftfs_destroy_inode,
	.write_inode		= ftfs_write_inode,
	.evict_inode		= ftfs_evict_inode,
	.put_super		= ftfs_put_super,
	.sync_fs		= ftfs_sync_fs,
	.statfs			= ftfs_super_statfs,
};

/*
 * fill inode with meta_key, metadata from database and inode number
 */
static struct inode *
ftfs_setup_inode(struct super_block *sb, struct ftfs_meta_key *meta_key,
                 struct ftfs_metadata *meta)
{
	struct inode *i;
	struct ftfs_inode *ftfs_inode;

	if ((i = iget_locked(sb, meta->u.st.st_ino)) == NULL)
		return ERR_PTR(-ENOMEM);

	ftfs_inode = FTFS_I(i);
	if (!(i->i_state & I_NEW)) {
		/* In-memory inode not binding to a dentry causes the git
		   bugs being seen */
		struct ftfs_meta_key *mkey = ftfs_get_write_lock(FTFS_I(i));
		FTFS_I(i)->key = meta_key;
		ftfs_put_write_lock(FTFS_I(i));
		kfree(mkey);
		return i;
	}

	BUG_ON(ftfs_inode->key);
	ftfs_inode->key = meta_key;
	init_rwsem(&ftfs_inode->key_lock);

	BUG_ON(meta->type != FTFS_METADATA_TYPE_NORMAL);
	FTFS_I(i)->nr_meta = meta->nr_meta;
	FTFS_I(i)->nr_data = meta->nr_data;
	i->i_rdev = meta->u.st.st_dev;
	i->i_mode = meta->u.st.st_mode;
	set_nlink(i, meta->u.st.st_nlink);
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	i->i_uid.val = meta->u.st.st_uid;
	i->i_gid.val = meta->u.st.st_gid;
#else
	i->i_uid = meta->u.st.st_uid;
	i->i_gid = meta->u.st.st_gid;
#endif
	i->i_size = meta->u.st.st_size;
	i->i_blocks = meta->u.st.st_blocks;
	TIME_T_TO_TIMESPEC(i->i_atime, meta->u.st.st_atime);
	TIME_T_TO_TIMESPEC(i->i_mtime, meta->u.st.st_mtime);
	TIME_T_TO_TIMESPEC(i->i_ctime, meta->u.st.st_ctime);

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
	Opt_circle_size
};

static const match_table_t tokens = {
	{Opt_circle_size, "max=%u"}
};

static void parse_options(char *options, struct ftfs_sb_info *sbi)
{
	char *p;
	substring_t args[MAX_OPT_ARGS];
	int option;

	sbi->max_circle_size = FTFS_MAX_CIRCLE_SIZE;

	if (!options)
		return;

	while ((p = strsep(&options, ",")) != NULL) {
		int token;

		if (!*p)
			continue;

		token = match_token(p, tokens, args);
		switch (token) {
		case Opt_circle_size:
			if (match_int(&args[0], &option))
				return;
			sbi->max_circle_size = option;
			break;
		default:
			goto out;
		}
	}
out:
	sbi->max_file_size = (sbi->max_circle_size) ?
	                     (((uint64_t)sbi->max_circle_size - 1) << FTFS_BSTORE_BLOCKSIZE_BITS) + 1 :
	                     0;
}

/*
 * fill in the superblock
 */
static int ftfs_fill_super(struct super_block *sb, void *data, int silent)
{
	int ret;
	int cpu;
	ino_t ino;
	struct inode *root;
	struct ftfs_metadata meta;
	struct ftfs_sb_info *sbi;
	DB_TXN *txn;

	// FTFS specific info
	sbi = kzalloc(sizeof(struct ftfs_sb_info), GFP_KERNEL);
	if (!sbi) {
		ret = -ENOMEM;
		goto failed;
	}

	ret = ftfs_bstore_env_open(sbi);
	if (ret)
		goto failed_free_sbi;

	parse_options(data, sbi);
	sbi->s_ftfs_info = alloc_percpu(struct ftfs_info);
	if (!sbi->s_ftfs_info)
		goto failed_free_sbi;
	sb->s_fs_info = sbi;

	sb_set_blocksize(sb, FTFS_BSTORE_BLOCKSIZE);
	sb->s_op = &ftfs_super_ops;
	sb->s_maxbytes = MAX_LFS_FILESIZE;

	TXN_GOTO_LABEL(retry);
	ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_meta_get(sbi->meta_db, &root_dir_meta_key,
	                           txn, &meta);
	if (ret) {
#ifdef TXN_ON
		IF_DBOP_CONFLICT(ret)
			goto retry;
#endif
		if (ret == -ENOENT) {
			ftfs_setup_metadata(&meta, 0755 | S_IFDIR, 0, 0,
			                    FTFS_ROOT_INO);
			ret = ftfs_bstore_meta_put(sbi->meta_db,
			                           &root_dir_meta_key,
			                           txn, &meta);
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			BUG_ON(ret);
		}
	}
	ret = ftfs_bstore_get_ino(sbi->meta_db, txn, &ino);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		BUG_ON(ret);
	}
	sbi->s_nr_cpus = 0;
	for_each_possible_cpu(cpu) {
		(per_cpu_ptr(sbi->s_ftfs_info, cpu))->next_ino = ino + cpu;
		(per_cpu_ptr(sbi->s_ftfs_info, cpu))->max_ino = ino;
		sbi->s_nr_cpus++;
	}
	ret = ftfs_bstore_txn_commit(txn, DB_TXN_SYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	root = ftfs_setup_inode(sb, &root_dir_meta_key, &meta);
	if (IS_ERR(root)) {
		ret = PTR_ERR(root);
		goto failed_free_sbi;
	}

	sb->s_root = d_make_root(root);
	if (!sb->s_root) {
		ret = -EINVAL;
		goto failed_free_percpu;
	}

	return 0;

failed_free_percpu:
	free_percpu(sbi->s_ftfs_info);
failed_free_sbi:
	kfree(sbi);
failed:
	return ret;
}

/*
 * mount ftfs, call kernel util mount_bdev
 * actual work of ftfs is done in ftfs_fill_super
 */
static struct dentry *ftfs_mount(struct file_system_type *fs_type, int flags,
                                 const char *dev_name, void *data)
{
	return mount_bdev(fs_type, flags, dev_name, data, ftfs_fill_super);
}

static void find_dirty_inodes(struct super_block *sb)
{
	struct inode *inode, *next;

	list_for_each_entry_safe(inode, next, &sb->s_inodes, i_sb_list) {
		if (inode->i_state & I_DIRTY)
			printk("Inode state %lu\n", inode->i_state);
	}
}

static void find_remaining_inodes(struct super_block *sb)
{
	struct inode *inode, *next;

	list_for_each_entry_safe(inode, next, &sb->s_inodes, i_sb_list) {
		if (atomic_read(&inode->i_count)) {
			printk("Inode i_count %d\n", atomic_read(&inode->i_count));
			continue;
		}

		spin_lock(&inode->i_lock);
		if (inode->i_state & I_DIRTY)
			printk("Inode state %lu\n", inode->i_state);
		spin_unlock(&inode->i_lock);
	}
}

#if 0
static void ftfs_writeback_inodes_sb(struct super_block *sb)
{
	struct dentry *object;
	struct dentry *this_parent;
	struct inode *inode;
	struct list_head *next;

	object = sb->s_root;
	this_parent = object;;
start:
	if (this_parent->d_sb != object->d_sb)
		goto end;
	next = this_parent->d_subdirs.next;
resume:
	while (next != &this_parent->d_subdirs) {
		this_parent = list_entry(next, struct dentry, d_u.d_child);
		goto start;
	}
end:
	inode = this_parent->d_inode;
	sync_inode_metadata(inode, 1);
	if (this_parent != object) {
		next = this_parent->d_u.d_child.next;
		this_parent = this_parent->d_parent;
		goto resume;
	}
}
#endif

static void ftfs_kill_sb(struct super_block *sb)
{
	sync_filesystem(sb);
	printk("dirty inode:\n");
	find_dirty_inodes(sb);
	kill_block_super(sb);
	printk("remaining inode:\n");
	find_remaining_inodes(sb);
}

static struct file_system_type ftfs_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "ftfs",
	.mount		= ftfs_mount,
	.kill_sb	= ftfs_kill_sb,
	.fs_flags	= FS_REQUIRES_DEV,
};

static int ftfs_init_writepages_vec(void)
{
	int i;
	struct ftfs_writepages_vec *writepages_vec;
	mutex_init(&ftfs_writepages_vec_list_mutex);
	for (i = 0; i < FTFS_WRITEPAGES_VEC_LIST_INIT_SIZE; i++) {
		writepages_vec = ftfs_alloc_writepages_vec();
		if (writepages_vec == NULL) {
			printk(KERN_ERR "FTFS_ERROR: Failed to alloc writepages_vec.\n");
			return -ENOMEM;
		}
		list_add(&writepages_vec->list, &ftfs_writepages_vec_list);
	}
	return 0;
}

static void ftfs_free_writepages_vec(void)
{
	struct ftfs_writepages_vec *writepages_vec, *tmp;
	list_for_each_entry_safe(writepages_vec, tmp, &ftfs_writepages_vec_list, list) {
		list_del(&(writepages_vec->list));
		kfree(writepages_vec);
	}
	mutex_destroy(&ftfs_writepages_vec_list_mutex);
}

int init_ftfs_fs(void)
{
	int ret;

	ret = ftfs_init_writepages_vec();
	if (ret)
		goto out;
	ftfs_inode_cachep =
		kmem_cache_create("ftfs_i",
		                  sizeof(struct ftfs_inode), 0,
		                  SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD,
		                  ftfs_i_init_once);
	if (!ftfs_inode_cachep) {
		printk(KERN_ERR "FTFS ERROR: Failed to initialize inode cache.\n");
		ret = -ENOMEM;
		goto out;
	}

	ret = register_filesystem(&ftfs_fs_type);
	if (ret) {
		printk(KERN_ERR "FTFS ERROR: Failed to register filesystem\n");
		goto error_register;
	}

	return 0;

error_register:
	kmem_cache_destroy(ftfs_inode_cachep);
out:
	ftfs_free_writepages_vec();
	return ret;
}

void exit_ftfs_fs(void)
{
	unregister_filesystem(&ftfs_fs_type);

	kmem_cache_destroy(ftfs_inode_cachep);

	ftfs_free_writepages_vec();
}
