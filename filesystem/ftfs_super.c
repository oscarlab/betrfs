#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/dcache.h>
#include <linux/namei.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/errno.h>
#include <linux/fsnotify.h>
#ifdef NOT_PAGE_CACHE
#include <asm/uaccess.h>
#endif
#include "tokudb.h"
#include "ftfs.h"
#include "ftfs_southbound.h"
#include "ftfs_fs.h"

static struct kmem_cache *ftfs_inode_cachep;

/*
 * setup meta_key_dbt for ftfs_inode
 * inode->i_dbt should be NULL
 * after this, ref_count of dbt is 1
 */
static inline int ftfs_i_setup_dbt(struct ftfs_inode_info *inode)
{
	char *path_buf, *path;
	struct dentry *de;
	int ret;
	struct ftfs_dbt *dbt;

	BUG_ON(inode->i_dbt);
	if (!(path_buf = kmalloc(PATH_MAX, GFP_KERNEL))) {
		ret = -ENOMEM;
		goto out;
	}
	de = d_find_alias(&inode->vfs_inode);
	if (!de) {
		ret = -ENOENT;
		goto out1;
	}
	path = dentry_path_raw(de, path_buf, PATH_MAX);
	dput(de);

	if (!(dbt = kmalloc(sizeof(struct ftfs_dbt), GFP_KERNEL))) {
		ret = -ENOMEM;
		goto out1;
	}

	ret = alloc_and_gen_meta_key_dbt(&dbt->dbt, path);
	if (ret)
		goto out2;

	dbt->lockref.count = 1;
	spin_lock_init(&dbt->lockref.lock);
	inode->i_dbt = dbt;
out1:
	kfree(path_buf);
out:
	return ret;
out2:
	kfree(dbt);
	goto out1;
}

/*
 * get meta_key_dbt from inode
 * please call ftfs_i_puy_dbt after you finished playing with dbt
 */
static inline struct ftfs_dbt *ftfs_i_get_dbt(struct inode *_inode)
{
	struct ftfs_inode_info *inode = FTFS_I(_inode);

	if (!inode->i_dbt) {
		int ret = ftfs_i_setup_dbt(inode);
		if (ret)
			return ERR_PTR(ret);
	}

	lockref_get(&inode->i_dbt->lockref);
	return inode->i_dbt;
}

/*
 * put meta_key_dbt, if ref_count is 0, free
 */
static inline void ftfs_i_put_dbt(struct ftfs_dbt *dbt)
{
	if (!lockref_put_or_lock(&dbt->lockref)) {
		BUG_ON(!dbt->dbt.data);
		kfree(dbt->dbt.data);
		/* our kernel doesn't have lockref_mark_dead */
		dbt->lockref.count = -128;
		spin_unlock(&dbt->lockref.lock);
		kfree(dbt);
	}
}

static inline void ftfs_i_clear_dbt(struct ftfs_inode_info *inode)
{
	if (inode->i_dbt) {
		ftfs_i_put_dbt(inode->i_dbt);
		inode->i_dbt = NULL;
	}
}

/*
 * allocate a new inode
 */
static struct inode *ftfs_alloc_inode(struct super_block *sb)
{
	struct ftfs_inode_info *inode;

	inode = kmem_cache_alloc(ftfs_inode_cachep, GFP_KERNEL);
	if (inode)
		INIT_LIST_HEAD(&inode->rename_locked);

	return inode ? &inode->vfs_inode : NULL;
}

/*
 * return a spent inode to the slab cache
 */
static void ftfs_destroy_inode(struct inode *_inode)
{
	struct ftfs_inode_info *inode = FTFS_I(_inode);

	ftfs_i_clear_dbt(inode);

	kmem_cache_free(ftfs_inode_cachep, inode);
}

/*
 * write inode to disk
 */
static int ftfs_write_inode(struct inode *inode, struct writeback_control *wbc)
{
	struct ftfs_dbt *meta_key;
	int ret;

	meta_key = ftfs_i_get_dbt(inode);
	if (IS_ERR(meta_key))
		return PTR_ERR(meta_key);

	ret = ftfs_metadata_wb(&meta_key->dbt, inode);

	ftfs_i_put_dbt(meta_key);
	return ret;
}

#ifdef NOT_PAGECACHE
static ssize_t ftfs_file_read(struct file *filp, char __user *buf, size_t len,
			loff_t *ppos)
{
	int r;
	ssize_t retval;
	struct inode *inode = file_inode(filp);
	struct scan_buf sbuf;
	uint64_t block_num, end_block_num;
	loff_t size;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);
	char *kbuf;

	if (IS_ERR(meta_key))
		return PTR_ERR(meta_key);
	kbuf = kmalloc(len, GFP_KERNEL);
	if (!kbuf) {
		retval = -ENOMEM;
		goto out1;
	}
	if (len < 0) {
		retval = -EINVAL;
		goto out;
	}
	if (!access_ok(VERIFY_WRITE, buf, len)) {
		retval = -EFAULT;
		goto out;
	}
	size = i_size_read(inode);
	if (size <= *ppos) {
		retval = 0;
		goto out;
	}

	sbuf.buf = kbuf;
	sbuf.offset = *ppos;
	sbuf.len = len;
	sbuf.bytes_read = 0;

	block_num = block_get_num_by_position(sbuf.offset);
	end_block_num = block_get_num_by_position(sbuf.offset + sbuf.len);

	TXN_GOTO_LABEL(retry);

	mutex_lock(&inode->i_mutex);
	bstore_txn_begin(NULL, &txn, TXN_READONLY);
	retval = ftfs_bstore_scan(&meta_key->dbt, txn, block_num, end_block_num,
				bstore_fill_block, &sbuf);
	mutex_unlock(&inode->i_mutex);
	DBOP_JUMP_ON_CONFLICT(retval, retry);

	r = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

	file_accessed(filp);

	size = i_size_read(inode);
	if (sbuf.bytes_read + *ppos > size)
		sbuf.bytes_read = size - *ppos;
	retval = sbuf.bytes_read;
	*ppos += retval;
	if (copy_to_user(buf, kbuf, retval))
		retval = -EFAULT;

out:
	kfree(kbuf);
out1:
	ftfs_i_put_dbt(meta_key);
	return retval;
}

static ssize_t __ftfs_file_write(struct file *filp, const char __user *buf,
				size_t len, loff_t *ppos)
{
	int r;
	ssize_t retval, write_size;
	struct inode *inode = file_inode(filp);
	uint64_t block_num;
	size_t block_offset, count;
	loff_t offset, size;
	void *kbuf;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);

	if (IS_ERR(meta_key))
		return PTR_ERR(meta_key);

	if (!access_ok(VERIFY_READ, buf, len)) {
		retval = -EFAULT;
		goto out1;
	}

	count = len;
	retval = generic_write_checks(filp, ppos, &count,
				S_ISBLK(inode->i_mode));
	if (retval || count == 0)
		goto out1;

	offset = *ppos;
	kbuf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	if (kbuf == NULL) {
		retval = -ENOMEM;
		goto out1;
	}

	retval = file_update_time(filp);
	if (retval != 0)
		goto out;

	TXN_GOTO_LABEL(retry);

	retval = 0;
	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	while (len > 0) {
		block_num = block_get_num_by_position(offset);
		block_offset = block_get_offset_by_position(offset);
		write_size = min(len, (FTFS_BSTORE_BLOCKSIZE - block_offset));
		BUG_ON(write_size <= 0);

		r = copy_from_user(kbuf, buf, write_size);
		if (r) {
			retval = r;
			bstore_txn_abort(txn);
			goto out;
		}

		if (write_size == FTFS_BSTORE_BLOCKSIZE) {
			r = ftfs_bstore_put(&meta_key->dbt,
					txn, block_num, buf);
			if (r) {
				DBOP_JUMP_ON_CONFLICT(r, retry);
				bstore_txn_abort(txn);
				retval = r;
				goto out;
			}
		} else {
			r = ftfs_bstore_update(&meta_key->dbt, txn, block_num,
					buf, write_size, block_offset);
			if (r) {
				DBOP_JUMP_ON_CONFLICT(r, retry);
				bstore_txn_abort(txn);
				retval = r;
				goto out;
			}

		}

		buf += write_size;
		retval += write_size;
		offset += write_size;
		len -= write_size;
	}

	r = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

	*ppos += retval;
	size = i_size_read(inode);
	if (*ppos > size) {
		i_size_write(inode, *ppos);
		mark_inode_dirty(inode);
	}

out:
	kfree(kbuf);
out1:
	ftfs_i_put_dbt(meta_key);
	return retval;
}

static ssize_t ftfs_file_write(struct file *filp, const char __user *buf,
			size_t len, loff_t *ppos)
{
	struct inode *inode = file_inode(filp);
	ssize_t ret;

	mutex_lock(&inode->i_mutex);
	ret = __ftfs_file_write(filp, buf, len, ppos);
	mutex_unlock(&inode->i_mutex);

	if (ret > 0) { // || ret == -EIOCBQUEUED) {
		ssize_t err;
		err = generic_write_sync(filp, *ppos, ret);
		if (err < 0 && ret > 0)
			ret = err;
	}

	return ret;
}
#endif /* NOT_PAGECACHE */

static struct inode *ftfs_setup_inode(struct super_block *sb,
				struct ftfs_metadata *meta, ino_t ino);

static int ftfs_readdir(struct file *file, struct dir_context *ctx)
{
	int ret;
	struct inode *inode = file_inode(file);
	struct ftfs_metadata meta;
	size_t len;
	struct readdir_scan_cb_info info;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);
	char *path;

	if (IS_ERR(meta_key))
		return PTR_ERR(meta_key);
	path = get_path_from_meta_key_dbt(&meta_key->dbt);

	TXN_GOTO_LABEL(retry);

	ret = bstore_txn_begin(NULL, &txn, TXN_READONLY);
	BUG_ON(ret);

	ret = ftfs_bstore_meta_get(get_path_from_meta_key_dbt(&meta_key->dbt),
				txn, &meta, FTFS_METADATA_SIZE);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		bstore_txn_abort(txn);
		goto out;
	}

	FTFS_DEBUG_ON(!S_ISDIR(meta.st.st_mode));

	len = strlen(path) + 2;
	info.dirname = kmalloc(len, GFP_KERNEL);
	if (info.dirname == NULL) {
		ret = -ENOMEM;
		bstore_txn_abort(txn);
		goto out1;
	}
	strcpy(info.dirname, path);
	if (strcmp(path, "/") != 0) {
		info.dirname[len - 2] = '/';
		info.dirname[len - 1] = '\0';
		info.root_first = 0;
	} else {
		info.root_first = 1;
	}

	if (!ctx->pos && !dir_emit_dots(file, ctx))
		goto out;

	info.skip = ctx->pos - 2;
	info.ctx = ctx;
	info.sb = inode->i_sb;
	ret = ftfs_bstore_meta_scan(info.dirname, txn, readdir_scan_cb, &info);
	if (ret == FTFS_BSTORE_NOTFOUND)
		ret = 0;
#ifdef TXN_ON
	IF_DBOP_CONFLICT(ret) {
		kfree(info.dirname);
		goto retry;
	}
#endif

out:
	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
	kfree(info.dirname);
out1:
	ftfs_i_put_dbt(meta_key);
	return ret;
}

static inline void
ftfs_setup_metadata(struct ftfs_metadata *meta, umode_t mode, loff_t size,
		dev_t rdev)
{
	time_t now;

	TIMESPEC_TO_TIME_T(now, CURRENT_TIME_SEC);
	memset(meta, 0, FTFS_METADATA_SIZE);
	meta->st.st_mode = mode;
	meta->st.st_size = size;
	meta->st.st_dev = rdev;
	meta->st.st_nlink = 1;
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	meta->st.st_uid = current_uid().val;
	meta->st.st_gid = current_gid().val;
#else
	meta->st.st_uid = current_uid();
	meta->st.st_gid = current_gid();
#endif
	meta->st.st_blksize = FTFS_BSTORE_BLOCKSIZE;
	meta->st.st_atime = now;
	meta->st.st_mtime = now;
	meta->st.st_ctime = now;
}

/*
 * Create a file
 */
static int ftfs_create(struct inode *dir, struct dentry *dentry, umode_t mode,
		bool excl)
{
	struct inode *inode;
	char *path, *path_buf;
	int ret;
	DB_TXN *txn = NULL;
	struct ftfs_metadata meta;
	DBT meta_key;

	path_buf = kmalloc(PATH_MAX, GFP_KERNEL);
	if (!path_buf) {
		ret = -ENOMEM;
		goto out;
	}
	ftfs_setup_metadata(&meta, mode, 0, 0);
	inode = ftfs_setup_inode(dir->i_sb, &meta,
				iunique(dir->i_sb, FTFS_ROOT_INO));

	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		goto out1;
	}
	path = dentry_path_raw(dentry, path_buf, PATH_MAX);
	ret = alloc_and_gen_meta_key_dbt(&meta_key, path);
	if (ret)
		goto out1;

	TXN_GOTO_LABEL(retry_create);

	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);

	ret = ftfs_metadata_create(&meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry_create);
		bstore_txn_abort(txn);
		goto abort;
	}
	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry_create);

	d_instantiate(dentry, inode);
out2:
	kfree(meta_key.data);
out1:
	kfree(path_buf);
out:
	return ret;
abort:
	__destroy_inode(inode);
	ftfs_destroy_inode(inode);
	goto out2;
}

static struct inode *ftfs_iget(struct super_block *sb, const char *path,
			DB_TXN *txn);

/*
 * Get inode using directory and name
 * do we need a transaction here?
 */
static struct dentry *ftfs_lookup(struct inode *dir, struct dentry *dentry,
	unsigned int flags)
{
	char *buf, *path;
	struct inode *inode;
	DB_TXN *txn = NULL;

	/* hope we can get full path from dentry directly,
	 * otherwise we have to get it from dir somehow */
	buf = kmalloc(PATH_MAX, GFP_KERNEL);
	if (!buf) {
		inode = ERR_PTR(-ENOMEM);
		goto out;
	}

	path = dentry_path_raw(dentry, buf, PATH_MAX);

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_READONLY);
	inode = ftfs_iget(dir->i_sb, path, txn);
	if (IS_ERR(inode)) {
		int err = PTR_ERR(inode);
		if (err == -ENOENT) {
			inode = NULL;
		} else {
			DBOP_JUMP_ON_CONFLICT(err, retry);
			bstore_txn_abort(txn);
			goto free;
		}
	}
#ifdef TXN_ON
	if(bstore_txn_commit(txn, DB_TXN_NOSYNC))
		BUG(); /* wkj: I don't know how to handle an error
			* here. put the inode and try again? */
#endif
free:
	kfree(buf);
out:
	return d_splice_alias(inode, dentry);
}

/*
 * real do unlink, used by rename and unlink
 * file is guaranteed to exist by those functions
 *
 * must wait until txn is committed to
 *	clear_nlink(inode);
 *	mark_inode_dirty(inode);
 * make this the caller's responsibility
 */
static inline int ftfs_do_unlink(DBT *meta_key, DB_TXN *txn,
				struct inode *inode)
{
	int ret = ftfs_metadata_delete(meta_key, txn);
	if (ret)
		return ret;

	if (i_size_read(inode) > 0)
		ret = ftfs_bstore_truncate(meta_key, txn, 0);

	return ret == DB_NOTFOUND ? 0 : ret;
}

/*
 * Unlink a file
 */
static int ftfs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = dentry->d_inode;
	int ret;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);

	if (IS_ERR(meta_key))
		return PTR_ERR(meta_key);

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_do_unlink(&meta_key->dbt, txn, inode);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		bstore_txn_abort(txn);
	} else {
		ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
		if (i_size_read(inode) > HOT_FLUSH_THRESHOLD)
			bstore_hot_flush(&meta_key->dbt, 0,
					 block_get_num_by_position(
						i_size_read(inode)));

		clear_nlink(inode);
		mark_inode_dirty(inode);
	}

	ftfs_i_put_dbt(meta_key);
	return ret;
}

/*
 * Make a directory. Nearly the same.
 */
static int ftfs_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode)
{
	return ftfs_create(dir, dentry, mode | S_IFDIR, 0);
}

/*
 * Remove a directory
 */
static int ftfs_rmdir(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode  = dentry->d_inode;
	int ret, err;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);

	if (IS_ERR(meta_key))
		return PTR_ERR(meta_key);
	if (strcmp(get_path_from_meta_key_dbt(&meta_key->dbt), "/") == 0) {
		ret = -EINVAL;
		goto out;
	}

	if (!S_ISDIR(inode->i_mode)) {
		ret = -ENOTDIR;
		goto out;
	}

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);

	err = directory_is_empty(&meta_key->dbt, txn, &ret);
	if (err) {
		DBOP_JUMP_ON_CONFLICT(err, retry);
		/* not sure what errors we could get besides
		 * recoverable txn conflicts, but they surely aren't good
		 * ones... */
		BUG();
	}

	if (!ret) {
		/* i think commit is fine after read-only operations,
		 * and believe that this will be faster than aborting */
		bstore_txn_commit(txn, DB_TXN_NOSYNC);
		ret = -ENOTEMPTY;
		goto out;
	}

	ret = ftfs_metadata_delete(&meta_key->dbt, txn);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		bstore_txn_abort(txn);
		goto out;
	}

	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	clear_nlink(inode);
	mark_inode_dirty(inode);
out:
	ftfs_i_put_dbt(meta_key);
	return ret;
}

/*
 * we are not just renaming to files (old->new), we are renaming
 * entire subtrees of files.
 *
 * we lock the whole subtree before rename for exclusive access. for
 * either success or fail, you have to call unlock or else you are
 * hosed
 */
static int prelock_children_for_rename(struct dentry *parent,
				struct list_head *locked)
{
	struct dentry *this_parent;
	struct list_head *next;
	struct inode *inode;

	this_parent = parent;
start:
	if (this_parent->d_sb != parent->d_sb)
		goto end;
	inode = this_parent->d_inode;
	if (inode == NULL)
		goto repeat;
	mutex_lock(&inode->i_mutex);
	list_add(&FTFS_I(inode)->rename_locked, locked);

repeat:
	next = this_parent->d_subdirs.next;
resume:
	while (next != &this_parent->d_subdirs) {
		this_parent = list_entry(next, struct dentry, d_u.d_child);
		goto start;
	}
end:
	if (this_parent != parent) {
		next = this_parent->d_u.d_child.next;
		this_parent = this_parent->d_parent;
		goto resume;
	}
	return 0;
}

static int unlock_children_after_rename(struct list_head *locked, int updated)
{
	struct ftfs_inode_info *inode, *tmp;

	list_for_each_entry_safe(inode, tmp, locked, rename_locked) {
		if (updated)
			ftfs_i_clear_dbt(inode);
		mutex_unlock(&inode->vfs_inode.i_mutex);
		list_del_init(&inode->rename_locked);
	}
	return 0;
}

/*
 * Rename
 * Note: old_dir and new_dir args are both parent folder inode
 * it's not really the files you want to operate on.
 *
 * rename is really ugly due to full-path keys.
 */
static int ftfs_rename(struct inode *old_dir, struct dentry *old_dentry,
		struct inode *new_dir, struct dentry *new_dentry)
{
	int ret, err;
	int del_new_inode;
	struct inode *old_inode, *new_inode;
	char *old_path, *new_path, *new_path_buf;
	LIST_HEAD(locked_children);
	DB_TXN *txn = NULL;
	struct ftfs_dbt *old_meta_key, *new_meta_key;

	new_path_buf = NULL;
	old_inode = old_dentry->d_inode;
	old_meta_key = ftfs_i_get_dbt(old_inode);
	if (IS_ERR(old_meta_key))
		return PTR_ERR(old_meta_key);

	new_inode = new_dentry->d_inode;

	prelock_children_for_rename(old_dentry,
				&locked_children);

	TXN_GOTO_LABEL(retry);

	del_new_inode = 0;
	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);

	new_meta_key = NULL;
	if (new_inode != NULL) {
		new_meta_key = ftfs_i_get_dbt(new_inode);
		if (IS_ERR(new_meta_key)) {
			ret = PTR_ERR(new_meta_key);
			goto dbt_put_abort;
		}

		if (S_ISDIR(old_inode->i_mode)) {
			if (!S_ISDIR(new_inode->i_mode)) {
				ret = -ENOTDIR;
				goto dbt_put_new_abort;
			}
			err = directory_is_empty(&new_meta_key->dbt, txn, &ret);
			if (err) {
				DBOP_JUMP_ON_CONFLICT(err, retry);
				/* not sure what errors we could get
				 * besides recoverable txn conflicts,
				 * but they surely aren't good
				 * ones... */
				BUG();
			}

			if (!ret) {
				ret = -ENOTEMPTY;
				goto dbt_put_new_abort;
			}

			/* guaranteed to be empty */
			ret = ftfs_metadata_delete(&new_meta_key->dbt, txn);
			if (ret) {
				DBOP_JUMP_ON_CONFLICT(ret, retry);
				goto dbt_put_new_abort;
			} else {
				del_new_inode = 1;
			}

		} else {
			if (S_ISDIR(new_inode->i_mode)) {
				ret = -ENOTDIR;
				goto dbt_put_new_abort;
			}

			ret = ftfs_do_unlink(&new_meta_key->dbt, txn, new_inode);
			if (ret) {
				DBOP_JUMP_ON_CONFLICT(ret, retry);
				goto dbt_put_new_abort;
			}
			del_new_inode = 1;
		}
		new_path = get_path_from_meta_key_dbt(&new_meta_key->dbt);
	} else {
		new_path_buf = kmalloc(PATH_MAX, GFP_KERNEL);
		if (new_path_buf == NULL) {
			ret = -ENOMEM;
			goto dbt_put_abort;
		}

		new_path = dentry_path_raw(new_dentry, new_path_buf, PATH_MAX);
	}
	old_path = get_path_from_meta_key_dbt(&old_meta_key->dbt);
	ret = ftfs_bstore_rename_prefix(old_path, new_path, txn);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto dbt_put_new_abort;
	}
	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	if (del_new_inode) {
		/* if we called unlink on new_inode, we had to wait to
		 * modify the vfs inode until after txn commit */
		clear_nlink(new_inode);
		mark_inode_dirty(new_inode);
	}

	unlock_children_after_rename(&locked_children, 1);

	if (new_meta_key)
		ftfs_i_put_dbt(new_meta_key);
	else
		kfree(new_path_buf);
	ftfs_i_put_dbt(old_meta_key);

	return ret;

abort:
	bstore_txn_abort(txn);
	unlock_children_after_rename(&locked_children, 0);
	return ret;
dbt_put_new_abort:
	if (new_meta_key)
		ftfs_i_put_dbt(new_meta_key);
	else
		kfree(new_path_buf);
dbt_put_abort:
	ftfs_i_put_dbt(old_meta_key);
	goto abort;
}

static int ftfs_setattr(struct dentry *dentry, struct iattr *iattr)
{
	struct inode *inode = dentry->d_inode;
	DB_TXN *txn = NULL;
	int ret;
	struct ftfs_dbt *meta_key;

	ret = inode_change_ok(inode, iattr);
	if (ret)
		goto out;

	if (iattr->ia_valid & ATTR_SIZE) {
		meta_key = ftfs_i_get_dbt(inode);
		if (IS_ERR(meta_key))
			return PTR_ERR(meta_key);
		/* trunc */
		if (iattr->ia_size < i_size_read(inode)) {
			uint64_t block_num =
				block_get_num_by_position(iattr->ia_size);

			if (block_get_offset_by_position(iattr->ia_size))
				block_num++;

			TXN_GOTO_LABEL(retry);

			bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
			ret = ftfs_bstore_truncate(&meta_key->dbt, txn,
						block_num);
			if (ret) {
				DBOP_JUMP_ON_CONFLICT(ret, retry);
				goto abort;
			}
			ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
			COMMIT_JUMP_ON_CONFLICT(ret, retry);
		}
		ftfs_i_put_dbt(meta_key);
		i_size_write(inode, iattr->ia_size);
	}

	setattr_copy(inode, iattr);
	mark_inode_dirty(inode);

out:
	return ret;
abort:
	bstore_txn_abort(txn);
	ftfs_i_put_dbt(meta_key);
	return ret;
}

/*
 *	Create a symlink.
 * Note: creat a symlink file /from/a target at /to/b
 *       dir is inode for /from, dentry is dentry for /from/a
 *       symname is the LINK_NAME in you command
 */
static int ftfs_symlink(struct inode *dir, struct dentry *dentry,
	const char *symname)
{
	struct inode *inode;
	char *path, *path_buf;
	int ret;
	struct ftfs_metadata meta;
	DB_TXN *txn = NULL;
	DBT meta_key;
	unsigned l = strlen(symname);

	/* we don't want it to exceed 1 block, just as ext2 */
	if (l > dir->i_sb->s_blocksize) {
		ret = -ENAMETOOLONG;
		goto out;
	}

	path_buf = kmalloc(PATH_MAX, GFP_KERNEL);
	if (!path_buf) {
		ret = -ENOMEM;
		goto out;
	}

	ftfs_setup_metadata(&meta, S_IFLNK | S_IRWXUGO, l, 0);
	inode = ftfs_setup_inode(dir->i_sb, &meta,
				iunique(dir->i_sb, FTFS_ROOT_INO));
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		goto out1;
	}
	path = dentry_path_raw(dentry, path_buf, PATH_MAX);
	ret = alloc_and_gen_meta_key_dbt(&meta_key, path);
	if (ret)
		goto out1;

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);

	/* check if file exists */
	ret = ftfs_bstore_meta_get(path, txn, &meta, FTFS_METADATA_SIZE);
	if (ret == 0) {
		ret = -EEXIST;
		goto abort;
	} else {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
	}

	ret = ftfs_bstore_put(&meta_key, txn, 0, symname);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		if (ret == FTFS_BSTORE_NOMEM)
			ret = -ENOMEM;
		goto abort;
	}

	ret = ftfs_metadata_create(&meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}

	d_instantiate(dentry, inode);
	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);
out2:
	kfree(meta_key.data);
out1:
	kfree(path_buf);
out:
	return ret;
abort:
	bstore_txn_abort(txn);
	inode_dec_link_count(inode);
	iput(inode);
	goto out2;
}

/*
 * follow a symbolic link to the inode it points to
 * returned a cookie passed to put link
 */
static void *ftfs_follow_link(struct dentry *dentry, struct nameidata *nd)
{
	int ret;
	char *buf;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(dentry->d_inode);

	if (IS_ERR(meta_key))
		return meta_key;

	buf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	if (buf == NULL) {
		return ERR_PTR(-ENOMEM);
	}

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_READONLY);
	ret = ftfs_bstore_get(&meta_key->dbt, txn, 0, buf);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		bstore_txn_abort(txn);
		if (ret == FTFS_BSTORE_NOMEM)
			ret = -ENOMEM;
		return ERR_PTR(ret);
	}
	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	nd_set_link(nd, buf);

	return (void *)buf;
}

/*
 * called by vfs to release resources allocated by follow_link
 */
static void ftfs_put_link(struct dentry *dentry, struct nameidata *nd,
			void *cookie)
{
	if (IS_ERR(cookie))
		return;
	kfree(cookie);
}


static inline int ftfs_read_one_page(struct inode *inode,
					struct page *page)
{
	int ret, r;
	struct scan_buf sbuf;
	loff_t isize;
	uint64_t block_num, end_block_num;

	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);

	sbuf.bytes_read = 0;
	isize = i_size_read(inode);
	sbuf.offset = page_offset(page);

	if (sbuf.offset < isize)
		sbuf.len = min(isize - sbuf.offset, (loff_t)PAGE_SIZE);
	else
		return 0; /* nothing to read */

	block_num = block_get_num_by_position(sbuf.offset);
	end_block_num = block_get_num_by_position(sbuf.offset + sbuf.len);

	sbuf.buf = kmap(page);
	if (!sbuf.buf)
		return -ENOMEM;


	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_READONLY);
	ret = ftfs_bstore_scan(&meta_key->dbt, txn, block_num, end_block_num,
			bstore_fill_block, &sbuf);
	DBOP_JUMP_ON_CONFLICT(ret, retry);
	r = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

	/* zero fill end-of-file pages */

	if (sbuf.len < PAGE_SIZE)
		memset(sbuf.buf, 0, PAGE_SIZE - sbuf.len);

	kunmap(page);
	return ret;
}

static inline ssize_t
ftfs_writen(struct inode *inode, char *buf, size_t len, loff_t offset)
{
	ssize_t retval, write_size;
	int r;
	uint64_t block_num;
	size_t block_offset;
	DB_TXN *txn = NULL;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(inode);

	if (IS_ERR(meta_key)) {
		retval = PTR_ERR(meta_key);
		goto out;
	}

	TXN_GOTO_LABEL(retry);

	retval = 0;
	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	while (len > 0) {
		block_num = block_get_num_by_position(offset);
		block_offset = block_get_offset_by_position(offset);
		write_size = min(len, (FTFS_BSTORE_BLOCKSIZE - block_offset));
		BUG_ON(write_size <= 0);

		if (write_size == FTFS_BSTORE_BLOCKSIZE) {
			r = ftfs_bstore_put(&meta_key->dbt, txn,
					block_num, buf);
		} else {
			/* alternatively, we can do a put for last block here.
			 * Doing that needs read file size in this function
			 * and an additional if, but may gain performance by
			 * replacing update with */
			r = ftfs_bstore_update(&meta_key->dbt, txn,
					block_num, buf,
					write_size, block_offset);
		}

		if (r) {
			DBOP_JUMP_ON_CONFLICT(r, retry);
			bstore_txn_abort(txn);
			retval = r;
			goto out1;
		}

		buf += write_size;
		retval += write_size;
		offset += write_size;
		len -= write_size;
	}

	r = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

out1:
	ftfs_i_put_dbt(meta_key);
out:
	return retval;
}

static inline int
__ftfs_writepage(struct page *page, size_t len, loff_t offset)
{
	int ret;
	char *buf;
	struct inode *inode;

	inode = page->mapping->host;
	set_page_writeback(page);
	buf = kmap(page);
	if (offset & ~PAGE_CACHE_MASK)
		buf = &buf[offset & ~PAGE_CACHE_MASK];
	ret = ftfs_writen(inode, buf, len, offset);
	kunmap(page);
	end_page_writeback(page);

	return ret;
}

/**
 * ftfs_readpages - populate an address space with some pages & start
 * reads against them
 * @filp: file pointer
 * @mapping: the address_space to populate
 * @pages: The address of a list_head which contains the target pages.  These
 *   pages have their ->index populated and are otherwise uninitialised.
 *   The page at @pages->prev has the lowest file offset, and reads should be
 *   issued in @pages->prev to @pages->next order.
 * @nr_pages: The number of pages at *@pages
 *
 * This function walks the pages and populates them from the bstore,
 * using bulk fetch so that the basement nodes are cached. This is
 * handled by the cursor flags used in __helper__
 */

static int ftfs_readpages(struct file *filp, struct address_space *mapping,
			struct list_head *pages, unsigned nr_pages)
{
	int ret;
	struct ftfs_dbt *meta_key = ftfs_i_get_dbt(mapping->host);
	if (IS_ERR(meta_key)) {
		ret = PTR_ERR(meta_key);
		goto out;
	}

	ret = ftfs_bstore_scan_pages(meta_key, pages, nr_pages, mapping);
	ftfs_i_put_dbt(meta_key);
out:
	return ret;
}

static int ftfs_readpage(struct file *file, struct page *page)
{
	int ret;
	struct inode *inode = page->mapping->host;

	ret = ftfs_read_one_page(inode, page);
	if (ret < 0)
		goto out;

	flush_dcache_page(page);
	SetPageUptodate(page);
	ret = 0;
out:
	unlock_page(page);
	return ret;
}

static int ftfs_writepage(struct page *page, struct writeback_control *wbc)
{
	int ret;
	loff_t len, size;
	struct inode *inode = page->mapping->host;

	size = i_size_read(inode);
	if (page->index == size >> PAGE_CACHE_SHIFT)
		len = size & ~PAGE_CACHE_MASK;
	else
		len = PAGE_CACHE_SIZE;

	ret =  __ftfs_writepage(page, len, page_offset(page));

	if (ret > 0) {
		ret = 0;
	} else if (ret < 0) {
		if (ret == -EAGAIN) {
			redirty_page_for_writepage(wbc, page);
			ret = 0;
		} else {
			SetPageError(page);
			mapping_set_error(page->mapping, ret);
		}
	}

	unlock_page(page);

	return ret;
}

static int ftfs_write_begin(struct file *file, struct address_space *mapping,
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

static int ftfs_write_end(struct file *file, struct address_space *mapping,
			  loff_t pos, unsigned len, unsigned copied,
			  struct page *page, void *fsdata)
{
	/* make sure that ftfs can't guarantee uptodate page */
	loff_t last_pos = pos + copied;
	int i_size_changed = 0;
	struct inode *inode = page->mapping->host;
	int ret;

	/*
	 * 1. if page is uptodate/writesize=PAGE_SIZE (we have new content),
	 *	write to page cache and wait for generic_writepage to write
	 *	to disk (generic aio style);
	 * 2. if not, only write to disk so that we avoid read-before-write.
	 */
	if (copied != PAGE_CACHE_SIZE &&
	    (!PageUptodate(page) || !PageDirty(page))) {
		ret = __ftfs_writepage(page, copied, pos);
		BUG_ON(ret < 0);
		clear_page_dirty_for_io(page);
	} else {
		SetPageUptodate(page);
		if (!PageDirty(page))
			__set_page_dirty_nobuffers(page);
	}

	/* holding i_mutconfigex */
	if (last_pos > inode->i_size) {
		i_size_write(inode, last_pos);
		i_size_changed = 1;
	}

	unlock_page(page);
	page_cache_release(page);

	/* do as generic_write_end */
	if (i_size_changed)
		mark_inode_dirty(inode);

	return copied;
}

static const struct inode_operations ftfs_special_inode_operations = {
	 /* special file operations. Add functions as needed */
	.setattr		= ftfs_setattr,
};

static int ftfs_mknod(struct inode *dir, struct dentry *dentry,  umode_t mode,
		dev_t rdev)
{
	struct inode *inode;
	char *path, *path_buf;
	int ret;
	DB_TXN *txn;
	struct ftfs_metadata meta;
	DBT meta_key;

	if (!new_valid_dev(rdev)) {
		ret = -EINVAL;
		goto out;
	}
	path_buf = kmalloc(PATH_MAX, GFP_KERNEL);
	if (!path_buf) {
		ret = -ENOMEM;
		goto out;
	}
	ftfs_setup_metadata(&meta, mode, 0, rdev);
	inode = ftfs_setup_inode(dir->i_sb, &meta,
				iunique(dir->i_sb, FTFS_ROOT_INO));
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		goto out1;
	}
	path = dentry_path_raw(dentry, path_buf, PATH_MAX);
	ret = alloc_and_gen_meta_key_dbt(&meta_key, path);
	if (ret)
		goto out1;

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_metadata_create(&meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}

	inode = ftfs_iget(dir->i_sb, path, txn);
	if (IS_ERR(inode)) {
		ret = PTR_ERR(inode);
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}

	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	d_instantiate(dentry, inode);

out2:
	kfree(meta_key.data);
out1:
	kfree(path_buf);
out:
	return ret;
abort:
	bstore_txn_abort(txn);
	inode_dec_link_count(inode);
	iput(inode);
	goto out2;
}


static int ftfs_fsync(struct file *file, loff_t start, loff_t end,
                      int datasync)
{
	int ret = generic_file_fsync(file, start, end, datasync);

	if (!ret)
		ret = bstore_flush_log();

	return ret;
}

static int ftfs_sync_fs(struct super_block *sb, int wait)
{
	int ret = bstore_flush_log();

	if (!ret)
		ret = sync_blockdev(sb->s_bdev);

	return ret;
}

static const struct address_space_operations ftfs_aops = {
	.readpage		= ftfs_readpage,
	.readpages		= ftfs_readpages,
	.writepage		= ftfs_writepage,
	.write_begin		= ftfs_write_begin,
	.write_end		= ftfs_write_end,
};

static const struct file_operations ftfs_file_operations = {
	/* file file operations */
	.llseek			= generic_file_llseek,
	.fsync			= ftfs_fsync,
#ifdef NOT_PAGECACHE
	.read			= ftfs_file_read,
	.write			= ftfs_file_write,
#else
	.read			= do_sync_read,
	.write			= do_sync_write,
	.aio_read		= generic_file_aio_read,
	.aio_write		= generic_file_aio_write,
#endif
	.mmap			= generic_file_mmap,
};

static const struct inode_operations ftfs_file_inode_operations = {
	/* file inode operations */
	.setattr		= ftfs_setattr,
};

static const struct file_operations ftfs_dir_operations = {
	/* dir file operations */
	.read			= generic_read_dir,
	.iterate		= ftfs_readdir,
	.fsync			= generic_file_fsync,
};

static const struct inode_operations ftfs_dir_inode_operations = {
	/* dir inode operations */
	.create			= ftfs_create,
	.lookup			= ftfs_lookup,
	.unlink			= ftfs_unlink,
	.symlink		= ftfs_symlink,
	.mkdir			= ftfs_mkdir,
	.rmdir			= ftfs_rmdir,
	.rename			= ftfs_rename,
	.setattr		= ftfs_setattr,
	.mknod			= ftfs_mknod,
};

static const struct inode_operations ftfs_symlink_inode_operations = {
	.readlink		= generic_readlink,
	.follow_link		= ftfs_follow_link,
	.put_link		= ftfs_put_link,
	.setattr		= ftfs_setattr,
};

static const struct super_operations ftfs_super_ops = {
	/* add functions as needed */
	.alloc_inode		= ftfs_alloc_inode,
	.destroy_inode		= ftfs_destroy_inode,
	.write_inode		= ftfs_write_inode,
	.statfs			= ftfs_super_statfs,
	.sync_fs		= ftfs_sync_fs,
};

static struct inode *ftfs_setup_inode(struct super_block *sb,
				struct ftfs_metadata *meta, ino_t ino)
{
	struct inode *i;

	/* XXX: inode use ino for hash key by default,
	 * but we don't have it. So hash maybe useless here.
	 * path is not a good idea, either */
	i = iget_locked(sb, ino);
	if (!i) {
		return ERR_PTR(-ENOMEM);
	}
	if (!(i->i_state & I_NEW))
		return i;
	i->i_rdev = meta->st.st_dev;
	i->i_ino = ino;
	i->i_mode = meta->st.st_mode;
	set_nlink(i, meta->st.st_nlink);
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	i->i_uid.val = meta->st.st_uid;
	i->i_gid.val = meta->st.st_gid;
#else
	i->i_uid = meta->st.st_uid;
	i->i_gid = meta->st.st_gid;
#endif
	i->i_size = meta->st.st_size;
	i->i_blocks = meta->st.st_blocks;
	TIME_T_TO_TIMESPEC(i->i_atime, meta->st.st_atime);
	TIME_T_TO_TIMESPEC(i->i_mtime, meta->st.st_mtime);
	TIME_T_TO_TIMESPEC(i->i_ctime, meta->st.st_ctime);

	if (S_ISREG(i->i_mode)) {
		/* Regular file */
		i->i_op = &ftfs_file_inode_operations;
		i->i_fop = &ftfs_file_operations;
		i->i_data.a_ops = &ftfs_aops;
	} else if (S_ISDIR(i->i_mode)) {
		/* Directory */
		i->i_op = &ftfs_dir_inode_operations;
		i->i_fop = &ftfs_dir_operations;
	} else if (S_ISLNK(i->i_mode)) {
		/* Sym link */
		i->i_op = &ftfs_symlink_inode_operations;
		i->i_data.a_ops = &ftfs_aops;
	} else  if (S_ISCHR(i->i_mode) || S_ISBLK(i->i_mode) ||
		S_ISFIFO(i->i_mode) || S_ISSOCK(i->i_mode)) {
		i->i_op = &ftfs_special_inode_operations;
		init_special_inode(i, i->i_mode, i->i_rdev); /* duplicates
							      * a small amount
							      * of work */
	} else {
		BUG();
	}


	unlock_new_inode(i);
	return i;
}

/*
 * get a ftfs inode based on its path
 */
static struct inode *ftfs_iget(struct super_block *sb, const char *path,
			DB_TXN *txn)
{
	int ret;
	struct ftfs_metadata meta;

	ret = ftfs_bstore_meta_get(path, txn, &meta, FTFS_METADATA_SIZE);
	if (ret) {
#ifdef TXN_ON
	IF_DBOP_CONFLICT(ret)
			return ERR_PTR(ret);
#endif
		return ERR_PTR(-ENOENT);
	}

	return ftfs_setup_inode(sb, &meta, iunique(sb, FTFS_ROOT_INO));
}

static inline int ftfs_create_path(const char *path, DB_TXN *txn,
				struct ftfs_metadata *meta)
{
	DBT meta_key;
	int ret;

	ret = alloc_and_gen_meta_key_dbt(&meta_key, path);
	if (ret)
		goto out;
	ret = ftfs_metadata_create(&meta_key, txn, meta);

	kfree(meta_key.data);
out:
	return ret;
}

/*
 * fill in the superblock
 */
static int ftfs_fill_super(struct super_block *sb, void *data, int silent)
{
	int ret;
	struct inode *root = NULL;
	struct ftfs_metadata meta;
	DB_TXN *txn = NULL;
	ret = ftfs_bstore_env_open();
	BUG_ON(ret != 0);

	sb_set_blocksize(sb, FTFS_BSTORE_BLOCKSIZE);
	sb->s_op = &ftfs_super_ops;
	sb->s_maxbytes = MAX_LFS_FILESIZE;

	TXN_GOTO_LABEL(retry);

	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_meta_get("/", txn, &meta, FTFS_METADATA_SIZE);
	if (ret) {
#ifdef TXN_ON
	IF_DBOP_CONFLICT(ret)
			goto retry;
#endif
		if (ret == FTFS_BSTORE_NOTFOUND) {
			ftfs_setup_metadata(&meta, 0755 | S_IFDIR, 0, 0);
			ret = ftfs_create_path("/", txn, &meta);
			DBOP_JUMP_ON_CONFLICT(ret, retry);
			BUG_ON(ret != 0);
		}
	}
	/*
	 * I think we want this one to be durable, and it (mounting)
	 * is not exactly a performance critical operation
	 */
	ret = bstore_txn_commit(txn, DB_TXN_SYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	root = ftfs_setup_inode(sb, &meta, FTFS_ROOT_INO);
	if (IS_ERR(root))
		return PTR_ERR(root);

	sb->s_root = d_make_root(root);
	if (!sb->s_root)
		return -EINVAL;

	return 0;
}

/*
 * get a superblock for mounting
 */
static struct dentry *ftfs_mount(struct file_system_type *fs_type,
	int flags, const char *dev_name, void *data)
{
	return mount_bdev(fs_type, flags, dev_name, data, ftfs_fill_super);
}

/*
 * destry a ftfs superblock
 */
static void ftfs_kill_sb(struct super_block *sb)
{
	struct block_device *bdev = sb->s_bdev;

	if (sb->s_root) {
		bstore_checkpoint();
		sync_filesystem(sb);
	}
	sync_blockdev(bdev);

	kill_block_super(sb);

	ftfs_bstore_env_close();
}

static struct file_system_type ftfs_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "ftfs",
	.mount		= ftfs_mount,
	.kill_sb	= ftfs_kill_sb,
	.fs_flags	= FS_REQUIRES_DEV,
};

static void ftfs_i_init_once(void *_inode)
{
	struct ftfs_inode_info *inode = _inode;

	inode->i_dbt = NULL;

	inode_init_once(&inode->vfs_inode);
}

int init_ftfs_fs(void)
{
	int ret;

	ftfs_inode_cachep =
		kmem_cache_create("ftfs_i",
				sizeof(struct ftfs_inode_info), 0,
				SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD,
				ftfs_i_init_once);
	if (!ftfs_inode_cachep) {
		printk(KERN_ERR
			"FTFS error: Failed to initialize inode cache\n");
		return -ENOMEM;
	}

	ret = register_filesystem(&ftfs_fs_type);
	if (ret) {
		printk(KERN_ERR
			"FTFS error: Failed to register filesystem\n");
		goto error_register;
	}
	return 0;

error_register:
	kmem_cache_destroy(ftfs_inode_cachep);
	return ret;
}

void exit_ftfs_fs(void)
{
	unregister_filesystem(&ftfs_fs_type);

	kmem_cache_destroy(ftfs_inode_cachep);
}
