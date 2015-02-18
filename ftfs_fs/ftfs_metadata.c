#include <linux/sched.h>
#include <linux/cred.h>
#include <linux/slab.h>

#include "ftfs_fs.h"

/*
 * add an entry to metadata table
 */
int
ftfs_metadata_create(DBT *meta_key, DB_TXN *txn, struct ftfs_metadata *meta)
{
	return ftfs_bstore_meta_put(meta_key, txn, meta);
}

/*
 * delete an entry in metadata table
 */
int
ftfs_metadata_delete(DBT *meta_key, DB_TXN *txn)
{
	return ftfs_bstore_meta_delete(meta_key, txn);
}

/*
 * write back a dirty inode, use put directly
 */
int
ftfs_metadata_wb(DBT *meta_key, struct inode *i)
{
	int ret;
	DB_TXN *txn;
	struct ftfs_metadata meta;

	meta.st.st_dev = i->i_rdev;
	meta.st.st_mode = i->i_mode;
	meta.st.st_nlink = i->i_nlink;
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	meta.st.st_uid = i->i_uid.val;
	meta.st.st_gid = i->i_gid.val;
#else
	meta.st.st_uid = i->i_uid;
	meta.st.st_gid = i->i_gid;
#endif
	meta.st.st_size = i->i_size;
	meta.st.st_blocks = i->i_blocks;
	TIMESPEC_TO_TIME_T(meta.st.st_atime, i->i_atime);
	TIMESPEC_TO_TIME_T(meta.st.st_mtime, i->i_mtime);
	TIMESPEC_TO_TIME_T(meta.st.st_ctime, i->i_ctime);
retry:
	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_meta_put(meta_key, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		bstore_txn_abort(txn);
	} else {
		ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	return ret;
}
