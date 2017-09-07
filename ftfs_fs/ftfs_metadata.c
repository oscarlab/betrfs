/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include <linux/sched.h>
#include <linux/cred.h>
#include <linux/slab.h>

#include "ftfs_fs.h"

/*
 * add an entry to metadata table
 */
int
ftfs_metadata_create(struct ftfs_meta_key *mkey, DB_TXN *txn, struct ftfs_metadata *meta)
{
	return ftfs_bstore_meta_put(mkey, txn, meta);
}

/*
 * delete an entry in metadata table
 */
int
ftfs_metadata_delete(struct ftfs_meta_key *mkey, DB_TXN *txn)
{
	return ftfs_bstore_meta_delete(mkey, txn);
}

/*
 * write back a dirty inode, use put directly
 */
int
ftfs_metadata_wb(struct ftfs_meta_key *mkey, struct inode *i)
{
	int ret;
	DB_TXN *txn;
	struct ftfs_metadata meta;

	meta.type = 0;
	meta.u.st.st_dev = i->i_rdev;
	meta.u.st.st_mode = i->i_mode;
	meta.u.st.st_nlink = i->i_nlink;
#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	meta.u.st.st_uid = i->i_uid.val;
	meta.u.st.st_gid = i->i_gid.val;
#else
	meta.u.st.st_uid = i->i_uid;
	meta.u.st.st_gid = i->i_gid;
#endif
	meta.u.st.st_size = i->i_size;
	meta.u.st.st_blocks = i->i_blocks;
	TIMESPEC_TO_TIME_T(meta.u.st.st_atime, i->i_atime);
	TIMESPEC_TO_TIME_T(meta.u.st.st_mtime, i->i_mtime);
	TIMESPEC_TO_TIME_T(meta.u.st.st_ctime, i->i_ctime);
retry:
	bstore_txn_begin(NULL, &txn, TXN_MAY_WRITE);
	ret = ftfs_bstore_meta_put(mkey, txn, &meta);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		bstore_txn_abort(txn);
	} else {
		ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	return ret;
}
