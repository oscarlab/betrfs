#ifndef FTFS_FS_H
#define FTFS_FS_H

#include <linux/list.h>
#include <asm/stat.h>
#include <linux/fs.h>
#include <linux/page-flags.h>
#include <linux/mm_types.h>
#include <linux/fscache.h>
#include "ftfs.h"
#include "ftfs_southbound.h"
#include "tokudb.h"

#define HOT_FLUSH_THRESHOLD 1 << 14

#define TXN_MAY_WRITE DB_SERIALIZABLE
#define TXN_READONLY (DB_TXN_SNAPSHOT|DB_TXN_READ_ONLY)
#define DB_PUT_FLAGS 0
#define DB_GET_FLAGS 0
#define DB_DEL_FLAGS 0
#define DB_UPDATE_FLAGS 0
#define DB_CURSOR_FLAGS 0
#define IF_DBOP_CONFLICT(err)						\
	if (((err)==DB_LOCK_DEADLOCK)||					\
		((err)==DB_LOCK_NOTGRANTED)||				\
		((err)==TOKUDB_INTERRUPTED)||				\
		((err)==TOKUDB_TRY_AGAIN)||				\
		((err)==TOKUDB_CANCELED))

#define TXN_ON 1
#ifdef TXN_ON
#define DBOP_JUMP_ON_CONFLICT(err, label)				\
	if (((err)==DB_LOCK_DEADLOCK)||					\
		((err)==DB_LOCK_NOTGRANTED)||				\
		((err)==TOKUDB_INTERRUPTED)||				\
		((err)==TOKUDB_TRY_AGAIN)||				\
		((err)==TOKUDB_CANCELED)) {				\
		ftfs_error(__func__, "dbop err=%d", (err));		\
		bstore_txn_abort(txn);					\
		goto label;						\
	}								\

#define COMMIT_JUMP_ON_CONFLICT(err, label)				\
	if (err) {							\
		if (((err)==DB_LOCK_DEADLOCK)||				\
			((err)==DB_LOCK_NOTGRANTED)||			\
			((err)==TOKUDB_INTERRUPTED)||			\
			((err)==TOKUDB_TRY_AGAIN)||			\
			((err)==TOKUDB_CANCELED)) {			\
			goto label;					\
		}							\
		ftfs_error(__func__, "txn commit err=%d", (err));	\
		BUG();							\
	}
#define TXN_GOTO_LABEL(label)       label:
#else					
#define DBOP_JUMP_ON_CONFLICT(err, label)				
#define COMMIT_JUMP_ON_CONFLICT(err, label)
#define TXN_GOTO_LABEL(label)
#endif

int init_ftfs_fs(void);
void exit_ftfs_fs(void);

/* if you want to change block size in ftfs,
 * modify FTFS_BSTORE_BLOCKSIZE_BITS */

#define FTFS_BSTORE_BLOCKSIZE_BITS		12
#define FTFS_BSTORE_BLOCKSIZE			\
		(1 << (FTFS_BSTORE_BLOCKSIZE_BITS))
#define block_get_num_by_position(pos)		\
		((pos) >> (FTFS_BSTORE_BLOCKSIZE_BITS))
#define block_get_offset_by_position(pos)	\
		((pos) & ((FTFS_BSTORE_BLOCKSIZE) - 1))

#define FTFS_SUPER_MAGIC 0xF3560
#define FTFS_ROOT_INO 1

#define TIME_T_TO_TIMESPEC(ts, t) do \
		{ \
			ts.tv_sec = t; \
			ts.tv_nsec = 0; \
		} while (0)

#define TIMESPEC_TO_TIME_T(t, ts) t = ts.tv_sec;

struct ftfs_dbt {
	DBT dbt;
	struct lockref lockref;
};

struct ftfs_inode_info {
	struct ftfs_dbt *i_dbt;
	struct list_head rename_locked;
	struct inode vfs_inode;
};

static inline struct ftfs_inode_info *FTFS_I(struct inode *inode)
{
	return container_of(inode, struct ftfs_inode_info, vfs_inode);
}

#define FTFS_BSTORE_NOTFOUND	-1
#define FTFS_BSTORE_NOMEM	-2

/* last bytes of data db key, 0 for meta db key */
#define DATA_DB_KEY_MAGIC 115

struct ftfs_metadata {
	struct stat st;
};
#define FTFS_METADATA_SIZE (sizeof(struct ftfs_metadata))

#ifdef FTFS_SHORT_BLOCK
typedef int (*filler_fn)(uint64_t block_num, void *block, size_t len,
			void *extra);
#else
typedef int (*filler_fn)(uint64_t block_num, void *block, void *extra);
#endif
typedef int (*bstore_meta_scan_callback_fn)(const char *name,
	void *meta, void *extra);

#define BSTORE_SCAN_CONTINUE	1
#define BSTORE_SCAN_NEXT_TIME	2


/* this needs to be wrapped in a function because db_env is a static
 * global. i just wrapped the others for consistency */
int bstore_txn_begin(DB_TXN *parent, DB_TXN **txn, int flags);
int bstore_flush_log(void);
int bstore_checkpoint(void);
int bstore_txn_commit(DB_TXN *txn, int flags);
int bstore_txn_abort(DB_TXN *txn);


int bstore_hot_flush(DBT *meta_key, uint64_t start, uint64_t end);
int bstore_hot_flush_all(void);

int ftfs_bstore_env_open(void);
int ftfs_bstore_env_close(void);

int ftfs_bstore_meta_get(const char *name, DB_TXN *txn, void *buf,
			size_t size);
int ftfs_bstore_meta_put(DBT *meta_key, DB_TXN *txn,
                         struct ftfs_metadata *meta);
int ftfs_bstore_meta_delete(DBT *meta_key, DB_TXN *txn);
int ftfs_bstore_meta_scan(const char *name, DB_TXN *txn,
			bstore_meta_scan_callback_fn cb, void *extra);
int ftfs_bstore_truncate(DBT *meta_key, DB_TXN *parent, uint64_t block_num);
int ftfs_bstore_rename_prefix(const char *oldprefix, const char *newprefix,
			DB_TXN *parent);
int ftfs_bstore_put(DBT *meta_key, DB_TXN *txn, uint64_t block_num,
                    const void *buf);
int ftfs_bstore_get(DBT *meta_key, DB_TXN *txn, uint64_t block_num, void *buf);
int ftfs_bstore_update(DBT *meta_key, DB_TXN *txn, uint64_t block_num,
                       const void *buf, size_t size, size_t offset);

struct readdir_scan_cb_info {
	loff_t skip;
	int root_first;
	char *dirname;
	struct dir_context *ctx;
	struct super_block *sb;
};
int readdir_scan_cb(const char *name, void *meta, void *extra);

struct scan_buf;

int ftfs_bstore_scan(DBT *meta_key, DB_TXN *txn,
                     uint64_t block_num, uint64_t prefetch_block_num,
                     filler_fn filler, struct scan_buf *sbuf);

struct scan_buf {
	void *buf;
	loff_t offset; /* current offset in file */
	size_t len; /* total length of buf (fixed) */
	size_t bytes_read; /* offset within buf */
	int more_to_come;
};

struct block_scan_cb_info {
	filler_fn filler;
	DBT *key;
	uint64_t prefetch_block_num; /* prefetch up to this block */
	struct scan_buf *scan_buf;
	int do_continue;
};

int ftfs_bstore_scan_pages(struct ftfs_dbt *meta_key,
			struct list_head *pages, unsigned nr_pages,
			struct address_space *mapping);

#define FTIO_MAX_INLINE 128
/* we reuse struct bio_vec because it is convenient */
struct ftio {
	//unsigned long ft_flags;
	//unsigned long ft_rw;
	unsigned short ft_max_vecs; /* maximum number of vecs */
	unsigned short ft_vcnt; /* how many fi_vecs populated */
	unsigned short ft_bvidx; /* current index into fi_vec */
	struct bio_vec *ft_bio_vec;
	struct bio_vec ft_inline_vecs[0];
};
static inline struct bio_vec *current_ftio_bvec(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	return ftio->ft_bio_vec + ftio->ft_bvidx;
}
static inline struct page *current_ftio_page(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	return (ftio->ft_bio_vec + ftio->ft_bvidx)->bv_page;
}

static inline void advance_ftio_page(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	ftio->ft_bvidx++;
}

static inline void unlock_ftio_pages(struct ftio *ftio)
{
	unsigned i;
	for (i = 0; i < ftio->ft_vcnt; i++)
		unlock_page((ftio->ft_bio_vec + i)->bv_page);
}

static inline void set_ftio_pages_uptodate(struct ftio *ftio)
{
	unsigned i;
	for (i = 0; i < ftio->ft_vcnt; i++)
		SetPageUptodate((ftio->ft_bio_vec + i)->bv_page);
}


static inline void set_ftio_pages_error(struct ftio *ftio)
{
	unsigned i;
	for (i = 0; i < ftio->ft_vcnt; i++) {
		struct page *page = (ftio->ft_bio_vec + i)->bv_page;
		ClearPageUptodate(page);
		SetPageError(page);
	}
}


static inline void advance_ftio_bytes(struct ftio *ftio, size_t bytes)
{
	struct bio_vec *bvec = current_ftio_bvec(ftio);
	bvec->bv_offset += bytes;
	BUG_ON(bvec->bv_offset > bvec->bv_len);
	if (bvec->bv_offset == bvec->bv_len)
		advance_ftio_page(ftio);
}

struct scan_pages_info {
	filler_fn filler;
	DBT *key;
	uint64_t prefetch_block_num; /* prefetch up to this block */
	struct ftio *ftio;
	int do_continue;
};

/* fills zeros up to and including target block */
int fill_sparse(struct scan_buf *info, uint64_t target_block);

static inline void update_scan_buf(struct scan_buf *sb,
	size_t read_size)
{
	sb->buf += read_size;
	sb->offset += read_size;
	sb->bytes_read += read_size;
}

#ifdef FTFS_SHORT_BLOCK
int bstore_fill_block(uint64_t current_block_num, void *block_buf, size_t len,
		void *extra);
#else
int bstore_fill_block(uint64_t current_block_num, void *block_buf, void *extra);
#endif

int meta_update_cb(const DBT *oldval, DBT *newval, void *extra);
struct ftfs_utime {
	time_t ctime, mtime, atime;
};

int ftfs_metadata_create(DBT *meta_key, DB_TXN *txn,
                         struct ftfs_metadata *meta);
int ftfs_metadata_delete(DBT *meta_key, DB_TXN *txn);
int ftfs_metadata_wb(DBT *meta_key, struct inode *inode);

struct directory_is_empty_info {
	const char *dirname;
	int saw_child;
};

int directory_is_empty(DBT *meta_key, DB_TXN *txn, int *is_empty);

int alloc_and_gen_meta_key_dbt(DBT *dbt, const char *name);
char *get_path_from_meta_key_dbt(DBT *dbt);

void ftfs_print_engine_status(void);
#endif /* FTFS_FS_H */
