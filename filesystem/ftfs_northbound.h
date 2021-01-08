/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef FTFS_FS_H
#define FTFS_FS_H

/* This file defines a number of internal APIs implemented and
 * used by the "northbound" code.
 */

#include <linux/list.h>
#include <linux/fs.h>
#include <linux/rwsem.h>
#include <linux/fscache.h>
#include <linux/list_sort.h>
#include <linux/slab.h>

#include "ftfs.h"
#include "tokudb.h"
#include "sb_malloc.h"

#define HOT_FLUSH_THRESHOLD 1 << 14

#define TXN_MAY_WRITE DB_SERIALIZABLE
#define TXN_READONLY (DB_TXN_SNAPSHOT|DB_TXN_READ_ONLY)
#define DB_PUT_FLAGS 0
#define DB_GET_FLAGS 0
#define DB_DEL_FLAGS DB_DELETE_ANY
#define DB_UPDATE_FLAGS 0
#define DB_CURSOR_FLAGS 0
#define DB_RENAME_FLAGS 0
#define DB_CLONE_FLAGS  0
#define DB_RD_FLAGS     0
#define DB_MOVE_FLAGS   (DB_CLONE_FLAGS | DB_CLONE_DELETE_OLD)

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

int init_ftfs_fs(void);
void exit_ftfs_fs(void);

#define FTFS_BSTORE_BLOCKSIZE_BITS	PAGE_CACHE_SHIFT
#define FTFS_BSTORE_BLOCKSIZE		PAGE_CACHE_SIZE

#define PAGE_TO_BLOCK_NUM(page)         ((uint64_t)((page->index) + 1))

// now data db starts with blocknum 1, blocknum 0 is reserved
#define block_get_num_by_position(pos)		\
		(((pos) >> (FTFS_BSTORE_BLOCKSIZE_BITS)) + 1)
#define block_get_off_by_position(pos)		\
		((pos) & ((FTFS_BSTORE_BLOCKSIZE) - 1))

#define nb_get_block_num_by_size(size)					\
	(((size) == 0) ? 0 : (((size) - 1) >> (FTFS_BSTORE_BLOCKSIZE_BITS)) + 1)

#define TIME_T_TO_TIMESPEC(ts, t) do \
		{ \
			ts.tv_sec = t; \
			ts.tv_nsec = 0; \
		} while (0)
#define TIMESPEC_TO_TIME_T(t, ts) t = ts.tv_sec;

#define FTFS_UINT64_MAX (0xFFFFFFFFFFFFFFFFULL)

#define FTFS_IOCTL_MAGIC 0x94
#define FTFS_IOC_CLONE   _IOW(FTFS_IOCTL_MAGIC, 9, int)

#define FTFS_ROOT_INO 0

#define LARGE_IO_THRESHOLD 256

#define FTFS_DEFAULT_CIRCLE 128
#define FTFS_INO_INC        1000

struct ftfs_info {
	ino_t next_ino;
	ino_t max_ino;
};

// FTFS superblock info
struct ftfs_sb_info {
	// DB info
	DB_ENV *db_env;
	DB *data_db;
	DB *meta_db;
	bool is_rotational;
	unsigned s_nr_cpus;
	struct ftfs_info __percpu *s_ftfs_info;
};

struct ftfs_inode {
	struct inode vfs_inode;
	struct rw_semaphore key_lock;
	DBT meta_dbt;
	struct list_head rename_locked;
	uint64_t ftfs_flags;
	/* the number of pages requests
	 * in last range query
	 */
	uint32_t last_nr_pages;
	/* offset of the last range query */
	uint64_t last_offset;
};

#define FTFS_FLAG_DELETED ((uint64_t)(1 << 0))

#define META_KEY_MAX_LEN (PATH_MAX)
#define DATA_KEY_MAX_LEN (META_KEY_MAX_LEN + sizeof(uint64_t))
#define KEY_MAX_LEN      (DATA_KEY_MAX_LEN)

#define BNUM_POS(size) (size - sizeof(uint64_t))

#define KEY_PATH(key) ((char *)(key))

static inline uint64_t
nb_data_key_get_blocknum(char *key, uint32_t key_size)
{
	return be64_to_cpu(*(uint64_t *)(key + BNUM_POS(key_size)));
}

static inline void
nb_data_key_set_blocknum(char *key, uint32_t key_size, uint64_t blocknum)
{
	*(uint64_t *)(key + BNUM_POS(key_size)) = cpu_to_be64(blocknum);
}


#define SIZEOF_META_KEY(key) (strlen(KEY_PATH(key)) + 1)
#define SIZEOF_DATA_KEY(key) (SIZEOF_META_KEY(key) + sizeof(uint64_t))
#define DATA_META_KEY_SIZE_DIFF (sizeof(uint64_t))

static inline int
key_is_same_of_key(char *key1, char *key2)
{
	return !strcmp(KEY_PATH(key1), KEY_PATH(key2));
}

static inline struct ftfs_inode *FTFS_I(struct inode *inode)
{
	return container_of(inode, struct ftfs_inode, vfs_inode);
}

struct ftfs_metadata {
	struct stat st;
};

static inline void dbt_init(DBT *dbt)
{
	memset(dbt, 0, sizeof(*dbt));
}

static inline void dbt_destroy(DBT *dbt)
{
	if (dbt->data)
		sb_free(dbt->data);
	dbt_init(dbt);
}

static inline void dbt_copy(DBT *dst, DBT *src)
{
	dst->data = src->data;
	dst->size = src->size;
	dst->ulen = src->ulen;
	dst->flags = src->flags;
}

static inline void dbt_setup(DBT *dbt, const void *data, size_t size)
{
	dbt->data = (void *)data;
	dbt->size = size;
	dbt->ulen = size;
	dbt->flags = DB_DBT_USERMEM;
}

static inline void dbt_setup_buf(DBT *dbt, const void *data, size_t size)
{
	dbt->data = (void *)data;
	dbt->size = 0;
	dbt->ulen = size;
	dbt->flags = DB_DBT_USERMEM;
}

static inline int dbt_alloc(DBT *dbt, size_t size)
{
	dbt->data = sb_malloc(size);
	if (dbt->data == NULL)
		return -ENOMEM;
	dbt->size = 0;
	dbt->ulen = size;
	dbt->flags = DB_DBT_USERMEM;

	return 0;
}

void dbt_copy_data_from_meta(DBT *data_dbt, DBT *meta_dbt, uint64_t block_num);

int dbt_alloc_data_from_meta(DBT *data_dbt, DBT *meta_dbt, uint64_t block_num);

int dbt_alloc_child_meta_from_meta(DBT *dbt, DBT *parent_dbt, const char *name);

int dbt_alloc_prefix_from_meta(DBT *prefix_dbt, DBT *meta_dbt);

int dbt_alloc_meta_movdir(DBT *old_prefix_dbt, DBT *new_prefix_dbt,
                          DBT *old_dbt, DBT *new_dbt);

struct ftio_vec {
	struct page *fv_page;
};
#define FTIO_MAX_INLINE 128
struct ftio {
	unsigned short ft_max_vecs; /* maximum number of vecs */
	unsigned short ft_vcnt; /* how many ft_vecs populated */
	unsigned short ft_bvidx; /* current index into fi_vec */
	unsigned short last_read_cnt; /* number of pages read last range query */
	unsigned long  last_offset; /* offset of last read */
	struct ftio_vec ft_io_vec[];
};

static inline struct ftio_vec *ftio_current_vec(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	return ftio->ft_io_vec + ftio->ft_bvidx;
}

static inline struct page *ftio_current_page(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	return (ftio->ft_io_vec + ftio->ft_bvidx)->fv_page;
}

static inline struct page *ftio_last_page(struct ftio *ftio)
{
	return (ftio->ft_io_vec + (ftio->ft_vcnt - 1))->fv_page;
}

static inline struct page *ftio_first_page(struct ftio *ftio)
{
	return (ftio->ft_io_vec)->fv_page;
}

static inline void ftio_advance_page(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	ftio->ft_bvidx++;
}

static inline int ftio_job_done(struct ftio *ftio)
{
	return (ftio->ft_bvidx >= ftio->ft_vcnt);
}

static inline struct ftio *ftio_alloc(int nr_iovecs)
{
	struct ftio *ftio;

	ftio = sb_malloc(sizeof(*ftio) + nr_iovecs * sizeof(struct ftio_vec));
	if (ftio) {
		ftio->ft_max_vecs = nr_iovecs;
		ftio->ft_vcnt = 0;
		ftio->ft_bvidx = 0;
	}

	return ftio;
}

static inline void ftio_free(struct ftio *ftio)
{
	sb_free(ftio);
}

static int nb_page_cmp(void *priv, struct list_head *a, struct list_head *b)
{
	struct page *a_pg = container_of(a, struct page, lru);
	struct page *b_pg = container_of(b, struct page, lru);

	if (a_pg->index < b_pg->index)
		return 1;
	else if (a_pg->index > b_pg->index)
		return -1;
	return 0;
}

static inline void ftio_add_page(struct ftio *ftio, struct page *page)
{
	struct ftio_vec *fv;
	BUG_ON(ftio->ft_vcnt >= ftio->ft_max_vecs);
	fv = ftio->ft_io_vec + ftio->ft_vcnt++;
	fv->fv_page = page;
}

static inline void ftio_setup(struct ftio *ftio, struct list_head *pages,
                              int nr_pages, struct address_space *mapping)
{
	unsigned page_idx;

	list_sort(NULL, pages, nb_page_cmp);

	for (page_idx = 0; page_idx < nr_pages; page_idx++) {
		struct page *page = list_entry(pages->prev, struct page, lru);
		prefetchw(&page->flags);
		list_del(&page->lru);
		if (!add_to_page_cache_lru(page, mapping, page->index, GFP_KERNEL))
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

#define nb_bstore_txn_begin(env, parent, txn, flags)	\
	env->txn_begin(env, parent, txn, flags)
#define nb_bstore_txn_commit(txn, flags)		\
	txn->commit(txn, flags)
#define nb_bstore_txn_abort(txn)			\
	txn->abort(txn)

#define DBOP_JUMP_ON_CONFLICT(err, label)				\
	if (((err)==DB_LOCK_DEADLOCK)||					\
		((err)==DB_LOCK_NOTGRANTED)||				\
		((err)==TOKUDB_INTERRUPTED)||				\
		((err)==TOKUDB_TRY_AGAIN)||				\
		((err)==TOKUDB_CANCELED)) {				\
		nb_bstore_txn_abort(txn);				\
		goto label;						\
	}								\

#define nb_bstore_flush_log(env)	env->log_flush(env, 0)
#define nb_bstore_checkpoint(env)	env->txn_checkpoint(env, 0, 0, 0)

int nb_bstore_get_ino(DB *meta_db, DB_TXN *txn, ino_t *ino);
/* Functions in nb_sbtore.c */
int nb_bstore_update_ino(DB *meta_db, DB_TXN *txn, ino_t ino);

//int ftfs_bstore_data_hot_flush(DB *data_db, struct ftfs_meta_key *meta_key,
//                               uint64_t start, uint64_t end);

int nb_bstore_env_open(struct ftfs_sb_info *sbi);
int nb_bstore_env_close(struct ftfs_sb_info *sbi);

int nb_bstore_meta_get(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
		       struct ftfs_metadata *metadata);
int nb_bstore_meta_put(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
		       struct ftfs_metadata *metadata);
int nb_bstore_meta_del(DB *meta_db, DBT *meta_dbt, DB_TXN *txn);
int nb_bstore_meta_readdir(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
			   struct dir_context *ctx);

int nb_bstore_get(DB *data_db, DBT *data_dbt, DB_TXN *txn, void *buf);
int nb_bstore_put(DB *data_db, DBT *data_dbt, DB_TXN *txn,
		  const void *buf, size_t len, int is_seq);
int nb_bstore_update(DB *data_db, DBT *data_dbt, DB_TXN *txn,
		     const void *buf, size_t size, loff_t offset);
// truncate a file in data_db (we dont do extend-like falloc in our truncate),
// preserve offset bytes in block new_num, (offset == 0) means delete that block
int nb_bstore_trunc(DB *data_db, DBT *meta_dbt, DB_TXN *txn,
		    uint64_t new_num, uint64_t offset);
// For deleting whole directories in rmdir
int dir_delete(DBT *meta_dbt, DB *data_db, DB_TXN *txn);

int nb_bstore_scan_one_page(DB *data_db, DBT *meta_dbt, DB_TXN *txn,
                              struct page *page);
int nb_bstore_scan_pages(DB *data_db, DBT *meta_dbt, DB_TXN *txn,
                           struct ftio *ftio);

int nb_dir_is_empty(DB *meta_db, DBT *meta_dbt, DB_TXN *txn, int *ret);


#define FTFS_BSTORE_MOVE_LARGE_THRESHOLD (1 << 22)

int nb_bstore_file_clone_copy(DB *data_db,
                                DBT *old_meta_dbt, DBT *new_meta_dbt,
                                DB_TXN *txn, bool delete_src);
int nb_bstore_file_clone(DB *data_db,
                           DBT *src_key, DBT *dst_key, DB_TXN *txn,
                           bool delete_src);
int nb_bstore_dir_clone(DB *meta_db, DB *data_db,
                          DBT *src_key, DBT *dst_key, DB_TXN *txn,
                          bool delete_src);

// XXX: these 3 functions shouldn't be used any more if we want to mount
// multiple ftfs. They are kept here for southbound proc filesystems.
// Once southbound discards them, they should be removed
void nb_print_engine_status(void);
int bstore_checkpoint(void);
int bstore_hot_flush_all(void);
int nb_bstore_dump_node(bool is_data, int64_t b);

#endif /* FTFS_FS_H */
