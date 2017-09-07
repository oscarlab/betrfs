/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef FTFS_FS_H
#define FTFS_FS_H

#include <linux/list.h>
#include <linux/fs.h>
#include <linux/rwsem.h>
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
		ftfs_error(__func__, "%d dbop err=%d", __LINE__, (err));\
		ftfs_bstore_txn_abort(txn);				\
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

#define FTFS_BSTORE_BLOCKSIZE_BITS	12
#define FTFS_BSTORE_BLOCKSIZE		\
		(1 << (FTFS_BSTORE_BLOCKSIZE_BITS))
#define block_get_num_by_position(pos)		\
		((pos) >> (FTFS_BSTORE_BLOCKSIZE_BITS))
#define block_get_offset_by_position(pos)	\
		((pos) & ((FTFS_BSTORE_BLOCKSIZE) - 1))

#define ftfs_get_block_num_by_size(size)	\
		((((size) - 1) >> (FTFS_BSTORE_BLOCKSIZE_BITS)) + 1)

#define FTFS_SUPER_MAGIC 0XF3560

#define TIME_T_TO_TIMESPEC(ts, t) do \
		{ \
			ts.tv_sec = t; \
			ts.tv_nsec = 0; \
		} while (0)
#define TIMESPEC_TO_TIME_T(t, ts) t = ts.tv_sec;

#define FTFS_UINT64_MAX (0xFFFFFFFFFFFFFFFFULL)

#define META_KEY_MAGIC 0
#define DATA_KEY_MAGIC 1

#define IS_META_KEY_DBT(dbt_p)	(*((uint8_t *)dbt_p->data) == META_KEY_MAGIC)
#define IS_DATA_KEY_DBT(dbt_p)	(*((uint8_t *)dbt_p->data) == DATA_KEY_MAGIC)

#define FTFS_ROOT_INO 1

#define LARGE_IO_THRESHOLD 256

#define FTFS_MAX_CIRCLE_SIZE		10000
#define FTFS_INO_INC			1000

struct ftfs_info {
	ino_t next_ino, max_ino;
};

// FTFS superblock info
struct ftfs_sb_info {
	// DB info
	DB_ENV *db_env;
	DB *data_db;
	DB *meta_db;
	unsigned long long max_file_size;
	uint32_t max_circle_size; // circle size limit
	struct ftfs_info __percpu *s_ftfs_info;
	unsigned s_nr_cpus;
};

struct ftfs_inode {
	struct inode vfs_inode;
	struct rw_semaphore key_lock;
	struct ftfs_meta_key *key;
	struct list_head rename_locked;
	int nr_meta, nr_data;
};

struct ftfs_meta_key {
	uint8_t magic;
	uint64_t circle_id;
	char path[];
};

struct ftfs_data_key {
	uint8_t magic;
	uint64_t circle_id;
	uint64_t block_num;
	char path[];
};

#define SIZEOF_KEY(key) (sizeof(key) + strlen((key).path) + 1)
#define DBT_PATH_LEN(dbt_size, key_type) (dbt_size - sizeof(key_type) - 1)

#define META_KEY_MAX_LEN (sizeof(struct ftfs_meta_key) + PATH_MAX + 1)
#define DATA_KEY_MAX_LEN (sizeof(struct ftfs_data_key) + PATH_MAX + 1)
#define KEY_MAX_LEN (META_KEY_MAX_LEN > DATA_KEY_MAX_LEN ? \
                     META_KEY_MAX_LEN : DATA_KEY_MAX_LEN)

static inline void
copy_data_key_from_meta_key(struct ftfs_data_key *data_key,
                            const struct ftfs_meta_key *meta_key,
                            uint64_t block_num)
{
	data_key->magic = DATA_KEY_MAGIC;
	data_key->circle_id = meta_key->circle_id;
	data_key->block_num = block_num;
	strcpy(data_key->path, meta_key->path);
}

static inline struct ftfs_data_key *
alloc_data_key_from_meta_key(const struct ftfs_meta_key *meta_key,
                             uint64_t block_num)
{
	struct ftfs_data_key *data_key;

	data_key = kmalloc(sizeof(struct ftfs_data_key) + strlen(meta_key->path) + 1,
	                   GFP_KERNEL);
	if (data_key == NULL)
		return NULL;

	copy_data_key_from_meta_key(data_key, meta_key, block_num);

	return data_key;
}

static inline void
copy_child_meta_key_from_meta_key(struct ftfs_meta_key *child_meta_key,
                                  const struct ftfs_meta_key *meta_key,
                                  const char *child_name)
{
	child_meta_key->magic = META_KEY_MAGIC;
	child_meta_key->circle_id = meta_key->circle_id;
	snprintf(child_meta_key->path,
	         strlen(meta_key->path) + strlen(child_name) + 2,
	         "%s/%s", meta_key->path, child_name);
}

static inline struct ftfs_meta_key *
alloc_child_meta_key_from_meta_key(const struct ftfs_meta_key *meta_key,
                                   const char *child_name)
{
	struct ftfs_meta_key *child_meta_key;

	child_meta_key = kmalloc(SIZEOF_KEY(*meta_key) + strlen(child_name) + 1,
	                         GFP_KERNEL);
	if (!child_meta_key)
		return NULL;

	copy_child_meta_key_from_meta_key(child_meta_key, meta_key, child_name);

	return child_meta_key;
}

static inline struct ftfs_meta_key *alloc_meta_key_from_circle_id(uint64_t circle_id)
{
	struct ftfs_meta_key *meta_key;

	meta_key = kmalloc(sizeof(struct ftfs_meta_key) + 1, GFP_KERNEL);
	if (!meta_key)
		return NULL;
	meta_key->magic = META_KEY_MAGIC;
	meta_key->circle_id = circle_id;
	*meta_key->path = '\0';

	return meta_key;
}

static inline uint64_t meta_key_get_circle_id(const struct ftfs_meta_key *key)
{
	return key->circle_id;
}

static inline uint64_t data_key_get_circle_id(const struct ftfs_data_key *key)
{
	return key->circle_id;
}

static inline const char *meta_key_get_path(const struct ftfs_meta_key *key)
{
	return key->path;
}

static inline const char *data_key_get_path(const struct ftfs_data_key *key)
{
	return key->path;
}

static inline uint64_t data_key_get_block_num(const struct ftfs_data_key *key)
{
	return key->block_num;
}

static inline void
data_key_set_block_num(struct ftfs_data_key *key, uint64_t block_num)
{
	key->block_num = block_num;
}

static inline void meta_key_free(struct ftfs_meta_key *meta_key)
{
	kfree(meta_key);
}

static inline void data_key_free(struct ftfs_data_key *data_key)
{
	kfree(data_key);
}

static inline int meta_key_is_circle_root(struct ftfs_meta_key *meta_key)
{
	return (*meta_key->path == '\0');
}

static inline struct ftfs_inode *FTFS_I(struct inode *inode)
{
	return container_of(inode, struct ftfs_inode, vfs_inode);
}

enum ftfs_metadata_type {
	FTFS_METADATA_TYPE_NORMAL,
	FTFS_METADATA_TYPE_REDIRECT
};

typedef struct ftfs_metadata {
	enum ftfs_metadata_type type;
	int nr_meta, nr_data;
	union {
		struct stat st;
		uint64_t circle_id;
	} u;
} ftfs_metadata_t;

struct ftio_vec {
	struct page *fv_page;
#if (FTFS_BSTORE_BLOCKSIZE != PAGE_CACHE_SIZE)
	unsigned int fv_offset;
#endif
};
#define FTIO_MAX_INLINE 128
struct ftio {
	unsigned short ft_max_vecs; /* maximum number of vecs */
	unsigned short ft_vcnt; /* how many ft_vecs populated */
	unsigned short ft_bvidx; /* current index into fi_vec */
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
static inline void ftio_advance_page(struct ftio *ftio)
{
	BUG_ON(ftio->ft_bvidx >= ftio->ft_vcnt);
	ftio->ft_bvidx++;
}
static inline int ftio_job_done(struct ftio *ftio)
{
	return (ftio->ft_bvidx >= ftio->ft_vcnt);
}

#ifdef TXN_ON
#define ftfs_bstore_txn_begin(env, parent, txn, flags)	\
		env->txn_begin(env, parent, txn, flags)
#define ftfs_bstore_txn_commit(txn, flags)		\
		txn->commit(txn, flags)
#define ftfs_bstore_txn_abort(txn)			\
		txn->abort(txn)
#else
#define ftfs_bstore_txn_begin(env, parent, txn, flags)	0
#define ftfs_bstore_txn_commit(txn, flags)		0
#define ftfs_bstore_txn_abort(txn)			0
#endif

#define ftfs_bstore_flush_log(env)	env->log_flush(env, 0)
#define ftfs_bstore_checkpoint(env)	env->txn_checkpoint(env, 0, 0, 0)

int ftfs_bstore_get_ino(DB *meta_db, DB_TXN *txn, ino_t *ino);
int ftfs_bstore_update_ino(struct ftfs_sb_info *sbi, ino_t ino);

int ftfs_bstore_data_hot_flush(DB *data_db, struct ftfs_meta_key *meta_key,
                               uint64_t start, uint64_t end);

int ftfs_bstore_env_open(struct ftfs_sb_info *sbi);
int ftfs_bstore_env_close(struct ftfs_sb_info *sbi);

int ftfs_bstore_meta_get(DB *meta_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn, struct ftfs_metadata *metadata);
int ftfs_bstore_meta_put(DB *meta_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn, struct ftfs_metadata *metadata);
int ftfs_bstore_meta_del(DB *meta_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn);
int ftfs_bstore_meta_readdir(DB *meta_db, struct ftfs_meta_key *meta_key,
                             DB_TXN *txn, struct dir_context *ctx);

int ftfs_bstore_get(DB *data_db, struct ftfs_data_key *data_key,
                    DB_TXN *txn, void *buf);
int ftfs_bstore_put(DB *data_db, struct ftfs_data_key *data_key,
                    DB_TXN *txn, const void *buf, size_t len, int is_seq);
int ftfs_bstore_update(DB *data_db, struct ftfs_data_key *data_key,
                       DB_TXN *txn,
                       const void *buf, size_t size, loff_t offset);
int ftfs_bstore_truncate(DB *data_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn, uint64_t min_num, uint64_t max_num);
int ftfs_bstore_scan_one_page(DB *data_db, struct ftfs_meta_key *meta_key,
                              DB_TXN *txn, struct page *page);
int ftfs_bstore_scan_pages(DB *data_db, struct ftfs_meta_key *meta_key,
                           DB_TXN *txn, struct ftio *ftio);

int ftfs_directory_is_empty(DB *meta_db, struct ftfs_meta_key *meta_key,
                            DB_TXN *txn, int *ret);

#define FTFS_BSTORE_MOVE_NO_DATA	0x1
int ftfs_bstore_move(DB *meta_db, DB *data_db,
                     struct ftfs_meta_key *old_meta_key,
                     struct ftfs_meta_key *new_meta_key,
                     DB_TXN *txn, int is_dir, struct ftfs_metadata *old_meta);

// XXX: these 3 functions shouldn't be used any more if we want to mount
// multiple ftfs. They are kept here for southbound proc filesystems.
// Once southbound discards them, they should be removed
void ftfs_print_engine_status(void);
int bstore_checkpoint(void);
int bstore_hot_flush_all(void);

#endif /* FTFS_FS_H */
