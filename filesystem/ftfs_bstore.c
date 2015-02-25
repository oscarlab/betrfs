#include <linux/kernel.h>
#include <linux/types.h>
#include <linux/errno.h>
#include <linux/string.h>
#include <linux/slab.h>
#include <linux/list_sort.h>
#include <asm/uaccess.h>
#include "ftfs_fs.h"

size_t db_cachesize;

#define DATA_DB_NAME "ftfs_data"
#define META_DB_NAME "ftfs_meta"
static DB_ENV *db_env;
static DB *data_db, *meta_db;

int bstore_flush_log(void)
{
	return db_env->log_flush(db_env, 0);
}

int bstore_checkpoint(void)
{
	if (unlikely(!db_env))
		return -EINVAL;
	return db_env->txn_checkpoint(db_env, 0, 0, 0);
}

int bstore_txn_commit(DB_TXN *txn, int flags)
{
#ifdef TXN_ON
	return txn->commit(txn, flags);
#else
	return 0;
#endif
}

int bstore_txn_abort(DB_TXN *txn)
{
#ifdef TXN_ON
	return txn->abort(txn);
#else
	return 0;
#endif

}

int bstore_txn_begin(DB_TXN *parent, DB_TXN **txn, int flags)
{
#ifdef TXN_ON
	return db_env->txn_begin(db_env, parent, txn, flags);
#else
	return 0;
#endif
}

/*
 * initialize a DBT with the given data pointer and size
 */
static void dbt_init(DBT *dbt, const void *data, size_t size)
{
	memset(dbt, 0, sizeof(DBT));
	dbt->data = (void *)data;
	dbt->size = size;
	dbt->ulen = size;
	dbt->flags = DB_DBT_USERMEM;
}

/*
 * Copy the buffers referred to by one dbt into another.
 */
static void dbt_copy_allocate(DBT *a, DBT *b)
{
	memset(b, 0, sizeof(DBT));
	b->data = kmalloc(a->size, GFP_KERNEL);
	memcpy(b->data, a->data, a->size);
	b->size = a->size;
	b->ulen = a->ulen;
	b->flags = a->flags;
}

/*
 * count number of slashes in a string
 */
static inline unsigned count_slash(const char *str)
{
	unsigned count = 0;
	for (; *str != '\0'; str++) {
		if (*str == '/') {
			count++;
		}
	}
	return count;
}

/*
 * Generate Meta key, last byte of meta key would be NULL
 */
static void generate_meta_key_dbt(DBT *key_dbt, char *key_buf,
	size_t key_len, const char *name)
{
	unsigned slash_count = cpu_to_le32(count_slash(name));
	size_t key_name_prefix_len = key_len - sizeof(unsigned) - 1;
	memcpy(key_buf, &slash_count, sizeof(unsigned));
	memcpy(key_buf + sizeof(unsigned), name, key_name_prefix_len);
	key_buf[key_len - 1] = 0;
	dbt_init(key_dbt, key_buf, key_len);
}

/*
 * generate data key, name + uint64_t block_num + DATA_DB_KEY_MAGIC
 */
static void generate_data_key_dbt(DBT *key_dbt, char *key_buf,
	size_t key_buf_len, const char *name, uint64_t block_num)
{
	uint64_t k = cpu_to_le64(block_num);

	size_t key_name_prefix_len = key_buf_len - sizeof(uint64_t) - 1;
	memcpy(key_buf, name, key_name_prefix_len);
	memcpy(key_buf + key_name_prefix_len, &k, sizeof(uint64_t));
	key_buf[key_buf_len - 1] = DATA_DB_KEY_MAGIC;
	dbt_init(key_dbt, key_buf, key_buf_len);
}

/* (unsigned slash_count) + str (including last \0) + 0 (magic) */
#define META_KEY_BUF_LEN(str) (strlen(str) + sizeof(unsigned) + 2)
/* str (including last \0) + (uint64_t block_num) + MAGIC (magic) */
#define DATA_KEY_BUF_LEN(str) (strlen(str) + sizeof(uint64_t) + 2)

/*
 * alloc buffer for generating
 */
int alloc_and_gen_meta_key_dbt(DBT *dbt, const char *name)
{
	char *key_buf;
	size_t key_buf_len;

	key_buf_len = META_KEY_BUF_LEN(name);
	key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!key_buf)
		return -ENOMEM;
	generate_meta_key_dbt(dbt, key_buf, key_buf_len, name);
	return 0;
}

char *get_path_from_meta_key_dbt(DBT *dbt)
{
	return ((char *)dbt->data) + sizeof(unsigned);
}

static unsigned get_meta_key_slash_count(DBT const *key)
{
	return le32_to_cpu(*((unsigned *)key->data));
}

static uint64_t get_data_key_block_num(DBT const *key)
{
	uint64_t *ptr = key->data + key->size - sizeof(uint64_t) - 1;
	return le64_to_cpu(*ptr);
}

/*
 * Key comparison function in database
 * meta keys are sorted by num of slashes and then memcpy
 * data keys are sorted by memcpy directly
 */
static int env_keycmp(DB *DB, DBT const *a, DBT const *b)
{
	unsigned char *k1 = a->data;
	unsigned char *k2 = b->data;
	int v1, v2, comparelen;
	unsigned num_slashes_k1, num_slashes_k2;

	/* check the last bytes of the keys to see whether its meta
	 * or data key */
	int magic = *(k1 + a->size - 1);
	if (magic == DATA_DB_KEY_MAGIC) {
		comparelen = min(a->size, b->size) - sizeof(uint64_t) - 1;
		goto just_memcmp;
	}
	/* for meta db, we need to check num of slashs first */
	num_slashes_k1 = get_meta_key_slash_count(a);
	num_slashes_k2 = get_meta_key_slash_count(b);
	if (num_slashes_k1 > num_slashes_k2) {
		return 1;
	} else if (num_slashes_k1 < num_slashes_k2) {
		return -1;
	}
	k1 += sizeof(unsigned);
	k2 += sizeof(unsigned);
	comparelen = min(a->size, b->size) - sizeof(unsigned);
just_memcmp:
#undef UNROLL
#define UNROLL 8
#define CMP_BYTE(i) v1 = k1[i]; v2 = k2[i]; if (v1 != v2) return v1 - v2;
	for (; comparelen > UNROLL; 
			k1 += UNROLL, k2 += UNROLL, comparelen -= UNROLL) {
		if (*(uint64_t *)k1 == *(uint64_t *)k2)
			continue;
		CMP_BYTE(0);
		CMP_BYTE(1);
		CMP_BYTE(2);
		CMP_BYTE(3);
		CMP_BYTE(4);
		CMP_BYTE(5);
		CMP_BYTE(6);
		CMP_BYTE(7);
	}
	for (; comparelen > 0; k1++, k2++, comparelen--) {
		CMP_BYTE(0);
	}
#undef UNROLL
#undef CMP_BYTE
	if (a->size == b->size && magic == DATA_DB_KEY_MAGIC) {
		/*return le64_to_cpu(*((uint64_t *)k1))
		       - le64_to_cpu(*((uint64_t *)k2));
		 * easier for understanding and maintainance
		 */
		return get_data_key_block_num(a)
		       - get_data_key_block_num(b);
	}
	return (a->size - b->size);
}

/*
 * Info passed to the block update callback. It says that
 * size bytes should be applied to the block, starting at
 * offset, with the given buf.
 */
struct block_update_cb_info {
	size_t size;
	uint64_t offset;
	char buf[];
};

/*
 * Update a block by copying bytes from the update op's buf
 * into the new buff starting at offset, for size bytes.
 */
static int block_update_cb(const DBT *oldval, DBT *newval, void *extra)
{
	struct block_update_cb_info *info = extra;

	BUG_ON(extra == NULL);

#ifdef FTFS_SHORT_BLOCK
	if (info->offset + info->size > newval->size) {
		newval->size = info->offset + info->size;
		newval->data = kmalloc(newval->size, GFP_KERNEL);
	}
#else /* FTFS_SHORT_BLOCK */
	if (newval->size == 0) {
		newval->size = FTFS_BSTORE_BLOCKSIZE;
		newval->data = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	}
	BUG_ON(newval->size != FTFS_BSTORE_BLOCKSIZE);
#endif /* FTFS _SHORT_BLOCK */
	if (oldval != NULL) {
		memcpy(newval->data, oldval->data, oldval->size);
#ifdef FTFS_SHORT_BLOCK
		if (newval->size > oldval->size)
			memset(newval->data + oldval->size, 0,
				newval->size - oldval->size);
#endif
	} else {
		memset(newval->data, 0, newval->size);
	}
	memcpy(newval->data + info->offset, info->buf, info->size);

	return 0;
}

/*
 * The update callback comes back with a key and old value.
 * The extra DBT has a update op pointer in its data.
 *
 * If the callback passes a null in old_val, that means there was
 * no old value for the given key and we must create one.
 *
 * Use the callback in the update op strcture to update the old value
 * into a new value, then set the new value and free up the bytes
 * allocated by the callback.
 */
static int env_update_cb(DB *db, const DBT *key,
	const DBT *old_val, const DBT *extra,
	void (*set_val)(const DBT *new_val, void *set_extra),
	void *set_extra)
{
	int ret;
	DBT val, newval;
	size_t newval_buf_size;
	char *newval_buf;

	BUG_ON(db == NULL || key == NULL);
	BUG_ON(extra == NULL || extra->data == NULL);

	BUG_ON(((char *)key->data)[key->size - 1] != DATA_DB_KEY_MAGIC);

	if (old_val != NULL) {
		newval_buf_size = old_val->size;
		newval_buf = kmalloc(newval_buf_size, GFP_KERNEL);
	} else {
		/* delay allocation of block_update_cb */
		newval_buf_size = 0;
		newval_buf = NULL;
	}
	newval.data = newval_buf;
	newval.size = newval_buf_size;
	ret = block_update_cb(old_val, &newval, extra->data);

	if (ret == 0) {
		dbt_init(&val, newval.data, newval.size);
		set_val(&val, set_extra);
		if (newval.data != newval_buf) {
			kfree(newval.data);
		}
	}

	kfree(newval_buf);

	return ret;
}

void ftfs_print_engine_status() {
	uint64_t nrows;
	int bufsiz;
	char * buff;

	if (!db_env) {
		ftfs_error(__func__, "no db_env");
		return;
	}

	db_env->get_engine_status_num_rows(db_env, &nrows);
	bufsiz = nrows * 128;   // assume 128 characters per row
	buff = (char *) kmalloc(sizeof(char)*bufsiz, GFP_KERNEL);
	db_env->get_engine_status_text(db_env, buff, bufsiz);
	kfree(buff);
}

/*
 * Set up DB environment.
 */
int ftfs_bstore_env_open(void)
{
	int ret;
	uint32_t db_env_flags = 0;
	uint32_t gigab, bytes;
	int db_flags;
	DB_TXN *txn = NULL;

	/* open db_env */
	BUG_ON(db_env != NULL);
	ret = db_env_create(&db_env, 0);
	BUG_ON(ret != 0);

	gigab = db_cachesize / (1L << 30);
	bytes = db_cachesize % (1L << 30);

	BUG_ON(gigab <= 0 && bytes <= 0);
	ret = db_env->set_cachesize(db_env, gigab, bytes, 1);
	BUG_ON(ret != 0);

	db_env->set_update(db_env, env_update_cb);
	ret = db_env->set_default_bt_compare(db_env, env_keycmp);
	BUG_ON(ret != 0);

#ifdef TXN_ON
	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
		DB_INIT_LOCK | DB_RECOVER | DB_INIT_LOG | DB_INIT_TXN;
#else
	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
		DB_INIT_LOCK ;
#endif
	ret = db_env->open(db_env, "/db", db_env_flags, 0755);
	BUG_ON(ret != 0);

	ret = bstore_txn_begin(NULL, &txn, 0);
	BUG_ON(ret !=0);

	/* open data db */
	db_flags = DB_CREATE | DB_THREAD;
	BUG_ON(data_db != NULL);
	ret = db_create(&data_db, db_env, 0);
	BUG_ON(ret != 0);
	// db_set_read_params(data_db);
	ret = data_db->open(data_db, txn, DATA_DB_NAME, NULL,
			DB_BTREE, db_flags, 0644);
	BUG_ON(ret != 0);

	/* open meta db */
	BUG_ON(meta_db != NULL);
	ret = db_create(&meta_db, db_env, 0);
	BUG_ON(ret != 0);
	// db_set_read_params(meta_db);
	ret = meta_db->open(meta_db, NULL, META_DB_NAME, NULL,
			DB_BTREE, db_flags, 0644);
	BUG_ON(ret != 0);

	ret = bstore_txn_commit(txn, DB_TXN_SYNC);
	BUG_ON(ret != 0);

	/* set the cleaning and checkpointing thread periods */
	db_env_flags = 60;
	db_env->checkpointing_set_period(db_env, db_env_flags);
	db_env_flags = 1;
	db_env->cleaner_set_period(db_env, db_env_flags);
	db_env_flags = 1000;
	db_env->change_fsync_log_period(db_env, db_env_flags);

	return 0;
}

/*
 * Close DB environment
 */
int ftfs_bstore_env_close(void)
{
	int ret;

	ret = bstore_flush_log();
	BUG_ON(ret);

	BUG_ON(data_db == NULL);
	ret = data_db->close(data_db, 0);
	BUG_ON(ret != 0);
	data_db = NULL;

	BUG_ON(meta_db == NULL);
	ret = meta_db->close(meta_db, 0);
	BUG_ON(ret != 0);
	meta_db = NULL;

	BUG_ON(db_env == NULL);
	ret = db_env->close(db_env, 0);
	BUG_ON(ret != 0);
	db_env = NULL;

	return 0;
}

/*
 * Get the metadata block for a given bstore by name
 */
int ftfs_bstore_meta_get(const char *name, DB_TXN *txn, void *buf, size_t size)
{
	int ret;
	DBT key, value;

	size_t key_buf_len = META_KEY_BUF_LEN(name);
	char *key_buf = kmalloc(key_buf_len, GFP_KERNEL);

	if (key_buf == NULL)
		return FTFS_BSTORE_NOMEM;

	generate_meta_key_dbt(&key, key_buf, key_buf_len, name);
	dbt_init(&value, buf, size);
	ret = meta_db->get(meta_db, txn, &key, &value, DB_GET_FLAGS);
	if (ret == DB_NOTFOUND)
		ret = FTFS_BSTORE_NOTFOUND;

	kfree(key_buf);

	return ret;
}

int
ftfs_bstore_meta_put(DBT *meta_key, DB_TXN *txn, struct ftfs_metadata *meta)
{
	int ret;
	DBT value;

	dbt_init(&value, meta, FTFS_METADATA_SIZE);
	ret = meta_db->put(meta_db, txn, meta_key, &value, DB_PUT_FLAGS);

	return ret;
}

int
ftfs_bstore_meta_delete(DBT *meta_key, DB_TXN *txn)
{
	return meta_db->del(meta_db, txn, meta_key, DB_DEL_FLAGS);
}

static int is_directly_under_dir(const char *filename,
	const char *dirname)
{
	int same_prefix =
		(strncmp(filename, dirname, strlen(dirname)) == 0);
	return same_prefix &&
		count_slash(filename) == count_slash(dirname);
}

static int directory_is_empty_meta_scan(const char *name,
	void *meta, void *extra)
{
	struct directory_is_empty_info *info = extra;

	/* We are supposed to scan with a starting key
	 * that has a slash after a real directory name,
	 * which shouldn't directly exist, but rather
	 * position us on the next thing alphabetically
	 * with root as an exception
	 */
	if (strcmp(info->dirname, "/") != 0)
		BUG_ON(strcmp(name, info->dirname) == 0);

	info->saw_child = is_directly_under_dir(name, info->dirname);

	return 0;
}

int directory_is_empty(DBT *meta_key, DB_TXN *txn, int *is_empty)
{
	int ret;
	struct directory_is_empty_info info;
	char *path = get_path_from_meta_key_dbt(meta_key);
	size_t len = strlen(path) + 2;
	char *path_with_slash;

	path_with_slash = kmalloc(len, GFP_KERNEL);
	if (path_with_slash == NULL)
		return -ENOMEM;

	strcpy(path_with_slash, path);
	if (strcmp(path, "/") != 0) {
		path_with_slash[len - 2] = '/';
		path_with_slash[len - 1] = '\0';
	}
	info.dirname = path_with_slash;
	info.saw_child = 0;
	ret = ftfs_bstore_meta_scan(path_with_slash, txn,
		directory_is_empty_meta_scan, &info);
	kfree(path_with_slash);
	*is_empty = !info.saw_child;
	return ret == FTFS_BSTORE_NOTFOUND ? 0 : ret;
}

struct meta_scan_cb_info {
	bstore_meta_scan_callback_fn cb;
	void *extra;
	int do_continue;
};

static int meta_scan_cb(DBT const *key, DBT const *val, void *extra)
{
	int ret;
	struct meta_scan_cb_info *info = extra;

	info->do_continue = 0;
	ret = info->cb(((char *)key->data) + sizeof(unsigned),
	               val->data, info->extra);
	if (ret == BSTORE_SCAN_CONTINUE) {
		ret = 0;
		info->do_continue = 1;
	} else if (ret == BSTORE_SCAN_NEXT_TIME)
		ret = 0;
	return ret;
}

/*
 * scan the metadata, starting with the metadata whose
 * key is greater than or equal to the given name.
 * (usually it's path_with_slashs, so return next entry in meta_db)
 * works like bstore_scan otherwise.
 * XXX: wkj correct cursor flags for proper isolation
 */
int ftfs_bstore_meta_scan(const char *name, DB_TXN *txn,
	bstore_meta_scan_callback_fn cb, void *extra)
{
	int r, ret;
	DBT key;
	DBC *cursor;
	struct meta_scan_cb_info info;
	size_t key_buf_len = META_KEY_BUF_LEN(name);
	char *key_buf = kmalloc(key_buf_len, GFP_KERNEL);

	if (!key_buf)
		return FTFS_BSTORE_NOMEM;

	generate_meta_key_dbt(&key, key_buf, key_buf_len, name);

	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto free_out;

	info.cb = cb;
	info.extra = extra;
	info.do_continue = 0;
	ret = cursor->c_getf_set_range(cursor, 0, &key, meta_scan_cb, &info);
	while (ret == 0 && info.do_continue) {
		/* to enable bulk fetch (basement nodes cache for readahead),
		   cursor callback should return TOKUDB_CURSOR_CONTINUE */
		ret = cursor->c_getf_next(cursor, 0, meta_scan_cb, &info);
		 /* not sure how to handle cursor errors in this
		  * loop... we can expect a DB_NOTFOUND, and that is
		  * ok. what about other errors? */
	}

	if(ret == DB_NOTFOUND)
		ret = FTFS_BSTORE_NOTFOUND;
	r = cursor->c_close(cursor);
	BUG_ON(r != 0);

free_out:
	kfree(key_buf);
	return ret;
}

/**
 * Get a block from the store, writing its contents into buf, which
 * needs to be at least BSTORE_BLOCK_SIZE bytes.
 *
 * If the block requested was not previously initialized by an update
 * or put, get will return BSTORE_UNITIALIZED_GET, and buf is unchanged
 */
int ftfs_bstore_get(DBT *meta_key, DB_TXN *txn, uint64_t block_num, void *buf)
{
	int ret;
	DBT key, value;
	char *path = get_path_from_meta_key_dbt(meta_key);
	size_t key_buf_len = DATA_KEY_BUF_LEN(path);
	char *key_buf = kmalloc(key_buf_len, GFP_KERNEL);

	if (key_buf == NULL)
		return FTFS_BSTORE_NOMEM;
	generate_data_key_dbt(&key, key_buf, key_buf_len, path, block_num);
	dbt_init(&value, buf, FTFS_BSTORE_BLOCKSIZE);
	ret = data_db->get(data_db, txn, &key, &value, DB_GET_FLAGS);
	if (ret == DB_NOTFOUND)
		ret = FTFS_BSTORE_NOTFOUND;

	kfree(key_buf);
	return ret;
}

/**
 * Put a block into the store, whose contents are the first
 * BSTORE_BLOCK_SIZE bytes from buf.
 */
int ftfs_bstore_put(DBT *meta_key, DB_TXN *txn, uint64_t block_num,
                    const void *buf)
{
	int ret;
	DBT key, value;
	char *path = get_path_from_meta_key_dbt(meta_key);
	size_t key_buf_len = DATA_KEY_BUF_LEN(path);
	char *key_buf;

	key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (key_buf == NULL)
		return FTFS_BSTORE_NOMEM;
	generate_data_key_dbt(&key, key_buf, key_buf_len, path, block_num);

	dbt_init(&value, buf, FTFS_BSTORE_BLOCKSIZE);

	ret = data_db->put(data_db, txn, &key, &value, DB_PUT_FLAGS);

	kfree(key_buf);
	return ret;
}

#define strprefix(st, pre) strncmp(st, pre, strlen(pre)) == 0

/*
 * test if two keys share the same prefix that bstore block
 * keys have. the name prefix is everything except the last
 * sizeof(uint64_t) bytes.
 */
static int keys_share_name_prefix(DBT const *a, DBT const *b)
{
	return (a->size == b->size) && strprefix(a->data, b->data);
}

/*
 * examine the given key an dmark should_delete
 * if the keys share a name prefix and the given
 * key's block id is greater than or equal to the
 * info's block id
 */
struct truncate_cursor_cb_info {
	DBT *key;
	uint64_t block_num;
	int should_delete;
};

static int truncate_cursor_cb(DBT const *key, DBT const *value, void *extra)
{
	struct truncate_cursor_cb_info *info = extra;

	info->should_delete = 0;
	if (keys_share_name_prefix(key, info->key) &&
		get_data_key_block_num(key) >= info->block_num) {

		/* if share the same prefix, copy things into key won't
		 * affect anything */
		memcpy(info->key->data, key->data, key->size);
		info->should_delete = 1;
	}

	return 0;
}

int bstore_hot_flush_all(void)
{
	int ret;
	uint64_t loops = 0;
	if (!data_db)
		return -EINVAL;

	ret = data_db->hot_optimize(data_db, NULL, NULL,
				NULL, NULL, &loops);

	ftfs_log(__func__, "%llu loops, returning %d", loops, ret);

	return ret;

}

int bstore_hot_flush(DBT *meta_key, uint64_t start, uint64_t end)
{
	int ret;
	DBT lkey, rkey;
	char *lbuf, *rbuf;
	uint64_t loops;
	char *path = get_path_from_meta_key_dbt(meta_key);
	size_t buf_len = DATA_KEY_BUF_LEN(path);
	rbuf = kmalloc(buf_len, GFP_KERNEL);
	if (!rbuf)
		return -ENOMEM;
	lbuf = kmalloc(buf_len, GFP_KERNEL);
	if (!lbuf) {
		kfree(rbuf);
		return -ENOMEM;
	}

	generate_data_key_dbt(&lkey, lbuf, buf_len, path, start);
	generate_data_key_dbt(&rkey, rbuf, buf_len, path, end);
	ret = data_db->hot_optimize(data_db, &lkey, &rkey,
				NULL, NULL, &loops);

	ftfs_log(__func__, "hot optimized(%s, %llu, %llu): %d\n",
		path, start, end, ret);
	kfree(rbuf);
	kfree(lbuf);
	return ret;
}

/*
 * truncate a bstore, deleting any blocks greater than or
 * equal to the given block number
 * XXX: wkj understand txn flags
 */
int ftfs_bstore_truncate(DBT *meta_key, DB_TXN *txn, uint64_t block_num)
{
	int ret;
	DBT key, prev_key;
	DBC *cursor;
	char *path = get_path_from_meta_key_dbt(meta_key);
	size_t key_buf_len = DATA_KEY_BUF_LEN(path);
	char *key_buf, *prev_key_buf;
	struct truncate_cursor_cb_info info;

	key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (key_buf == NULL)
		return -ENOMEM;
	prev_key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (prev_key_buf == NULL) {
		kfree(key_buf);
		return -ENOMEM;
	}
	generate_data_key_dbt(&key, key_buf, key_buf_len, path,
	                      block_num);
	generate_data_key_dbt(&prev_key, prev_key_buf, key_buf_len, path,
	                      block_num);
	info.key = &key;
	info.block_num = block_num;
	info.should_delete = 0;

	ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	ret = cursor->c_getf_set_range(cursor, 0, &key,
	                               truncate_cursor_cb, &info);
	if (ret)
		goto out;

	while(ret == 0 && info.should_delete) {
		/* afraid of breaking cursor, do del later */
		memcpy(prev_key_buf, key_buf, key_buf_len);
		ret = cursor->c_getf_next(cursor, 0,
			truncate_cursor_cb, &info);
		if (ret && ret != DB_NOTFOUND)
			goto out;

		/* wkj: why not set DB_WRITECURSOR and do cursor->del? */
		data_db->del(data_db, txn, &prev_key, 0);
		/* do we care about this return value?  if we do,
		   should we just gtfo on any nonzero? what about
		   DB_NOTFOUND? */
	}

	ret = cursor->c_close(cursor);

out:
	kfree(key_buf);
	kfree(prev_key_buf);
	return ret;
}

/* whether cb should get the current val or check if we should name it */
#define RENAME_PREFIX_CB_OP_GET 1
#define RENAME_PREFIX_CB_OP_CHECK 2

struct rename_prefix_cb_info {
	int op;
	union {
		struct {
			int should_rename;
			const char *oldprefix;
		} check;
		struct {
			DBT *key;
			DBT *value;
		} get;
	} u;
};

static int rename_prefix_cb(DBT const *key, DBT const *value, void *extra)
{
	struct rename_prefix_cb_info *info = extra;
	char *c;

	if (info->op == RENAME_PREFIX_CB_OP_GET) {
		dbt_copy_allocate((DBT *)key, info->u.get.key);
		dbt_copy_allocate((DBT *)value, info->u.get.value);
	} else {
		BUG_ON(info->op != RENAME_PREFIX_CB_OP_CHECK);
		info->u.check.should_rename = 0;
		c = key->data;
		if (c[key->size - 1] != DATA_DB_KEY_MAGIC)
			c += sizeof(unsigned);
		if (strprefix(c, info->u.check.oldprefix)) {
			c += strlen(info->u.check.oldprefix);
			if (*c == '\0' || *c == '/') {
				info->u.check.should_rename = 1;
			}
		}
	}

	return 0;
}

/* rename_cursor_current_prefix would store old key in del_info_key and
 * rename_delete would remove it from db later */
static int rename_cursor_current_prefix(DB *db, DBC *cursor, DB_TXN *txn,
					const char *oldprefix,
					const char *newprefix,
					int slash_count_diff,
					DBT *del_info_key)
{
	int ret;
	DBT value;
	DBT newkey;
	struct rename_prefix_cb_info info;
	size_t oldprefix_len, newprefix_len, newkey_size;
	char *newkey_buf;

	del_info_key->data = NULL;
	value.data = NULL;
	info.u.get.key = del_info_key;
	info.u.get.value = &value;
	info.op = RENAME_PREFIX_CB_OP_GET;
	ret = cursor->c_getf_current(cursor, 0, rename_prefix_cb, &info);
	if (ret)
		goto out;
	oldprefix_len = strlen(oldprefix);
	newprefix_len = strlen(newprefix);

	newkey_size = info.u.get.key->size - oldprefix_len + newprefix_len;

	newkey_buf = kmalloc(newkey_size, GFP_KERNEL);
	if (ZERO_OR_NULL_PTR(newkey_buf)) {
		ret = -ENOMEM;
		goto out;
	}

	if (db == data_db) {
		memcpy(newkey_buf, newprefix, newprefix_len);
		memcpy(newkey_buf + newprefix_len,
		       info.u.get.key->data + oldprefix_len,
		       info.u.get.key->size - oldprefix_len);
	} else {
		BUG_ON(db != meta_db);
		memcpy(newkey_buf + sizeof(unsigned),
		       newprefix, newprefix_len);
		memcpy(newkey_buf + sizeof(unsigned) + newprefix_len,
		       info.u.get.key->data + sizeof(unsigned) + oldprefix_len,
		       info.u.get.key->size - oldprefix_len - sizeof(unsigned));
		*((unsigned *)newkey_buf) =
			cpu_to_le32(*((unsigned *)info.u.get.key->data)
			            + slash_count_diff);
	}
	dbt_init(&newkey, newkey_buf, newkey_size);

	ret = db->put(db, txn, &newkey, &value, DB_PUT_FLAGS);

	kfree(newkey_buf);
out:
	if (value.data)
		kfree(value.data);
	return ret;
}

/*
 * This function must be called after previous (rename_cursor_current_prefix)
 * function in order to deletion old entry in database and clean things up in
 * del_info_key. May be we can do it in a better way...
 */
static inline int rename_delete(DB *db, DB_TXN *txn, DBT *del_info_key)
{
	int ret = db->del(db, txn, del_info_key, DB_DEL_FLAGS);
	return ret;
}

static int rename_prefix(DB *db, DB_TXN *txn, const char *oldprefix,
	const char *newprefix)
{
	int ret, i, done, prealloc_i, prealloc_len;
	unsigned slash_count_diff = 0;
	size_t oldprefix_len, prefix_buf_len;
	char *prefix_buf;
	DBC *cursor;
	DBT key;
	struct rename_prefix_cb_info info;

	FTFS_DEBUG_ON(db != data_db && db != meta_db);

	oldprefix_len = strlen(oldprefix);
	if (db == data_db) {
		prealloc_i = 0;
		prealloc_len = DATA_KEY_BUF_LEN(oldprefix);
	} else {
		BUG_ON(db != meta_db);
		/* meta key needs more work */
		prealloc_i = 4;
		prealloc_len = oldprefix_len + sizeof(unsigned) + 2
		               + (prealloc_i << 1);
	}
	prefix_buf = kmalloc(prealloc_len, GFP_KERNEL);
	if (!prefix_buf)
		return -ENOMEM;

	ret = db->cursor(db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	if (db == data_db) {
		generate_data_key_dbt(&key, prefix_buf,
		                      prealloc_len, oldprefix, 0);
		prefix_buf_len = 0;
	} else {
		BUG_ON(db != meta_db);
		slash_count_diff = count_slash(oldprefix);
		*((unsigned *)prefix_buf) = cpu_to_le32(slash_count_diff);
		slash_count_diff = count_slash(newprefix) - slash_count_diff;
		strcpy(prefix_buf + sizeof(unsigned), oldprefix);
		prefix_buf_len = oldprefix_len + sizeof(unsigned) + 2;
		prefix_buf[prefix_buf_len - 2] = 0;
		prefix_buf[prefix_buf_len - 1] = 0;
		dbt_init(&key, prefix_buf, prefix_buf_len);
	}
	prefix_buf_len = prealloc_len - (prealloc_i << 1) ;
	for (i = 0, done = 0; !done; i++) {
		if (i > prealloc_i) {
			char *tmp = prefix_buf;;
			BUG_ON(db != meta_db);
			prealloc_i <<= 1;
			prealloc_len = oldprefix_len + sizeof(unsigned) + 2
			               + (prealloc_i << 1);
			prefix_buf = krealloc(prefix_buf, prealloc_len,
			                      GFP_KERNEL);
			if (ZERO_OR_NULL_PTR(prefix_buf)) {
				kfree(tmp);
				ret = -ENOMEM;
				goto close_cursor_out;
			}
		}

		if (i) {
			BUG_ON(db != meta_db);
			prefix_buf_len += 2;
			prefix_buf[prefix_buf_len - 4] = '/';
			prefix_buf[prefix_buf_len - 3] = 0x1;
			prefix_buf[prefix_buf_len - 2] = 0;
			prefix_buf[prefix_buf_len - 1] = 0;
			*((unsigned *)prefix_buf) =
				cpu_to_le32(
					le32_to_cpu(*((unsigned *)prefix_buf))
			                + 1);
			dbt_init(&key, prefix_buf, prefix_buf_len);
		}

		info.op = RENAME_PREFIX_CB_OP_CHECK;
		info.u.check.oldprefix = oldprefix;
		info.u.check.should_rename = 0;

		ret = cursor->c_getf_set_range(cursor, 0, &key,
					       rename_prefix_cb, &info);
		if (ret != 0 && ret != DB_NOTFOUND)
			goto free_close_cursor_out;

		if (!info.u.check.should_rename) {
			done = 1;
		} else {
			do {
				DBT del_info_key;
				ret = rename_cursor_current_prefix(db, cursor,
							txn,
							oldprefix,
							newprefix,
							slash_count_diff,
							&del_info_key);
				if (ret) {
					if (del_info_key.data)
						kfree(del_info_key.data);
					goto free_close_cursor_out;
				}

				info.u.check.should_rename = 0;
				ret = cursor->c_getf_next(cursor, 0,
							  rename_prefix_cb,
							  &info);
				if(ret != 0 && ret != DB_NOTFOUND) {
					if (del_info_key.data)
						kfree(del_info_key.data);
					goto free_close_cursor_out;
				}

				BUG_ON(!del_info_key.data);
				ret = rename_delete(db, txn, &del_info_key);
				kfree(del_info_key.data);
				if (ret)
					goto free_close_cursor_out;

			} while (info.u.check.should_rename);
		}

		if (db == data_db)
			done = 1;
	}

out:
	kfree(prefix_buf);
	return cursor->c_close(cursor);
	/*
	 * I am not sure if we can fall through; I think ret might be
	 * nonzero (DB_NOTFOUND)
	 */
free_close_cursor_out:
	kfree(prefix_buf);
close_cursor_out:
	i = cursor->c_close(cursor);
	if (i)
		return i;
	return ret;
}

/*
 * rename all bstores whos name matches the given prefix
 * by replacing the original prefix with the new one.
 */
int ftfs_bstore_rename_prefix(const char *oldprefix, const char *newprefix,
			DB_TXN *parent)
{
	int ret;
	DB_TXN *txn = NULL;
    TXN_GOTO_LABEL(retry)
	ret = bstore_txn_begin(parent, &txn, TXN_MAY_WRITE);
	BUG_ON(ret);

	ret = rename_prefix(data_db, txn, oldprefix, newprefix);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}

	ret = rename_prefix(meta_db, txn, oldprefix, newprefix);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		goto abort;
	}

	/*
	 * DB_TXN_SYNC -> ACID, DB_TXN_NOSYNC ACI (not durable)
	 * overrides flags set in txn_begin()
	 */
	ret = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	return ret;

abort:
	bstore_txn_abort(txn);
	return ret;
}

static unsigned char filetype_table[] = {
	DT_UNKNOWN, DT_FIFO, DT_CHR, DT_UNKNOWN,
	DT_DIR, DT_UNKNOWN, DT_BLK, DT_UNKNOWN,
	DT_REG, DT_UNKNOWN, DT_LNK, DT_UNKNOWN,
	DT_SOCK, DT_UNKNOWN, DT_WHT, DT_UNKNOWN
};

#define ftfs_get_type(mode) filetype_table[(mode >> 12) & 15]
/*
 * readdir callback
 */
int readdir_scan_cb(const char *name, void *meta, void *extra) {
	struct readdir_scan_cb_info *info = extra;
	struct ftfs_metadata *m = meta;
	int i;;

	/* seems a ugly way to fix root */
	if (info->root_first) {
		info->root_first = 0;
		return BSTORE_SCAN_CONTINUE;
	}
	BUG_ON(strcmp(name, info->dirname) == 0);

	if (info->skip > 0) {
		info->skip--;
		return BSTORE_SCAN_CONTINUE;
	}
	if (is_directly_under_dir(name, info->dirname)) {
		for (i = strlen(name); name[i] != '/'; i--) ;
		i++;
		if (dir_emit(info->ctx, name + i, strlen(name + i),
			     iunique(info->sb, FTFS_ROOT_INO),
			     ftfs_get_type(m->st.st_mode))) {
			info->ctx->pos++;
			return BSTORE_SCAN_CONTINUE;
		}
		/*
		 * caller may give readdir limited amount of memory
		 * so that readdir can't be finished in one run
		 * Tell meta_scan, we want to do this next time
		 */
		return BSTORE_SCAN_NEXT_TIME;
	}

	return 0;
}

int fill_sparse(struct scan_buf *sbuf, uint64_t end_block_num)
{
	uint64_t block_num = block_get_num_by_position(sbuf->offset);
	size_t block_offset = block_get_offset_by_position(sbuf->offset);

	/* inclusive */
	while (sbuf->len > sbuf->bytes_read && block_num <= end_block_num) {
		size_t read_size = min(sbuf->len - sbuf->bytes_read,
				FTFS_BSTORE_BLOCKSIZE - block_offset);
		BUG_ON(read_size <= 0);
		memset(sbuf->buf, 0, read_size);

		update_scan_buf(sbuf, read_size);

		block_num = block_get_num_by_position(sbuf->offset);
		block_offset = block_get_offset_by_position(sbuf->offset);
	}

	return 0;
}

int fill_sparse_ftio(struct ftio *ftio, uint64_t end_block_num)
{
	size_t size, pos;
	uint64_t block;
	struct bio_vec *bvec;
	char *page_buf;

	bvec = current_ftio_bvec(ftio);
	pos = page_offset(bvec->bv_page) + bvec->bv_offset;
	block = block_get_num_by_position(pos);

	while (ftio->ft_bvidx < ftio->ft_vcnt && block <= end_block_num) {
		size = min(bvec->bv_len - bvec->bv_offset,
			(unsigned int)FTFS_BSTORE_BLOCKSIZE);
		BUG_ON(size <= 0);

		page_buf = kmap(bvec->bv_page);
		memset(page_buf, 0, size);
		kunmap(bvec->bv_page);

		advance_ftio_bytes(ftio, size);

		/* now update our current page pointer */
		bvec = current_ftio_bvec(ftio);
		pos = page_offset(bvec->bv_page) + bvec->bv_offset;
		block = block_get_num_by_position(pos);
	}

	return 0;
}


static int block_scan_cb(DBT const *key, DBT const *val, void *extra)
{
	int ret = 0;
	uint64_t block;
	struct block_scan_cb_info *info = extra;

	info->do_continue = 0;

	block = block_get_num_by_position(info->scan_buf->offset);

	if (keys_share_name_prefix(info->key, key)) {
		uint64_t db_block = get_data_key_block_num(key);
		if (db_block <= info->prefetch_block_num) {

			/* at least one non-sparse block is in our
			 * target read range */

			for ( ; block < db_block; block++) {
#ifdef FTFS_SHORT_BLOCK
				ret = info->filler(block, NULL, 0, info->scan_buf);
#else
				ret = info->filler(block, NULL, info->scan_buf);
#endif
				if (ret != BSTORE_SCAN_CONTINUE)
					goto out;
			}

#ifdef FTFS_SHORT_BLOCK
			ret = info->filler(db_block, val->data, val->size, info->scan_buf);
#else
			ret = info->filler(db_block, val->data, info->scan_buf);
#endif
			goto out;
		}
	}

	/*
	 * We have encountered a sparse file and are either:
	 *   1) in the middle of a sparse file and our entire prefetch
	 *      range consists of sparse data (keys share a prefix but
	 *      db_block > prefetch_block_num)
	 *   2) at the end of a sparse file, and the returned key is
	 *      from the next file in our db.
	 * Either way, we want to zero out the rest of the read.
	 */

	for ( ; block <= info->prefetch_block_num; block++) {
#ifdef FTFS_SHORT_BLOCK
		ret = info->filler(block, NULL, 0, info->scan_buf); //ret check?
#else
		ret = info->filler(block, NULL, info->scan_buf);
#endif
		if (ret != BSTORE_SCAN_CONTINUE)
			return ret;
	}
out:
	if (ret == BSTORE_SCAN_CONTINUE) {
		info->do_continue = 1;
		/* cursor callbacks can enable bulk fetching
		 * (caching of basement nodes) if they return
		 * TOKUDB_CURSOR_CONTINUE */
		return TOKUDB_CURSOR_CONTINUE;
	}

	return ret;
}


static int page_scan_cb(DBT const *key, DBT const *val, void *extra)
{
	int ret = 0;
	struct scan_pages_info *info = extra;
	struct ftio *ftio = info->ftio;
	struct bio_vec *bvec = current_ftio_bvec(ftio);
	struct page *page = bvec->bv_page;
	size_t pos = page_offset(page) + bvec->bv_offset;
	uint64_t block = block_get_num_by_position(pos);

	info->do_continue = 0;

	if (keys_share_name_prefix(info->key, key)) {
		uint64_t db_block = get_data_key_block_num(key);

		if (db_block <= info->prefetch_block_num) {

			/* at least one non-sparse block is in our
			 * target read range */

			for ( ; block < db_block; block++) {
				ret = info->filler(block, NULL, 0, info);
				if (ret != BSTORE_SCAN_CONTINUE)
					goto out;
			}

			ret = info->filler(db_block, val->data, val->size,
					info);
			goto out;
		}
	}

	/*
	 * We have encountered a sparse file and are either:
	 *   1) in the middle of a sparse file and our entire prefetch
	 *      range consists of sparse data (keys share a prefix but
	 *      db_block > prefetch_block_num)
	 *   2) at the end of a sparse file, and the returned key is
	 *      from the next file in our db.
	 * Either way, we want to zero out the rest of the read.
	 */

	for ( ; block <= info->prefetch_block_num; block++) {
		ret = info->filler(block, NULL, 0, info); /* ret check? */
		if (ret != BSTORE_SCAN_CONTINUE)
			return ret;
	}
out:
	if (ret == BSTORE_SCAN_CONTINUE) {
		info->do_continue = 1;
		/* cursor callbacks can enable bulk fetching
		 * (caching of basement nodes) if they return
		 * TOKUDB_CURSOR_CONTINUE */
		return TOKUDB_CURSOR_CONTINUE;
	}

	return ret;
}


/**
 * @block_buf, @len are from the callback. we can copy at most len
 * bytes from block_buf into our page
 */
int bstore_fill_page(const uint64_t block_num, void *block_buf,
		      size_t len, void *extra)
{
	struct scan_pages_info *info = extra;
	struct ftio *ftio = info->ftio;
	struct bio_vec *bvec = current_ftio_bvec(ftio);
	struct page *page = bvec->bv_page;
	size_t pos = page_offset(page) + bvec->bv_offset;
	uint64_t target_block_num = block_get_num_by_position(pos);

	if (ftio->ft_vcnt > ftio->ft_bvidx && block_num == target_block_num) {
		char *page_buf;
		size_t block_offset;
		size_t read_size;
		size_t available_len;
		BUG_ON(target_block_num != block_get_num_by_position(pos));
		block_offset = block_get_offset_by_position(pos);

		read_size = min((size_t)(bvec->bv_len - bvec->bv_offset),
				FTFS_BSTORE_BLOCKSIZE - block_offset);
		BUG_ON(read_size <= 0);

		page_buf = kmap(page);
		if (!page)
			return -ENOMEM;

		if (block_buf) {
			available_len = len - block_offset;
			if (read_size > available_len) {
				memcpy(page_buf, block_buf + block_offset,
				       available_len);
				memset(page_buf + available_len, 0,
				       read_size - available_len);
			} else {
				memcpy(page_buf, block_buf + block_offset,
				       read_size);
			}
		} else {
			memset(page_buf, 0, read_size);
		}

		advance_ftio_bytes(ftio, read_size);
		kunmap(page);
	}

	return ftio->ft_vcnt > ftio->ft_bvidx ? BSTORE_SCAN_CONTINUE : 0;
}


/*
 * Scan a bstore's blocks starting at first block greater than or
 * equal to block_num using the given callback and extra params.
 * The scan will attempt to prefetch blocks until the given prefetch
 *
 * This needs a non-null txn due to cursor operations
 */
int ftfs_bstore_scan(DBT *meta_key, DB_TXN *txn,
                     uint64_t block_num, uint64_t prefetch_block_num,
                     filler_fn filler, struct scan_buf *sbuf)
{
	int r, ret;
	DBT key, prefetch_key;
	DBC *cursor;
	char *path = get_path_from_meta_key_dbt(meta_key);
	size_t key_buf_len = DATA_KEY_BUF_LEN(path);
	char *key_buf, *prefetch_key_buf;
	struct block_scan_cb_info info;

	key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!key_buf)
		return -ENOMEM;

	prefetch_key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!prefetch_key_buf) {
		ret = -ENOMEM;
		goto out;
	}
	generate_data_key_dbt(&key, key_buf, key_buf_len, path, block_num);
	generate_data_key_dbt(&prefetch_key, prefetch_key_buf, key_buf_len,
	                      path, prefetch_block_num);
	ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		return ret;

	ret = cursor->c_set_bounds(cursor, &key, &prefetch_key, true, 0);
	if (ret)
		goto close_cursor;

	info.filler = filler;
	info.key = &key;
	info.prefetch_block_num = prefetch_block_num;
	info.scan_buf = sbuf;
	info.do_continue = 0;

	ret = cursor->c_getf_set_range(cursor, 0, &key,
				block_scan_cb, &info);
	if (ret == DB_NOTFOUND) {
		ret = fill_sparse(sbuf, prefetch_block_num);
		goto close_cursor;
	}

	BUG_ON(ret != 0);

	while (ret == 0 && info.do_continue) {
		info.do_continue = 0;
		ret = cursor->c_getf_next(cursor, 0, block_scan_cb, &info);
		BUG_ON(ret != 0 && ret != DB_NOTFOUND);
	}
	ret = 0;
close_cursor:
	r = cursor->c_close(cursor);
	if (r)
		ret = r;
	kfree(prefetch_key_buf);
out:
	kfree(key_buf);
	return ret;
}


static void ftio_free(struct ftio *ftio)
{
	kfree(ftio);
}

static struct ftio *ftio_alloc(int nr_iovecs)
{
	struct ftio *ftio;
	struct bio_vec *bvl = NULL;

	if (nr_iovecs > FTIO_MAX_INLINE) {
		/* do stuff to allocate an array of bio_vecs, assign
		 * to bvl, set flag indicating we are responsible for
		 * freeing them ourselves. */
		ftfs_error(__func__, "have not yet coded for this many bios:"
			"%d", nr_iovecs);
		return NULL;
	} else {
		int allocsize = sizeof(*ftio) +
			nr_iovecs * sizeof(struct bio_vec);

		ftio = kmalloc(allocsize, GFP_KERNEL);
		if (unlikely(!ftio))
			return NULL;
		memset(ftio, 0, allocsize);
		bvl = ftio->ft_inline_vecs;
	}

	ftio->ft_max_vecs = nr_iovecs;
	ftio->ft_vcnt = 0;
	ftio->ft_bvidx = 0;
	ftio->ft_bio_vec = bvl;
	return ftio;
}

static int ftfs_page_cmp(void *priv, struct list_head *a, struct list_head *b)
{
	struct page *a_pg = container_of(a, struct page, lru);
	struct page *b_pg = container_of(b, struct page, lru);

	s64 diff = b_pg->index - a_pg->index;

	if (diff < 0)
		return -1;
	if (diff > 0)
		return 1;
	return 0;
}

/*
 * wkj: Needs to be changed if page size exceeds block size
 */
static void ftio_add_page(struct ftio *ftio, struct page *page)
{
	struct bio_vec *bv = ftio->ft_bio_vec + ftio->ft_vcnt++;
	bv->bv_page = page;
	bv->bv_len = PAGE_CACHE_SIZE;
	bv->bv_offset = 0;
}

/**
 * This is kind of gross, but it works as follows:
 *
 * we sort the list of pages (from readpages) and then add them all to
 * @mapping (not up-to-date) and @ftio (which we use to fill them in).
 *
 * when we leave this function, all the pages in @ftio will be locked
 *    (from add_to_page_cache_lru) and ready to be read by the cursor
 *    callback for c_getf_next
 *
 * the caller is responsible for both setting the correct page state
 * via page flags {uptodate/error} AND unlocking the pages.
 */
static int setup_ftio(struct ftio *ftio, struct list_head *pages, int nr_pages,
		struct address_space *mapping)
{
	unsigned page_idx;
	unsigned max_idx = min(nr_pages, ftio->ft_max_vecs - ftio->ft_vcnt);

	list_sort(NULL, pages, ftfs_page_cmp);

	for (page_idx = 0; page_idx < max_idx; page_idx++) {
		struct page *page = list_entry(pages->prev, struct page, lru);
		prefetchw(&page->flags);
		list_del(&page->lru);
		if (!add_to_page_cache_lru(page, mapping,
					page->index, GFP_KERNEL)) {
			ftio_add_page(ftio, page);
		}
		page_cache_release(page);
	}
	BUG_ON(!list_empty(pages));
	return 0;
}

int do_scan_pages(const char *path, DB_TXN *txn,
			struct ftio *ftio, struct address_space *mapping)
{
	int ret, r;
	loff_t offset;
	uint64_t first_block, last_block;
	DBT key, prefetch_key;
	DBC *cursor;
	size_t key_buf_len = strlen(path) + sizeof(uint64_t) + 2;
	char *key_buf, *prefetch_key_buf;
	struct scan_pages_info info;

	key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!key_buf)
		return -ENOMEM;

	prefetch_key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!prefetch_key_buf) {
		ret = -ENOMEM;
		goto out;
	}

	offset = page_offset(current_ftio_page(ftio));
	first_block = block_get_num_by_position(offset);
	offset = page_offset((ftio->ft_bio_vec + ftio->ft_vcnt - 1)->bv_page);
	last_block = block_get_num_by_position(offset);

	generate_data_key_dbt(&key, key_buf, key_buf_len,
			path, first_block);
	generate_data_key_dbt(&prefetch_key, prefetch_key_buf, key_buf_len,
			path, last_block);

	ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret) {
		ftfs_error(__func__, "data_db->cursor(): %d", ret);
		goto freekeys_out;
	}


	ret = cursor->c_set_bounds(cursor, &key, &prefetch_key, true, 0);
	if (ret)
		goto close_cursor;

	info.filler = bstore_fill_page;
	info.key = &key;
	info.prefetch_block_num = last_block;
	info.ftio = ftio;
	info.do_continue = 0;

	ret = cursor->c_getf_set_range(cursor, 0, &key,
				page_scan_cb, &info);
	if (ret == DB_NOTFOUND) {
		ret = fill_sparse_ftio(ftio, last_block);
		goto close_cursor;
	}

	BUG_ON(ret != 0);

	while (ret == 0 && info.do_continue) {
		info.do_continue = 0;
		ret = cursor->c_getf_next(cursor, 0, page_scan_cb, &info);
		BUG_ON(ret != 0 && ret != DB_NOTFOUND);
	}
	ret = 0;
close_cursor:
	r = cursor->c_close(cursor);
	if (r)
		ret = r;
freekeys_out:
	kfree(prefetch_key_buf);
out:
	kfree(key_buf);
	return ret;
#if 0
	unsigned page_idx;
	int ret;
	for (page_idx = 0; page_idx < nr_pages; page_idx++) {
		struct page *page = list_entry(pages->prev, struct page, lru);

		prefetchw(&page->flags);
		list_del(&page->lru);
		if (!add_to_page_cache_lru(page, mapping,
					page->index, GFP_KERNEL)) {
			/* pass remaining page count as a hint so they
			 * know when we are done calling them */
			offset = page_offset(page);
			fillsize = size - offset > PAGE_SIZE ?
				PAGE_SIZE : size - offset;
			buf = kmap(page);
			BUG_ON(!buf);
			ret = ftfs_bstore_scanpage(path, buf, fillsize, offset,
						nr_pages - page_idx);
			if (fillsize < PAGE_SIZE)
				memset(buf+fillsize, 0, PAGE_SIZE-fillsize);
			flush_dcache_page(page);
			SetPageUptodate(page);
		}
		page_cache_release(page);
	}
	BUG_ON(!list_empty(pages));
	return 0;
#endif
}

int ftfs_bstore_scan_pages(struct ftfs_dbt *meta_key,
			struct list_head *pages, unsigned nr_pages,
			struct address_space *mapping)
{
	int ret, r;
	struct ftio *ftio;
	char *path = get_path_from_meta_key_dbt(&meta_key->dbt);
	DB_TXN *txn;

	ftio = ftio_alloc(nr_pages);
	if (!ftio)
		return -ENOMEM;

	ret = setup_ftio(ftio, pages, nr_pages, mapping);
	if (ret) {
		ftfs_error(__func__, "setup_ftio: %d)", ret);
		goto ftio_cleanup;
	}
	/* wkj: above code is suspect. maybe break ftio into ranges? */

retry:
	bstore_txn_begin(NULL, &txn, TXN_READONLY);
	ret = do_scan_pages(path, txn, ftio, mapping);
	DBOP_JUMP_ON_CONFLICT(ret, retry);
	r = bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(r, retry);

ftio_cleanup:
	/* now we need to unlock all the pages and mark them
	 * up-to-date (or error) */
	if (ret)
		/* clear up-to-date, set error */
		set_ftio_pages_error(ftio);
	else
		/* set up-to-date */
		set_ftio_pages_uptodate(ftio);

	unlock_ftio_pages(ftio);

	ftio_free(ftio);

	return ret;
}
#ifdef FTFS_SHORT_BLOCK
int bstore_fill_block(uint64_t target_block_num, void *block_buf,
		      size_t len, void *extra)
{
	struct scan_buf *sbuf = extra;

	size_t block_offset;
	size_t read_size;
	size_t available_len;

	if (sbuf->len - sbuf->bytes_read > 0) {
		BUG_ON(target_block_num !=
			block_get_num_by_position(sbuf->offset));
		block_offset = block_get_offset_by_position(sbuf->offset);

		read_size = min(sbuf->len - sbuf->bytes_read,
				FTFS_BSTORE_BLOCKSIZE - block_offset);
		BUG_ON(read_size <= 0);

		if (block_buf) {
			available_len = len - block_offset;
			if (read_size > available_len) {
				memcpy(sbuf->buf, block_buf + block_offset,
				       available_len);
				memset(sbuf->buf + available_len, 0,
				       read_size - available_len);
			} else {
				memcpy(sbuf->buf, block_buf + block_offset,
				       read_size);
			}
		} else {
			memset(sbuf->buf, 0, read_size);
		}

		update_scan_buf(sbuf, read_size);
	}

	return (sbuf->len - sbuf->bytes_read) > 0 ? BSTORE_SCAN_CONTINUE : 0;
}
#else /* FTFS_SHORT_BLOCK */
int bstore_fill_block(uint64_t target_block_num, void *block_buf, void *extra)
{
	struct scan_buf *sbuf = extra;

	size_t block_offset;
	size_t read_size;

	if ((sbuf->len - sbuf->bytes_read) > 0) {
		BUG_ON(target_block_num !=
			block_get_num_by_position(sbuf->offset));
		block_offset = block_get_offset_by_position(sbuf->offset);

		read_size = min(sbuf->len - sbuf->bytes_read,
				FTFS_BSTORE_BLOCKSIZE - block_offset);
		BUG_ON(read_size <= 0);

		block_buf ?
			memcpy(sbuf->buf, block_buf + block_offset,
				read_size) :
			memset(sbuf->buf, 0, read_size);

		update_scan_buf(sbuf, read_size);
	}

	return (sbuf->len - sbuf->bytes_read) > 0 ? BSTORE_SCAN_CONTINUE : 0;
}
#endif /* FTFS_SHORT_BLOCK */

/*
 * Update a block in the bstore. THe update takes buf and copies size
 * bytes into the affected block, starting at the given offset. The
 * other bytes are unchanged.
 */
int ftfs_bstore_update(DBT *meta_key, DB_TXN *txn, uint64_t block_num,
                       const void *buf, size_t size, size_t offset)
{
	int ret;
	DBT key, extra_dbt;
	struct block_update_cb_info *info;
	char *info_buf, *key_buf;
	size_t key_buf_len;
	size_t info_size = sizeof(struct block_update_cb_info) + size;
	char *path = get_path_from_meta_key_dbt(meta_key);
	key_buf_len = DATA_KEY_BUF_LEN(path);
	key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!key_buf)
		return -ENOMEM;

	generate_data_key_dbt(&key, key_buf, key_buf_len, path, block_num);

	info_buf = kmalloc(info_size, GFP_KERNEL);
	if (!info_buf) {
		kfree(key_buf);
		return -ENOMEM;
	}

	info = (struct block_update_cb_info *) info_buf;
	info->offset = offset;
	info->size = size;
	memcpy(info->buf, buf, size);

	dbt_init(&extra_dbt, info, info_size);
	ret = data_db->update(data_db, txn, &key, &extra_dbt, DB_UPDATE_FLAGS);

	kfree(key_buf);
	kfree(info_buf);
	return ret;
}
