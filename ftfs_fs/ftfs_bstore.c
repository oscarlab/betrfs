#include <linux/kernel.h>
#include <linux/slab.h>

#include "ftfs_fs.h"

size_t db_cachesize;

#define DB_ENV_PATH "/db"
#define DATA_DB_NAME "ftfs_data"
#define META_DB_NAME "ftfs_meta"

// XXX: delete these 2 variables once southbound dependency is solved
static DB_ENV *XXX_db_env;
static DB *XXX_data_db;

struct ftfs_meta_key next_circle_id_meta_key = {
	META_KEY_MAGIC,
	0,
	"next_circle_id"
};
struct ftfs_meta_key next_ino_meta_key = {
	META_KEY_MAGIC,
	0,
	"next_ino"
};

/*
 * init DBT with given data pointer and size
 */
static inline void
dbt_init(DBT *dbt, const void *data, size_t size)
{
	dbt->data = (void *)data;
	dbt->size = size;
	dbt->ulen = size;
	dbt->flags = DB_DBT_USERMEM;
}

static inline void
dbt_init_extra_size(DBT *dbt, const void *data, size_t size, size_t total_size)
{
	dbt->data = (void *)data;
	dbt->size = size;
	dbt->ulen = total_size;
	dbt->flags = DB_DBT_USERMEM;
}

static inline void
copy_data_key(struct ftfs_data_key *dst, struct ftfs_data_key *src)
{
	BUG_ON(src->magic != DATA_KEY_MAGIC);
	dst->magic = DATA_KEY_MAGIC;
	dst->circle_id = src->circle_id;
	dst->block_num = src->block_num;
	strcpy(dst->path, src->path);
}

static inline void
copy_meta_key_move_to_directory(struct ftfs_meta_key *new_key,
                                struct ftfs_meta_key *source,
                                struct ftfs_meta_key *target,
                                struct ftfs_meta_key *entry)
{
	size_t source_len = strlen(source->path);
	size_t target_len = strlen(target->path);
	size_t entry_len = strlen(entry->path);
	size_t path_size = target_len + (entry_len - source_len) + 1;

	new_key->magic = META_KEY_MAGIC;
	new_key->circle_id = target->circle_id;
	snprintf(new_key->path, path_size, "%s%s", target->path, entry->path + source_len);
}

static inline void
copy_data_key_move_to_directory(struct ftfs_data_key *new_key,
                                struct ftfs_meta_key *source,
                                struct ftfs_meta_key *target,
                                struct ftfs_data_key *entry)
{
	size_t source_len = strlen(source->path);
	size_t target_len = strlen(target->path);
	size_t entry_len = strlen(entry->path);
	size_t path_size = target_len + (entry_len - source_len) + 1;

	new_key->magic = DATA_KEY_MAGIC;
	new_key->circle_id = target->circle_id;
	new_key->block_num = entry->block_num;
	snprintf(new_key->path, path_size, "%s%s", target->path, entry->path + source_len);
}

static inline void
copy_child_data_key_from_meta_key(struct ftfs_data_key *child_data_key,
                                  const struct ftfs_meta_key *meta_key,
                                  const char *child_name, uint64_t block_num)
{
	child_data_key->magic = DATA_KEY_MAGIC;
	child_data_key->circle_id = meta_key->circle_id;
	child_data_key->block_num = block_num;
	snprintf(child_data_key->path,
	         strlen(meta_key->path) + strlen(child_name) + 2,
	         "%s/%s", meta_key->path, child_name);
}

static inline int
meta_key_is_child_of_meta_key(struct ftfs_meta_key *child_meta_key,
                              struct ftfs_meta_key *meta_key)
{
	const char *child_path = meta_key_get_path(child_meta_key);
	const char *path = meta_key_get_path(meta_key);
	size_t len = strlen(path);

	return
	       meta_key_get_circle_id(child_meta_key) == meta_key_get_circle_id(meta_key) &&
	       !strncmp(child_path, path, len) && child_path[len] == '/' &&
	       strchr(child_path + len + 1, '/') == NULL;
}

static inline int
meta_key_is_descendant_of_meta_key(struct ftfs_meta_key *child_meta_key,
                                   struct ftfs_meta_key *meta_key)
{
	const char *child_path = meta_key_get_path(child_meta_key);
	const char *path = meta_key_get_path(meta_key);
	size_t len = strlen(path);

	return
	       meta_key_get_circle_id(child_meta_key) == meta_key_get_circle_id(meta_key) &&
	       !strncmp(child_path, path, len) && child_path[len] == '/';
}

static inline int
data_key_is_descendant_of_meta_key(struct ftfs_data_key *child_data_key,
                                   struct ftfs_meta_key *meta_key)
{
	const char *child_path = data_key_get_path(child_data_key);
	const char *path = meta_key_get_path(meta_key);
	size_t len = strlen(path);

	return
	       data_key_get_circle_id(child_data_key) == meta_key_get_circle_id(meta_key) &&
	       !strncmp(child_path, path, len) &&
	       (child_path[len] == '/' || child_path[len] == '\0');
}

static inline int
data_key_is_same_of_meta_key(struct ftfs_data_key *data_key,
                             struct ftfs_meta_key *meta_key)
{
	const char *data_path = data_key_get_path(data_key);
	const char *meta_path = meta_key_get_path(meta_key);

	return
	       data_key_get_circle_id(data_key) == meta_key_get_circle_id(meta_key) &&
	       !strcmp(data_path, meta_path);
}

int ftfs_bstore_get_ino(DB *meta_db, DB_TXN *txn, ino_t *ino)
{
	int ret;
	ino_t curr_ino;
	DBT next_ino_key_dbt, next_ino_value_dbt;

	dbt_init(&next_ino_key_dbt, &next_ino_meta_key,
	         SIZEOF_KEY(next_ino_meta_key));
	dbt_init(&next_ino_value_dbt, &curr_ino,
	         sizeof(curr_ino));

	ret = meta_db->get(meta_db, txn, &next_ino_key_dbt,
	                   &next_ino_value_dbt, DB_GET_FLAGS);
	if (ret == DB_NOTFOUND) {
		*ino = curr_ino = FTFS_ROOT_INO + 1;
		ret = meta_db->put(meta_db, txn, &next_ino_key_dbt,
		                   &next_ino_value_dbt, DB_PUT_FLAGS);
	} else if (!ret)
		*ino = curr_ino;

	return ret;
}

int ftfs_bstore_update_ino(struct ftfs_sb_info *sbi, ino_t ino)
{
	DB_TXN *txn;
	int ret;
	ino_t curr_ino;
	DBT next_ino_key_dbt, next_ino_value_dbt;

	dbt_init(&next_ino_key_dbt, &next_ino_meta_key,
	         SIZEOF_KEY(next_ino_meta_key));
	dbt_init(&next_ino_value_dbt, &curr_ino,
	         sizeof(curr_ino));

	TXN_GOTO_LABEL(retry); 
	{

		ftfs_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_MAY_WRITE);

		ret = sbi->meta_db->get(sbi->meta_db, txn, &next_ino_key_dbt,
	        	           &next_ino_value_dbt, DB_GET_FLAGS);

		if (!ret && ino > curr_ino) {
			curr_ino = ino;
			ret = sbi->meta_db->put(sbi->meta_db, txn, &next_ino_key_dbt,
		        	           &next_ino_value_dbt, DB_PUT_FLAGS);
		}
	} if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
	}

	ret = ftfs_bstore_txn_commit(txn, DB_TXN_NOSYNC);
	COMMIT_JUMP_ON_CONFLICT(ret, retry);

	return ret;
}

int ftfs_bstore_data_hot_flush(DB *data_db, struct ftfs_meta_key *meta_key,
                               uint64_t start, uint64_t end)
{
	int ret;
	DBT start_key, end_key;
	uint64_t loops;
	struct ftfs_data_key *start_data_key, *end_data_key;

	start_data_key = alloc_data_key_from_meta_key(meta_key, start);
	if (start_data_key == NULL)
		return -ENOMEM;
	end_data_key = alloc_data_key_from_meta_key(meta_key, end);
	if (end_data_key == NULL) {
		ret = -ENOMEM;
		goto out;
	}

	dbt_init(&start_key, start_data_key, SIZEOF_KEY(*start_data_key));
	dbt_init(&end_key, end_data_key, SIZEOF_KEY(*end_data_key));

	ret = data_db->hot_optimize(data_db, &start_key, &end_key,
	                            NULL, NULL, &loops);

	ftfs_log(__func__, "hot optimized(%llu, %s, %llu, %llu): %d\n",
	         meta_key_get_circle_id(meta_key),
	         meta_key_get_path(meta_key),
	         start, end, ret);

	data_key_free(end_data_key);
out:
	data_key_free(start_data_key);
	return ret;
}

/*
 * Key comparison function in database
 * Find the first place where the two keys differ
 *      Let a and b be the entries in which they differ
 * If both paths have a / at or after the differing point
 *     Sort by a and b
 * If both paths don't have a / at or after the differing point
 *     Sort by a and b
 * Otherwise, the path without a / at or after the differing point goes first
 */
static int path_cmp(const char *a, const char *b, unsigned int alen, unsigned int blen)
{
	unsigned int clen = alen < blen ? alen : blen;
	const uint64_t *a64, *b64;
	const char *pa, *pb, *a_next_slash, *b_next_slash;

	a64 = (uint64_t *)a; b64 = (uint64_t *)b;

	while (clen >= 8 && *a64 == *b64) {
		clen -= 8;
		a64++;
		b64++;
	}

	pa = (const char *)a64; pb = (const char *)b64;
	while (clen > 0 && *pa == *pb) {
		clen--;
		pa++;
		pb++;
	}

	if (clen == 0)
		return alen < blen ? -4 : (alen > blen) ? 4 : 0;

	a_next_slash = memchr(pa, '/', alen - (pa-a));
	b_next_slash = memchr(pb, '/', blen - (pb-b));

	if ((a_next_slash == NULL) == (b_next_slash == NULL)) {
		if (*pa == '/')
			return pa > a && *(pa-1) == '/' ? 5 : -1;
		else if (*pb == '/')
			return pb > b && *(pb-1) == '/' ? -5 : 1;
		return *pa < *pb ? -2 : 2;
	} else if (a_next_slash) {
		return 3;
	}
	return -3;
}

static inline int circle_id_cmp(uint64_t id_a, uint64_t id_b)
{
	return id_a < id_b ? -1 : (id_a > id_b) ? 1 : 0;
}

static inline int block_num_cmp(uint64_t bn_a, uint64_t bn_b)
{
	return bn_a < bn_b ? -1 : (bn_a > bn_b) ? 1 : 0;
}

/*
 * Key comparison function in database
 * meta keys are sorted by num of slashes and then memcpy
 * data keys are sorted by memcpy directly
 */
static int env_keycmp(DB *DB, DBT const *a, DBT const *b)
{
	if (IS_DATA_KEY_DBT(a)) {
		int r;
		struct ftfs_data_key *ka = (struct ftfs_data_key *)a->data;
		struct ftfs_data_key *kb = (struct ftfs_data_key *)b->data;

		BUG_ON(!IS_DATA_KEY_DBT(b));

		r = circle_id_cmp(data_key_get_circle_id(ka),
		                  data_key_get_circle_id(kb));
		if (r)
			return r;

		r = path_cmp(data_key_get_path(ka),
		             data_key_get_path(kb),
		             DBT_PATH_LEN(a->size, *ka),
		             DBT_PATH_LEN(b->size, *kb));

		if (r)
			return r;

		r = block_num_cmp(data_key_get_block_num(ka),
		                  data_key_get_block_num(kb));

		return r;
	} else {
		int r;
		struct ftfs_meta_key * ka = (struct ftfs_meta_key *)a->data;
		struct ftfs_meta_key * kb = (struct ftfs_meta_key *)b->data;

		BUG_ON(!IS_META_KEY_DBT(a));
		BUG_ON(!IS_META_KEY_DBT(b));

		r = circle_id_cmp(meta_key_get_circle_id(ka),
		                  meta_key_get_circle_id(kb));
		if (r)
			return r;

		r = path_cmp(meta_key_get_path(ka),
		             meta_key_get_path(kb),
		             DBT_PATH_LEN(a->size, *ka),
		             DBT_PATH_LEN(b->size, *kb));

		return r;
	}
}

/*
 * block update callback info
 * set value in [offset, offset + size) to buf
 */
struct block_update_cb_info {
	size_t size;
	loff_t offset;
	char buf[];
};

/*
 * block update callback
 * given old_val and extra (which is of struct block_update_cb_info
 * construct new_val and write to newval and newval_size
 */
static int
block_update_cb(void **newval, size_t *newval_size,
                const DBT *old_val, const void *extra)
{
	const struct block_update_cb_info *info = extra;

#ifdef FTFS_SHORT_BLOCK
	if (old_val)
		*newval_size = ((info->offset + info->size) > old_val->size) ?
		               (info->offset + info->size) : old_val->size;
	else
		*newval_size = info->offset + info->size;
#else
	*newval_size = FTFS_BSTORE_BLOCKSIZE;
#endif
	*newval = kmalloc(*newval_size, GFP_KERNEL);
	if ((*newval) == NULL)
		return -ENOMEM;

	if (old_val != NULL) {
		memcpy(*newval, old_val->data, old_val->size);
#ifdef FTFS_SHORT_BLOCK
		if (info->offset > old_val->size)
			memset(*newval + old_val->size, 0,
			       info->offset - old_val->size);
#endif
	} else {
		if (info->offset > 0)
			memset(*newval, 0, info->offset);
	}
	memcpy(*newval + info->offset, info->buf, info->size);

	return 0;
}

/*
 * Update callback function
 * db: the database
 * key: key dbt of the kv pair
 * old_val: old_val of the key dbt, if its null, we must create val
 * extra: the struct we pass to db->update function
 * set_val: set value function, should be provided by tokudb
 * set_extra: argument for set_val callback
 */
static int
env_update_cb(DB *db, const DBT *key, const DBT *old_val, const DBT *extra,
              void (*set_val)(const DBT *newval, void *set_extra),
              void *set_extra)
{
	int ret;
	DBT val;
	size_t newval_size;
	void *newval;

	BUG_ON(db == NULL || key == NULL || extra == NULL ||
	       extra->data == NULL);
	// there is no meta update currently
	BUG_ON(IS_META_KEY_DBT(key));

	ret = block_update_cb(&newval, &newval_size, old_val, extra->data);

	if (!ret) {
		dbt_init(&val, newval, newval_size);
		set_val(&val, set_extra);
		kfree(newval);
	}

	return ret;
}

/*
 * Set up DB environment.
 */
int ftfs_bstore_env_open(struct ftfs_sb_info *sbi)
{
	int r;
	uint32_t db_env_flags, db_flags;
	uint32_t giga_bytes, bytes;
	DB_TXN *txn = NULL;
	DB_ENV *db_env;

	BUG_ON(sbi->db_env);
	r = db_env_create(&sbi->db_env, 0);
	if (r != 0) {
	  if (r == TOKUDB_HUGE_PAGES_ENABLED)
	    printk(KERN_ERR "Failed to create the TokuDB environment because Transparent Huge Pages (THP) are enabled.  Please disable THP following the instructions at https://docs.mongodb.com/manual/tutorial/transparent-huge-pages/.  You may set the parameter to madvise or never. (errno %d)\n", r);
	  else
	    printk(KERN_ERR "Failed to create the TokuDB environment, errno %d\n", r);
	  return r;
	}

	db_env = sbi->db_env;

	giga_bytes = db_cachesize / (1L << 30);
	bytes = db_cachesize % (1L << 30);
	r = db_env->set_cachesize(db_env, giga_bytes, bytes, 1);
	BUG_ON(r);

	db_env->set_update(db_env, env_update_cb);

	r = db_env->set_default_bt_compare(db_env, env_keycmp);
	BUG_ON(r);

#ifdef TXN_ON
	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
	               DB_INIT_LOCK | DB_RECOVER | DB_INIT_LOG | DB_INIT_TXN;
#else
	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
	               DB_INIT_LOCK;
#endif

	r = db_env->open(db_env, DB_ENV_PATH, db_env_flags, 0755);
	if (r) {
		r = -ENOENT;
		goto failed;
	}

	r = ftfs_bstore_txn_begin(db_env, NULL, &txn, 0);
	BUG_ON(r);

	db_flags = DB_CREATE | DB_THREAD;
	BUG_ON(sbi->data_db);
	r = db_create(&sbi->data_db, db_env, 0);
	BUG_ON(r);
	r = sbi->data_db->open(sbi->data_db, txn, DATA_DB_NAME, NULL,
	                       DB_BTREE, db_flags, 0644);
	BUG_ON(r);

	BUG_ON(sbi->meta_db);
	r = db_create(&sbi->meta_db, db_env, 0);
	BUG_ON(r);
	r = sbi->meta_db->open(sbi->meta_db, txn, META_DB_NAME, NULL,
	                       DB_BTREE, db_flags, 0644);
	BUG_ON(r);

	r = ftfs_bstore_txn_commit(txn, DB_TXN_SYNC);
	BUG_ON(r);

	/* set the cleaning and checkpointing thread periods */
	db_env_flags = 60; /* 60 s */
	r = db_env->checkpointing_set_period(db_env, db_env_flags);
	BUG_ON(r);
	db_env_flags = 1; /* 1s */
	r = db_env->cleaner_set_period(db_env, db_env_flags);
	BUG_ON(r);
	db_env_flags = 1000; /* 1000 ms */
	db_env->change_fsync_log_period(db_env, db_env_flags);

	XXX_db_env = sbi->db_env;
	XXX_data_db = sbi->data_db;

	return 0;

failed:
	return r;
}

/*
 * Close DB environment
 */
int ftfs_bstore_env_close(struct ftfs_sb_info *sbi)
{
	int ret;

	ret = ftfs_bstore_flush_log(sbi->db_env);
	BUG_ON(ret);

	BUG_ON(sbi->data_db == NULL);
	ret = sbi->data_db->close(sbi->data_db, 0);
	BUG_ON(ret);
	sbi->data_db = NULL;

	BUG_ON(sbi->meta_db == NULL);
	ret = sbi->meta_db->close(sbi->meta_db, 0);
	BUG_ON(ret);
	sbi->meta_db = NULL;

	BUG_ON(sbi->db_env == NULL);
	ret = sbi->db_env->close(sbi->db_env, 0);
	BUG_ON(ret != 0);
	sbi->db_env = 0;

	XXX_db_env = NULL;
	XXX_data_db = NULL;

	return 0;
}

int ftfs_bstore_meta_get(DB *meta_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn, struct ftfs_metadata *metadata)
{
	int ret;
	DBT key, value;

	dbt_init(&key, meta_key, SIZEOF_KEY(*meta_key));
	dbt_init(&value, metadata, sizeof(struct ftfs_metadata));

	ret = meta_db->get(meta_db, txn, &key, &value, DB_GET_FLAGS);
	if (ret == DB_NOTFOUND)
		ret = -ENOENT;

	return ret;
}

int ftfs_bstore_meta_put(DB *meta_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn, struct ftfs_metadata *metadata)
{
	int ret;
	DBT key, value;

	dbt_init(&key, meta_key, SIZEOF_KEY(*meta_key));
	dbt_init(&value, metadata, sizeof(struct ftfs_metadata));

	ret = meta_db->put(meta_db, txn, &key, &value, DB_PUT_FLAGS);

	return ret;
}

int ftfs_bstore_meta_del(DB *meta_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn)
{
	DBT key;

	dbt_init(&key, meta_key, SIZEOF_KEY(*meta_key));
	return meta_db->del(meta_db, txn, &key, DB_DEL_FLAGS);
}

static unsigned char filetype_table[] = {
	DT_UNKNOWN, DT_FIFO, DT_CHR, DT_UNKNOWN,
	DT_DIR, DT_UNKNOWN, DT_BLK, DT_UNKNOWN,
	DT_REG, DT_UNKNOWN, DT_LNK, DT_UNKNOWN,
	DT_SOCK, DT_UNKNOWN, DT_WHT, DT_UNKNOWN
};

#define ftfs_get_type(mode) filetype_table[(mode >> 12) & 15]

int ftfs_bstore_meta_readdir(DB *meta_db, struct ftfs_meta_key *meta_key,
                             DB_TXN *txn, struct dir_context *ctx)
{
	int ret, r;
	struct ftfs_meta_key *child_meta_key;
	struct ftfs_metadata meta;
	DBT child_meta_key_dbt, meta_dbt;
	DBC *cursor;

	if (ctx->pos == 1)
		return 0;
	child_meta_key = kmalloc(META_KEY_MAX_LEN, GFP_KERNEL);
	if (!child_meta_key)
		return -ENOMEM;
	if (ctx->pos == 0) {
		ctx->pos = (loff_t)kmalloc(NAME_MAX, GFP_KERNEL);
		BUG_ON(ctx->pos == (loff_t)NULL);
		copy_child_meta_key_from_meta_key(child_meta_key, meta_key, "");
	} else {
		copy_child_meta_key_from_meta_key(child_meta_key, meta_key, (char *)ctx->pos);
	}
	dbt_init_extra_size(&child_meta_key_dbt, child_meta_key, SIZEOF_KEY(*child_meta_key), META_KEY_MAX_LEN);
	dbt_init(&meta_dbt, &meta, sizeof(struct ftfs_metadata));
	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	r = cursor->c_get(cursor, &child_meta_key_dbt, &meta_dbt, DB_SET_RANGE);
	while (!r) {
		char *name;
		u64 ino;
		unsigned type;

		if (!meta_key_is_child_of_meta_key(child_meta_key, meta_key)) {
			kfree((char *)ctx->pos);
			ctx->pos = 1;
			break;
		}
		if (meta.type == FTFS_METADATA_TYPE_REDIRECT) {
			struct ftfs_meta_key *readdir_circle_meta_key =
				alloc_meta_key_from_circle_id(meta.u.circle_id);
			if (!readdir_circle_meta_key) {
				r = -ENOMEM;
				break;
			}
			r = ftfs_bstore_meta_get(meta_db, readdir_circle_meta_key, txn, &meta);
			meta_key_free(readdir_circle_meta_key);
			if (r)
				break;
		}
		ino = meta.u.st.st_ino;
		type = ftfs_get_type(meta.u.st.st_mode);
		name = strrchr(child_meta_key->path, '/') + 1;
		if (!dir_emit(ctx, name, strlen(name), ino, type)) {
			strcpy((char *)ctx->pos, name);
			break;
		}

		r = cursor->c_get(cursor, &child_meta_key_dbt, &meta_dbt, DB_NEXT);
	}

	if (r == DB_NOTFOUND) {
		kfree((char *)ctx->pos);
		ctx->pos = 1;
	}

	cursor->c_close(cursor);

	if (r && r != DB_NOTFOUND)
		ret = r;

out:
	kfree(child_meta_key);

	return ret;
}

int ftfs_bstore_get(DB *data_db, struct ftfs_data_key *data_key,
                    DB_TXN *txn, void *buf)
{
	int ret;
	DBT key, value;

	dbt_init(&key, data_key, SIZEOF_KEY(*data_key));
	dbt_init(&value, buf, FTFS_BSTORE_BLOCKSIZE);

	ret = data_db->get(data_db, txn, &key, &value, DB_GET_FLAGS);
	if (!ret && value.size < FTFS_BSTORE_BLOCKSIZE)
		memset(buf + value.size, 0, FTFS_BSTORE_BLOCKSIZE - value.size);
	if (ret == DB_NOTFOUND)
		ret = -ENOENT;

	return ret;
}

#define FTFS_USE_SEQ_PUT 1

// size of buf must be FTFS_BLOCK_SIZE
int ftfs_bstore_put(DB *data_db, struct ftfs_data_key *data_key,
                    DB_TXN *txn, const void *buf, size_t len, int is_seq)
{
	int ret;
	DBT key, value;

	dbt_init(&key, data_key, SIZEOF_KEY(*data_key));
	dbt_init(&value, buf, len);

#if FTFS_USE_SEQ_PUT
	ret = is_seq ?
	      data_db->seq_put(data_db, txn, &key, &value, DB_PUT_FLAGS) :
	      data_db->put(data_db, txn, &key, &value, DB_PUT_FLAGS);
#else
	ret = data_db->put(data_db, txn, &key, &value, DB_PUT_FLAGS);
#endif
	return ret;
}

int ftfs_bstore_update(DB *data_db, struct ftfs_data_key *data_key,
                       DB_TXN *txn,
                       const void *buf, size_t size, loff_t offset)
{
	int ret;
	DBT key, extra_dbt;
	struct block_update_cb_info *info;
	size_t info_size = sizeof(struct block_update_cb_info) + size;

	info = kmalloc(info_size, GFP_KERNEL);
	if (!info)
		return -ENOMEM;
	info->offset = offset;
	info->size = size;
	memcpy(info->buf, buf, size);

	dbt_init(&key, data_key, SIZEOF_KEY(*data_key));
	dbt_init(&extra_dbt, info, info_size);

	ret = data_db->update(data_db, txn, &key, &extra_dbt, DB_UPDATE_FLAGS);

	kfree(info);
	return ret;
}

int ftfs_bstore_truncate(DB *data_db, struct ftfs_meta_key *meta_key,
                         DB_TXN *txn, uint64_t min_num, uint64_t max_num)
{
	int ret;
	struct ftfs_data_key *min_data_key, *max_data_key;
	DBT min_data_key_dbt, max_data_key_dbt;

	min_data_key = alloc_data_key_from_meta_key(meta_key, min_num);
	if (!min_data_key)
		return -ENOMEM;
	max_data_key = alloc_data_key_from_meta_key(meta_key, max_num);
	if (!max_data_key) {
		ret = -ENOMEM;
		goto out;
	}

	dbt_init(&min_data_key_dbt, min_data_key, SIZEOF_KEY(*min_data_key));
	dbt_init(&max_data_key_dbt, max_data_key, SIZEOF_KEY(*max_data_key));

	ret = data_db->del_multi(data_db, txn,
	                         &min_data_key_dbt,
	                         &max_data_key_dbt,
	                         0, 0);

	data_key_free(max_data_key);
out:
	data_key_free(min_data_key);
	return ret;
}

#if (FTFS_BSTORE_BLOCKSIZE == PAGE_CACHE_SIZE)
int ftfs_bstore_scan_one_page(DB *data_db, struct ftfs_meta_key *meta_key,
                              DB_TXN *txn, struct page *page)
{
	int ret;
	uint64_t block_num;
	struct ftfs_data_key *data_key;
	void *buf;

	block_num = page->index;
	data_key = alloc_data_key_from_meta_key(meta_key, block_num);
	if (!data_key)
		return -ENOMEM;
	buf = kmap(page);

	ret = ftfs_bstore_get(data_db, data_key, txn, buf);
	if (ret == -ENOENT) {
		memset(buf, 0, FTFS_BSTORE_BLOCKSIZE);
		ret = 0;
	}

	kunmap(page);
	data_key_free(data_key);

	return ret;
}
#else // FTFS_BSTORE_BLOCKSIZE == PAGE_CACHE_SIZE
int ftfs_bstore_scan_one_page(DB *data_db, struct ftfs_meta_key *meta_key,
                              DB_TXN *txn, struct page *page)
{
	int ret;
	uint64_t block_num;
	size_t block_offset;
	loff_t offset;
	size_t len;
	struct ftfs_data_key *data_key;
	void *buf, *value_buf;
	DBT key_dbt, value_dbt;
	DBC *cursor;

	offset = page_offset(page);
	block_num = block_get_num_by_position(offset);
	data_key = kmalloc(DATA_KEY_MAX_LEN, GFP_KERNEL);
	if (!data_key)
		return -ENOMEM;
	value_buf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	if (!value_buf) {
		ret = -ENOMEM;
		goto out1;
	}
	copy_data_key_from_meta_key(data_key, meta_key, block_num);
	dbt_init(&key_dbt, data_key, DATA_KEY_MAX_LEN);
	dbt_init(&value_dbt, value_buf, FTFS_BSTORE_BLOCKSIZE);
	len = PAGE_CACHE_SIZE;
	buf = kmap(page);
	ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out2;
	ret = cursor->c_get(cursor, &key_dbt, &value_dbt, DB_SET_RANGE);
	while (len > 0 && ret) {
		ssize_t read_size;
		block_num = block_get_num_by_position(offset);
		block_offset = block_get_offset_by_position(offset);
		read_size = min(len, (FTFS_BSTORE_BLOCKSIZE - block_offset));

		if (data_key->circle_id == meta_key->circle_id &&
		    !strcmp(data_key->path, meta_key->path) &&
		    data_key->block_num == block_num) {
			memcpy(buf, value_buf, read_size);
			ret = cursor->c_get(cursor, &key_dbt, &value_dbt, DB_NEXT);
		} else
			memset(buf, 0, read_size);

		buf += read_size;
		offset += read_size;
		len -= read_size;
	}

	cursor->c_close(cursor);
out2:
	kunmap(page);
out1:
	kfree(value_buf);
	data_key_free(data_key);
	return ret;
}
#endif //FTFS_BSTORE_BLOCKSIZE == PAGE_CACHE_SIZE

/*
 * given a block (block_num, buf, len), fill ftio
 */
#if (FTFS_BSTORE_BLOCKSIZE == PAGE_CACHE_SIZE)
struct ftfs_scan_pages_cb_info {
	struct ftfs_meta_key *meta_key;
	struct ftio *ftio;
	int do_continue;
};

static int ftfs_scan_pages_cb(DBT const *key, DBT const *val, void *extra)
{
	struct ftfs_data_key *data_key = key->data;
	struct ftfs_scan_pages_cb_info *info = extra;
	struct ftio *ftio = info->ftio;

	if (data_key_is_same_of_meta_key(data_key, info->meta_key)) {
		struct page *page = ftio_current_page(ftio);
		uint64_t page_block_num = page->index;
		uint64_t block_num = data_key_get_block_num(data_key);
		void *page_buf;

		while (page_block_num < block_num) {
			page_buf = kmap(page);
			memset(page_buf, 0, PAGE_CACHE_SIZE);
			kunmap(page);

			ftio_advance_page(ftio);
			if (ftio_job_done(ftio))
				break;
			page = ftio_current_page(ftio);
			page_block_num = page->index;
		}

		if (page_block_num == block_num) {
			page_buf = kmap(page);
			if (val->size)
				memcpy(page_buf, val->data, val->size);
			if (val->size < PAGE_CACHE_SIZE)
				memset(page_buf + val->size, 0,
				       PAGE_CACHE_SIZE - val->size);
			kunmap(page);
			ftio_advance_page(ftio);
		}

		info->do_continue = !ftio_job_done(ftio);
	} else
		info->do_continue = 0;

	return 0;
}

static inline void ftfs_bstore_fill_rest_page(struct ftio *ftio)
{
	struct page *page;
	void *page_buf;

	while (!ftio_job_done(ftio)) {
		page = ftio_current_page(ftio);
		page_buf = kmap(page);
		memset(page_buf, 0, PAGE_CACHE_SIZE);
		kunmap(page);
		ftio_advance_page(ftio);
	}
}
#endif //FTFS_BSTORE_BLOCKSIZE == PAGE_CACHE_SIZE

int ftfs_bstore_scan_pages(DB *data_db, struct ftfs_meta_key *meta_key,
                           DB_TXN *txn, struct ftio *ftio)
{
	int ret, r;
	uint64_t block_num;
	struct ftfs_scan_pages_cb_info info;
	struct ftfs_data_key *data_key, *prefetch_data_key;
	DBT key_dbt, prefetch_key_dbt;
	DBC *cursor;

	if (ftio_job_done(ftio))
		return 0;
	block_num = ftio_current_page(ftio)->index;
	data_key = alloc_data_key_from_meta_key(meta_key, block_num);
	if (!data_key)
		return -ENOMEM;
	block_num = ftio_last_page(ftio)->index;
	prefetch_data_key = alloc_data_key_from_meta_key(meta_key, block_num);
	if (!prefetch_data_key) {
		ret = -ENOMEM;
		goto out;
	}

	dbt_init(&key_dbt, data_key, SIZEOF_KEY(*data_key));
	dbt_init(&prefetch_key_dbt, prefetch_data_key, SIZEOF_KEY(*prefetch_data_key));

	ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto free_out;

	ret = cursor->c_set_bounds(cursor, &key_dbt, &prefetch_key_dbt, true, 0);
	if (ret)
		goto close_cursor;

	info.meta_key = meta_key;
	info.ftio = ftio;

	r = cursor->c_getf_set_range(cursor, 0, &key_dbt, ftfs_scan_pages_cb, &info);
	while (info.do_continue && !r)
		r = cursor->c_getf_next(cursor, 0, ftfs_scan_pages_cb, &info);
	if (r && r != DB_NOTFOUND)
		ret = r;
	if (!ret)
		ftfs_bstore_fill_rest_page(ftio);

close_cursor:
	r = cursor->c_close(cursor);
	BUG_ON(r);
free_out:
	data_key_free(prefetch_data_key);
out:
	data_key_free(data_key);

	return ret;
}

struct ftfs_die_cb_info {
	struct ftfs_meta_key *meta_key;
	int *is_empty;
};

static int ftfs_die_cb(DBT const *key, DBT const *val, void *extra)
{
	struct ftfs_die_cb_info *info = extra;
	struct ftfs_meta_key *current_meta_key = key->data;

	*(info->is_empty) = !meta_key_is_child_of_meta_key(current_meta_key,
	                                                   info->meta_key);

	return 0;
}

int ftfs_directory_is_empty(DB *meta_db, struct ftfs_meta_key *meta_key,
                            DB_TXN *txn, int *is_empty)
{
	int ret, r;
	struct ftfs_meta_key *start_meta_key;
	struct ftfs_die_cb_info info;
	DBT key;
	DBC *cursor;

	start_meta_key = alloc_child_meta_key_from_meta_key(meta_key, "");
	if (!start_meta_key)
		return -ENOMEM;

	dbt_init(&key, start_meta_key, SIZEOF_KEY(*start_meta_key));

	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	info.meta_key = meta_key;
	info.is_empty = is_empty;
	ret = cursor->c_getf_set_range(cursor, 0, &key, ftfs_die_cb, &info);
	if (ret == DB_NOTFOUND) {
		ret = 0;
		*is_empty = 1;
	}

	r = cursor->c_close(cursor);
	BUG_ON(r);
out:
	meta_key_free(start_meta_key);

	return ret;
}

int ftfs_bstore_move(DB *meta_db, DB *data_db,
                     struct ftfs_meta_key *old_meta_key,
                     struct ftfs_meta_key *new_meta_key,
                     DB_TXN *txn, int flag, struct ftfs_metadata *old_meta)
{
	int ret, r, rot, is_dir;
	size_t key_buf_len;
	void *key_buf[2], *new_key_buf, *block_buf;
	struct ftfs_metadata meta;
	DBT old_meta_key_dbt, new_meta_key_dbt;
	DBT key_dbt[2], new_key_dbt, value_dbt;
	DBC *cursor = NULL;

	key_buf[0] = key_buf[1] = new_key_buf = block_buf = NULL;

	dbt_init(&old_meta_key_dbt, old_meta_key, SIZEOF_KEY(*old_meta_key));
	dbt_init(&new_meta_key_dbt, new_meta_key, SIZEOF_KEY(*new_meta_key));
	dbt_init(&value_dbt, old_meta, sizeof(struct ftfs_metadata));

	if ((ret = meta_db->put(meta_db, txn, &new_meta_key_dbt, &value_dbt, DB_PUT_FLAGS)) ||
	    (ret = meta_db->del(meta_db, txn, &old_meta_key_dbt, DB_DEL_FLAGS))) {
		ret = -EINVAL;
		goto out;
	}

	if ((flag & FTFS_BSTORE_MOVE_NO_DATA))
		goto out;

	ret = -ENOMEM;
	key_buf_len = KEY_MAX_LEN;
	key_buf[0] = kmalloc(key_buf_len, GFP_KERNEL);
	if (!key_buf[0])
		goto out;
	key_buf[1] = kmalloc(key_buf_len, GFP_KERNEL);
	if (!key_buf[1])
		goto out;
	new_key_buf = kmalloc(key_buf_len, GFP_KERNEL);
	if (!new_key_buf)
		goto out;
	block_buf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	if (!block_buf)
		goto out;

	is_dir = S_ISDIR(old_meta->u.st.st_mode);
	if (is_dir) {
		struct ftfs_meta_key *it_meta_key[2];
		struct ftfs_meta_key *new_it_meta_key = new_key_buf;

		it_meta_key[0] = key_buf[0];
		it_meta_key[1] = key_buf[1];
		rot = 0;
		copy_child_meta_key_from_meta_key(it_meta_key[rot], old_meta_key, "");
		dbt_init_extra_size(&key_dbt[rot], it_meta_key[rot], SIZEOF_KEY(*it_meta_key[rot]), key_buf_len);
		dbt_init(&key_dbt[1 - rot], it_meta_key[1 - rot], key_buf_len);
		dbt_init(&value_dbt, &meta, sizeof(struct ftfs_metadata));

		ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
		if (ret)
			goto out;
		r = cursor->c_get(cursor, &key_dbt[rot], &value_dbt, DB_SET_RANGE);
		while (!r) {
			if (!meta_key_is_descendant_of_meta_key(it_meta_key[rot], old_meta_key))
				break;
			copy_meta_key_move_to_directory(new_it_meta_key, old_meta_key,
			                                new_meta_key, it_meta_key[rot]);
			dbt_init(&new_key_dbt, new_it_meta_key, SIZEOF_KEY(*new_it_meta_key));
			ret = meta_db->put(meta_db, txn, &new_key_dbt, &value_dbt, DB_PUT_FLAGS);
			if (ret)
				goto out;
			rot = 1 - rot;
			r = cursor->c_get(cursor, &key_dbt[rot], &value_dbt, DB_NEXT);
			ret = meta_db->del(meta_db, txn, &key_dbt[1 - rot], DB_DEL_FLAGS);
			if (ret)
				goto out;
		}

		if (r && r != DB_NOTFOUND) {
			ret = r;
			goto out;
		}

		cursor->c_close(cursor);
		cursor = NULL;
	}

	{
		struct ftfs_data_key *it_data_key[2];
		struct ftfs_data_key *new_it_data_key = new_key_buf;

		it_data_key[0] = key_buf[0];
		it_data_key[1] = key_buf[1];
		rot = 0;
		if (is_dir)
			copy_child_data_key_from_meta_key(it_data_key[rot], old_meta_key, "", 0);
		else
			copy_data_key_from_meta_key(it_data_key[rot], old_meta_key, 0);
		dbt_init_extra_size(&key_dbt[rot], it_data_key[rot], SIZEOF_KEY(*it_data_key[rot]), key_buf_len);
		dbt_init(&key_dbt[1 - rot], it_data_key[1 - rot], key_buf_len);
		dbt_init(&value_dbt, block_buf, FTFS_BSTORE_BLOCKSIZE);

		ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
		if (ret)
			goto out;
		r = cursor->c_get(cursor, &key_dbt[rot], &value_dbt, DB_SET_RANGE);
		while (!r) {
			if (!data_key_is_descendant_of_meta_key(it_data_key[rot], old_meta_key))
				break;
			copy_data_key_move_to_directory(new_it_data_key, old_meta_key,
			                                new_meta_key, it_data_key[rot]);
			dbt_init(&new_key_dbt, new_it_data_key, SIZEOF_KEY(*new_it_data_key));
			ret = data_db->put(data_db, txn, &new_key_dbt, &value_dbt, DB_PUT_FLAGS);
			if (ret)
				goto out;
			rot = 1 - rot;
			r = cursor->c_get(cursor, &key_dbt[rot], &value_dbt, DB_NEXT);
			ret = data_db->del(data_db, txn, &key_dbt[1 - rot], DB_DEL_FLAGS);
			if (ret)
				goto out;
		}

		if (r && r != DB_NOTFOUND) {
			ret = r;
			goto out;
		}

		cursor->c_close(cursor);
		cursor = NULL;
	}

out:
	if (cursor)
		cursor->c_close(cursor);
	if (key_buf[0])
		kfree(key_buf[0]);
	if (key_buf[1])
		kfree(key_buf[1]);
	if (new_key_buf)
		kfree(new_key_buf);
	if (block_buf)
		kfree(block_buf);
	return ret;
}

/*
 * XXX: delete following functions
 */
int bstore_checkpoint(void)
{
	if (unlikely(!XXX_db_env))
		return -EINVAL;
	return XXX_db_env->txn_checkpoint(XXX_db_env, 0, 0, 0);
}

int bstore_hot_flush_all(void)
{
	int ret;
	uint64_t loops = 0;

	if (!XXX_data_db)
		return -EINVAL;

	ret = XXX_data_db->hot_optimize(XXX_data_db, NULL, NULL, NULL, NULL, &loops);

	ftfs_log(__func__, "%llu loops, returning %d", loops, ret);

	return ret;
}

void ftfs_print_engine_status(void)
{
	uint64_t nrows;
	int buff_size;
	char *buff;

	if (!XXX_db_env) {
		ftfs_error(__func__, "no db_env");
		return;
	}

	XXX_db_env->get_engine_status_num_rows(XXX_db_env, &nrows);
	buff_size = nrows * 128; //assume 128 chars per row
	buff = (char *)kmalloc(sizeof(char) * buff_size, GFP_KERNEL);
	if (buff == NULL)
		return;

	XXX_db_env->get_engine_status_text(XXX_db_env, buff, buff_size);
	kfree(buff);
}
