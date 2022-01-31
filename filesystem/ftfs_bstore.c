/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

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
static DB *XXX_meta_db;

static char ino_key[] = "m\x00\x00\x00\x00\x00\x00\x00\x00next_ino";

static char meta_desc_buf[] = "meta";
static char data_desc_buf[] = "data";
static DBT meta_desc = {
	.data = meta_desc_buf,
	.size = sizeof(meta_desc_buf),
	.ulen = sizeof(meta_desc_buf),
	.flags = DB_DBT_USERMEM,
};
static DBT data_desc = {
	.data = data_desc_buf,
	.size = sizeof(data_desc_buf),
	.ulen = sizeof(data_desc_buf),
	.flags = DB_DBT_USERMEM,
};

extern int
alloc_data_dbt_from_meta_dbt(DBT *data_dbt, DBT *meta_dbt, uint64_t block_num);

extern int
alloc_child_meta_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt, const char *name);

extern void
copy_data_dbt_from_meta_dbt(DBT *data_dbt, DBT *meta_dbt, uint64_t block_num);

extern int
alloc_meta_dbt_prefix(DBT *prefix_dbt, DBT *meta_dbt);

extern void copy_meta_dbt_from_ino(DBT *dbt, uint64_t ino);

static void
copy_child_meta_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt, const char *name)
{
	char *parent_key = parent_dbt->data;
	char *meta_key = dbt->data;
	size_t size;
	char *last_slash;

	if ((ftfs_key_path(parent_key))[0] == '\0')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	BUG_ON(size > dbt->ulen);
	ftfs_key_set_magic(meta_key, META_KEY_MAGIC);
	ftfs_key_copy_ino(meta_key, parent_key);
	if ((ftfs_key_path(parent_key))[0] == '\0') {
		sprintf(ftfs_key_path(meta_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(ftfs_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(ftfs_key_path(meta_key), ftfs_key_path(parent_key),
		       last_slash - ftfs_key_path(parent_key));
		sprintf(ftfs_key_path(meta_key) + (last_slash - ftfs_key_path(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
		//ftfs_error(__func__, "the key path=%s, parent=%s\n", ftfs_key_path(meta_key), ftfs_key_path(parent_key));
	}

	dbt->size = size;
}

static void
copy_child_data_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt,
                                  const char *name, uint64_t block_num)
{
	char *parent_key = parent_dbt->data;
	char *data_key = dbt->data;
	size_t size;
	char *last_slash;

	if ((ftfs_key_path(parent_key))[0] == '\0')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	size += DATA_META_KEY_SIZE_DIFF;
	BUG_ON(size > dbt->ulen);
	ftfs_key_set_magic(data_key, DATA_KEY_MAGIC);
	ftfs_key_copy_ino(data_key, parent_key);
	ftfs_data_key_set_blocknum(data_key, size, block_num);
	if ((ftfs_key_path(parent_key))[0] == '\0') {
		sprintf(ftfs_key_path(data_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(ftfs_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(ftfs_key_path(data_key), ftfs_key_path(parent_key),
		       last_slash - ftfs_key_path(parent_key));
		sprintf(ftfs_key_path(data_key) + (last_slash - ftfs_key_path(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
	}

	dbt->size = size;
}

static inline void
copy_subtree_max_meta_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt)
{
	copy_child_meta_dbt_from_meta_dbt(dbt, parent_dbt, "");
	*((char *)(dbt->data + dbt->size - 2)) = '\xff';
	//ftfs_error(__func__, "the key path=%s after\n", ftfs_key_path(dbt->data));
}

static inline void
copy_subtree_max_data_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt)
{
	copy_child_data_dbt_from_meta_dbt(dbt, parent_dbt, "", 0);
	*((char *)(dbt->data + dbt->size - sizeof(uint64_t) - 2)) = '\xff';
}

static void
copy_meta_dbt_movdir(const DBT *old_prefix_dbt, const DBT *new_prefix_dbt,
                     const DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key = new_dbt->data;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	BUG_ON(size > new_dbt->ulen);
	ftfs_key_set_magic(new_key, META_KEY_MAGIC);
	ftfs_key_copy_ino(new_key, new_prefix_key);
	//trace_printk("new prefix key=%s, old prefix=%s\n", ftfs_key_path(new_prefix_key), ftfs_key_path(old_prefix_dbt->data));
	sprintf(ftfs_key_path(new_key), "%s%s", ftfs_key_path(new_prefix_key),
	        old_key + old_prefix_dbt->size - 1);

	new_dbt->size = size;
}

static void
copy_data_dbt_movdir(const DBT *old_prefix_dbt, const DBT *new_prefix_dbt,
                     const DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key = new_dbt->data;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	BUG_ON(size > new_dbt->ulen);
	ftfs_key_set_magic(new_key, DATA_KEY_MAGIC);
	ftfs_key_copy_ino(new_key, new_prefix_key);
	sprintf(ftfs_key_path(new_key), "%s%s", ftfs_key_path(new_prefix_key),
	        old_key + old_prefix_dbt->size - 1);
	ftfs_data_key_set_blocknum(new_key, size,
		ftfs_data_key_get_blocknum(old_key, old_dbt->size));

	new_dbt->size = size;
}

static int
meta_key_is_child_of_meta_key(char *child_key, char *parent_key)
{
	char *last_slash;
	size_t first_part, second_part;

	if (ftfs_key_get_ino(child_key) != ftfs_key_get_ino(parent_key))
		return 0;

	if (ftfs_key_path(parent_key)[0] == '\0') {
		if (ftfs_key_path(child_key)[0] != '\x01' ||
		    ftfs_key_path(child_key)[1] != '\x01')
			return 0;
	} else {
		last_slash = strrchr(ftfs_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		first_part = last_slash - ftfs_key_path(parent_key);
		if (memcmp(ftfs_key_path(parent_key), ftfs_key_path(child_key), first_part))
			return 0;
		second_part = strlen(ftfs_key_path(parent_key)) - first_part - 1;
		if (memcmp(ftfs_key_path(child_key) + first_part,
		           ftfs_key_path(parent_key) + first_part + 1, second_part))
			return 0;
		if (ftfs_key_path(child_key)[first_part + second_part] != '\x01' ||
		    ftfs_key_path(child_key)[first_part + second_part + 1] != '\x01')
			return 0;
	}

	return 1;
}

// get the ino_num counting stored in meta_db
// for a brand new DB, it will init ino_num in meta_db (so it may write)
int ftfs_bstore_get_ino(DB *meta_db, DB_TXN *txn, ino_t *ino)
{
	int ret;
	DBT ino_key_dbt, ino_val_dbt;

	dbt_setup(&ino_key_dbt, ino_key, sizeof(ino_key));
	dbt_setup(&ino_val_dbt, ino, sizeof(*ino));

	ret = meta_db->get(meta_db, txn, &ino_key_dbt,
	                   &ino_val_dbt, DB_GET_FLAGS);
	if (ret == DB_NOTFOUND) {
		*ino = FTFS_ROOT_INO + 1;
		ret = meta_db->put(meta_db, txn, &ino_key_dbt,
		                   &ino_val_dbt, DB_PUT_FLAGS);
	}

	return ret;
}

// get the ino_num counting in meta_db
// if it is smaller than our ino, update that with our ino
int ftfs_bstore_update_ino(DB *meta_db, DB_TXN *txn, ino_t ino)
{
	int ret;
	ino_t curr_ino;
	DBT ino_key_dbt, ino_val_dbt;

	dbt_setup(&ino_key_dbt, ino_key, sizeof(ino_key));
	dbt_setup(&ino_val_dbt, &curr_ino, sizeof(curr_ino));

	ret = meta_db->get(meta_db, txn, &ino_key_dbt,
	                   &ino_val_dbt, DB_GET_FLAGS);
	if (!ret && ino > curr_ino) {
		curr_ino = ino;
		ret = meta_db->put(meta_db, txn, &ino_key_dbt,
		                   &ino_val_dbt, DB_PUT_FLAGS);
	}

	return ret;
}

static int env_keycmp(DB *DB, DBT const *a, DBT const *b)
{
	int r;
	uint32_t alen, blen;
	alen = a->size;
	blen = b->size;
	if (alen < blen) {
		r = memcmp(a->data, b->data, alen);
		if (r)
			return r;
		return -1;
	} else if (alen > blen) {
		r = memcmp(a->data, b->data, blen);
		if (r)
			return r;
		return 1;
	}
	// alen == blen
	return memcmp(a->data, b->data, alen);
}

#ifdef FTFS_PFSPLIT
static int
env_keypfsplit(DB *db, DBT const *a, DBT const *b,
               void (*set_key)(const DBT *new_key, void *set_extra),
               void *set_extra)
{
	char *sa, *sb;
	size_t i, size;
	int is_data;
	DBT pivot;
	char *buf;

	if (strcmp(db->cmp_descriptor->dbt.data, data_desc_buf) == 0) {
		is_data = 1;
	} else {
		BUG_ON(strcmp(db->cmp_descriptor->dbt.data, meta_desc_buf) != 0);
		is_data = 0;
	}

	sa = a->data;
	sb = b->data;
	i = 0;
	size = (a->size > b->size) ? b->size : a->size;
	if (is_data) {
		if (size <= 8)
			goto end_counting;
		size -= 8;
	}
	for (; i < size; i++) {
		if (sa[i] != sb[i])
			break;
	}

end_counting:
	if (set_key == NULL)
		return i;

	size = a->size;
	if (is_data) {
		if (size <= 8)
			goto use_a;
		size -= 8;
	}
	if (i >= size)
		goto use_a;
	if (b->size == 0) {
		goto check_data_key;
	}
	if (sa[i] == '\x01') {
		if (i > 0 && sa[i - 1] == '\x01') {
			i += 1;
		}
	}
	for (; i < size; i++) {
		if (sa[i] == '\x01') {
			break;
		}
	}
	if (i >= size)
		goto check_data_key;
	BUG_ON(i + 1 >= size);
	if (sa[i + 1] == '\xff')
		goto check_data_key;
	size = i + 3;
	if (is_data)
		size += 8;
	buf = kmalloc(size, GFP_KERNEL);
	if (buf == NULL)
		goto check_data_key;
	memcpy(buf, sa, i + 1);
	buf[i + 1] = '\xff';
	buf[i + 2] = '\x00';
	if (is_data)
		ftfs_data_key_set_blocknum(buf, size, 0);
use_our_key:
	dbt_setup(&pivot, buf, size);
	set_key(&pivot, set_extra);
	kfree(buf);
	return 0;
check_data_key:
	if (is_data) {
		size = a->size;
		buf = kmalloc(size, GFP_KERNEL);
		if (buf != NULL) {
			memcpy(buf, sa, size);
			ftfs_data_key_set_blocknum(buf, size, FTFS_UINT64_MAX);
			goto use_our_key;
		}
	}
use_a:
	set_key(a, set_extra);
	return 0;
}
#endif /* FTFS_PFSPLIT */

static int
env_keyrename(const DBT *old_prefix, const DBT *new_prefix, const DBT *old_dbt,
              void (*set_key)(const DBT *new_key, void *set_extra),
              void *set_extra)
{
	size_t new_len;
	void *new_key;
	DBT new_key_dbt;
	char *old_prefix_key = old_prefix->data;
	char *old_key = old_dbt->data;

	if (old_prefix->size > old_dbt->size)
		return -EINVAL;

	//This may happen when a kupsert was saved and added to the ancester
	//  list right before a cleaner thread kicks in and chooses to flush
	//  down this kupsert msg.... so the kupsert saved in the ancesters is
	//  stalei. This is possible because the cleaner threads do not lock
	//  hand-over-hand from the root to the leaf, instead it is done in the
	//  layer of cachetable which maintains a list of pairs and m_cleaner head
	if (!key_is_in_subtree_of_prefix(old_key, old_prefix_key, old_prefix->size) &&
	    !key_is_same_of_key(old_key, old_prefix_key))
		return 0;

	new_len = old_dbt->size - old_prefix->size + new_prefix->size;
	new_key = kmalloc(new_len, GFP_KERNEL);
	if (!new_key)
		return -ENOMEM;

	dbt_setup_buf(&new_key_dbt, new_key, new_len);
	if (IS_META_KEY_DBT(old_dbt))
		copy_meta_dbt_movdir(old_prefix, new_prefix,
		                     old_dbt, &new_key_dbt);
	else
		copy_data_dbt_movdir(old_prefix, new_prefix,
		                     old_dbt, &new_key_dbt);

	set_key(&new_key_dbt, set_extra);
	kfree(new_key);

	return 0;
}

static void env_keyprint(const DBT *key, bool is_trace_printable)
{
#if FTFS_NO_PRINT
	return;
#else
	if (key == NULL) {
		if(is_trace_printable) {
			trace_printk(KERN_INFO "ftfs_env_keypnt: key == NULL\n");
		} else {
			printk(KERN_INFO "ftfs_env_keypnt: key == NULL\n");
		}
	}
	else if (key->data == NULL) {
		if(is_trace_printable) {
			trace_printk(KERN_INFO "ftfs_env_keypnt: key->data == NULL\n");
		} else {

			printk(KERN_INFO "ftfs_env_keypnt: key->data == NULL\n");
		}
	}
	else if (IS_META_KEY_DBT(key)) {
		char *meta_key = key->data;
		if(is_trace_printable) {
			trace_printk(KERN_INFO "ftfs_env_keypnt: meta_key (%llu)%s, size=%u\n",
		        	         ftfs_key_get_ino(meta_key),
		                	 ftfs_key_path(meta_key),
					 key->size);
		} else {

			printk(KERN_INFO "ftfs_env_keypnt: meta_key (%llu)%s\n, size=%u\n",
		        	         ftfs_key_get_ino(meta_key),
		                	 ftfs_key_path(meta_key),
					 key->size);
		}
	} else if (IS_DATA_KEY_DBT(key)) {
		char *data_key = key->data;
		if(is_trace_printable) {
			trace_printk(KERN_INFO "ftfs_env_keypnt: data_key (%llu)%s:%llu\n",
		        	         ftfs_key_get_ino(data_key),
		                	 ftfs_key_path(data_key),
		                 	ftfs_data_key_get_blocknum(data_key, key->size));
		} else {

			printk(KERN_INFO "ftfs_env_keypnt: data_key (%llu)%s:%llu\n",
		        	         ftfs_key_get_ino(data_key),
		                	 ftfs_key_path(data_key),
		                 	ftfs_data_key_get_blocknum(data_key, key->size));
		}
	} else {
		BUG();
	}
#endif
}

#ifdef FTFS_LIFTING
static int
env_keylift(const DBT *lpivot, const DBT *rpivot,
            void (*set_lift)(const DBT *lift, void *set_extra), void *set_extra)
{
	uint32_t cmp_len, lift_len;
	const uint64_t *lp64, *rp64;
	const uint8_t *lp8, *rp8;
	char *lift_key;
	DBT lift_dbt;

	cmp_len = (lpivot->size < rpivot->size) ? lpivot->size : rpivot->size;
	lp64 = (uint64_t *)lpivot->data;
	rp64 = (uint64_t *)rpivot->data;
	while (cmp_len >= 8 && *lp64 == *rp64) {
		cmp_len -= 8;
		lp64 += 1;
		rp64 += 1;
	}
	lp8 = (uint8_t *)lp64;
	rp8 = (uint8_t *)rp64;
	while (cmp_len > 0 && *lp8 == *rp8) {
		cmp_len -= 1;
		lp8 += 1;
		rp8 += 1;
	}

	lift_len = lp8 - (uint8_t *)lpivot->data;
	if (lift_len == 0) {
		lift_key = NULL;
	} else {
		lift_key = kmalloc(lift_len, GFP_KERNEL);
		if (lift_key == NULL)
			return -ENOMEM;
		memcpy(lift_key, lpivot->data, lift_len);
	}
	dbt_setup(&lift_dbt, lift_key, lift_len);
	set_lift(&lift_dbt, set_extra);
	if (lift_len != 0)
		kfree(lift_key);

	return 0;
}

static int
env_keyliftkey(const DBT *key, const DBT *lifted,
               void (*set_key)(const DBT *new_key, void *set_extra),
               void *set_extra)
{
	uint32_t new_key_len;
	char *new_key;
	DBT new_key_dbt;

        // lifted not matching prefix, we cant lift
        if (lifted->size >= key->size ||
                memcmp(key->data, lifted->data, lifted->size) != 0)
            return -EINVAL;
	new_key_len = key->size - lifted->size;
        new_key = kmalloc(new_key_len, GFP_KERNEL);
        if (new_key == NULL)
                return -ENOMEM;
        memcpy(new_key, key->data + lifted->size, new_key_len);
	dbt_setup(&new_key_dbt, new_key, new_key_len);
	set_key(&new_key_dbt, set_extra);
	if (new_key_len != 0)
		kfree(new_key);

	return 0;
}

static int
env_keyunliftkey(const DBT *key, const DBT *lifted,
                 void (*set_key)(const DBT *new_key, void *set_extra),
                 void *set_extra)
{
	uint32_t new_key_len;
	char *new_key;
	DBT new_key_dbt;

	new_key_len = key->size + lifted->size;
	if (new_key_len == 0) {
		new_key = NULL;
	} else {
		new_key = kmalloc(new_key_len, GFP_KERNEL);
		if (new_key == NULL)
			return -ENOMEM;
		memcpy(new_key, lifted->data, lifted->size);
		memcpy(new_key + lifted->size, key->data, key->size);
	}
	dbt_setup(&new_key_dbt, new_key, new_key_len);
	set_key(&new_key_dbt, set_extra);
	if (new_key_len != 0)
		kfree(new_key);

	return 0;
}
#endif /* FTFS_LIFTING */

static struct toku_db_key_operations ftfs_key_ops = {
	.keycmp       = env_keycmp,
#ifdef FTFS_PFSPLIT
	.keypfsplit   = env_keypfsplit,
#else
	.keypfsplit   = NULL,
#endif
	.keyrename    = env_keyrename,
	.keyprint     = env_keyprint,
#ifdef FTFS_LIFTING
	.keylift      = env_keylift,
	.keyliftkey   = env_keyliftkey,
	.keyunliftkey = env_keyunliftkey,
#else
	.keylift      = NULL,
	.keyliftkey   = NULL,
	.keyunliftkey = NULL,
#endif
};

/*
 * block update callback info
 * set value in [offset, offset + size) to buf
 */
struct block_update_cb_info {
	loff_t offset;
	size_t size;
	char buf[];
};

static int
env_update_cb(DB *db, const DBT *key, const DBT *old_val, const DBT *extra,
              void (*set_val)(const DBT *newval, void *set_extra),
              void *set_extra)
{
	DBT val;
	size_t newval_size;
	void *newval;
	const struct block_update_cb_info *info = extra->data;

	if (info->size == 0) {
		// info->size == 0 means truncate
		if (!old_val) {
			newval_size = 0;
			newval = NULL;
		} else {
			newval_size = info->offset;
			if (old_val->size < newval_size) {
				// this means we should keep the old val
				// can we just forget about set_val in this case?
				// idk, to be safe, I did set_val here
				newval_size = old_val->size;
			}
			// now we guaranteed old_val->size >= newval_size
			newval = kmalloc(newval_size, GFP_KERNEL);
			if (!newval)
				return -ENOMEM;
			memcpy(newval, old_val->data, newval_size);
		}
	} else {
		// update [info->offset, info->offset + info->size) to info->buf
		newval_size = info->offset + info->size;
		if (old_val && old_val->size > newval_size)
			newval_size = old_val->size;
		newval = kmalloc(newval_size, GFP_KERNEL);
		if (!newval)
			return -ENOMEM;
		if (old_val) {
			// copy old val here
			memcpy(newval, old_val->data, old_val->size);
			// fill the place that is not covered by old_val
			//  nor info->buff with 0
			if (info->offset > old_val->size)
				memset(newval + old_val->size, 0,
				       info->offset - old_val->size);
		} else {
			if (info->offset > 0)
				memset(newval, 0, info->offset);
		}
		memcpy(newval + info->offset, info->buf, info->size);
	}

	dbt_setup(&val, newval, newval_size);
	set_val(&val, set_extra);
	kfree(newval);

	return 0;
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

	BUG_ON(sbi->db_env || sbi->data_db || sbi->meta_db);

	r = db_env_create(&sbi->db_env, 0);
	if (r != 0) {
		if (r == TOKUDB_HUGE_PAGES_ENABLED)
			printk(KERN_ERR "Failed to create the TokuDB environment because Transparent Huge Pages (THP) are enabled.  Please disable THP following the instructions at https://docs.mongodb.com/manual/tutorial/transparent-huge-pages/.  You may set the parameter to madvise or never. (errno %d)\n", r);
		else
			printk(KERN_ERR "Failed to create the TokuDB environment, errno %d\n", r);
		goto err;
	}

	db_env = sbi->db_env;

	giga_bytes = db_cachesize / (1L << 30);
	bytes = db_cachesize % (1L << 30);
	r = db_env->set_cachesize(db_env, giga_bytes, bytes, 1);
	if (r != 0)
		goto err;
	r = db_env->set_key_ops(db_env, &ftfs_key_ops);
	if (r != 0)
		goto err;

	db_env->set_update(db_env, env_update_cb);

	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
	               DB_INIT_LOCK | DB_RECOVER | DB_INIT_LOG | DB_INIT_TXN;

	r = db_env->open(db_env, DB_ENV_PATH, db_env_flags, 0755);
	if (r) {
		r = -ENOENT;
		goto err;
	}

	db_flags = DB_CREATE | DB_THREAD;
	r = db_create(&sbi->data_db, db_env, 0);
	if (r)
		goto err_close_env;
	r = db_create(&sbi->meta_db, db_env, 0);
	if (r)
		goto err_close_env;

	r = ftfs_bstore_txn_begin(db_env, NULL, &txn, 0);
	if (r)
		goto err_close_env;
	r = sbi->data_db->open(sbi->data_db, txn, DATA_DB_NAME, NULL,
	                       DB_BTREE, db_flags, 0644);
	if (r) {
		ftfs_bstore_txn_abort(txn);
		goto err_close_env;
	}
	r = sbi->data_db->change_descriptor(sbi->data_db, txn, &data_desc, DB_UPDATE_CMP_DESCRIPTOR);
	if (r) {
		ftfs_bstore_txn_abort(txn);
		goto err_close_env;
	}
	r = sbi->meta_db->open(sbi->meta_db, txn, META_DB_NAME, NULL,
	                       DB_BTREE, db_flags, 0644);
	if (r) {
		ftfs_bstore_txn_abort(txn);
		goto err_close_env;
	}
	r = sbi->meta_db->change_descriptor(sbi->meta_db, txn, &meta_desc, DB_UPDATE_CMP_DESCRIPTOR);
	if (r) {
		ftfs_bstore_txn_abort(txn);
		goto err_close_env;
	}

	r = ftfs_bstore_txn_commit(txn, DB_TXN_SYNC);
	if (r)
		goto err_close_env;

	/* set the cleaning and checkpointing thread periods */
	db_env_flags = 60; /* 60 s */
	r = db_env->checkpointing_set_period(db_env, db_env_flags);
	if (r)
		goto err_close;
	db_env_flags = 1; /* 1s */
	r = db_env->cleaner_set_period(db_env, db_env_flags);
	if (r)
		goto err_close;
	db_env_flags = 1000; /* 1000 ms */
	db_env->change_fsync_log_period(db_env, db_env_flags);

	XXX_db_env = sbi->db_env;
	XXX_data_db = sbi->data_db;
	XXX_meta_db = sbi->meta_db;

	return 0;

err_close:
	sbi->data_db->close(sbi->data_db, 0);
	sbi->meta_db->close(sbi->meta_db, 0);
err_close_env:
	db_env->close(db_env, 0);
err:
	return r;
}

/*
 * Close DB environment
 */
int ftfs_bstore_env_close(struct ftfs_sb_info *sbi)
{
	int ret;

	ret = ftfs_bstore_flush_log(sbi->db_env);
	if (ret)
		goto out;
	BUG_ON(sbi->data_db == NULL || sbi->meta_db == NULL || sbi->db_env == NULL);
	ret = sbi->data_db->close(sbi->data_db, 0);
	BUG_ON(ret);
	sbi->data_db = NULL;

	ret = sbi->meta_db->close(sbi->meta_db, 0);
	BUG_ON(ret);
	sbi->meta_db = NULL;

	ret = sbi->db_env->close(sbi->db_env, 0);
	BUG_ON(ret != 0);
	sbi->db_env = 0;

	XXX_db_env = NULL;
	XXX_data_db = NULL;
	XXX_meta_db = NULL;

out:
	return 0;
}

int ftfs_bstore_meta_get(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
                         struct ftfs_metadata *metadata)
{
	int ret;
	DBT value;

	dbt_setup(&value, metadata, sizeof(*metadata));

	ret = meta_db->get(meta_db, txn, meta_dbt, &value, DB_GET_FLAGS);
	if (ret == DB_NOTFOUND)
		ret = -ENOENT;

	return ret;
}

int ftfs_bstore_meta_put(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
                         struct ftfs_metadata *metadata)
{
	DBT value;

	dbt_setup(&value, metadata, sizeof(*metadata));

	return meta_db->put(meta_db, txn, meta_dbt, &value, DB_PUT_FLAGS);
}

int ftfs_bstore_meta_del(DB *meta_db, DBT *meta_dbt, DB_TXN *txn)
{
	return meta_db->del(meta_db, txn, meta_dbt, DB_DEL_FLAGS);
}

static unsigned char filetype_table[] = {
	DT_UNKNOWN, DT_FIFO, DT_CHR, DT_UNKNOWN,
	DT_DIR, DT_UNKNOWN, DT_BLK, DT_UNKNOWN,
	DT_REG, DT_UNKNOWN, DT_LNK, DT_UNKNOWN,
	DT_SOCK, DT_UNKNOWN, DT_WHT, DT_UNKNOWN
};

#define ftfs_get_type(mode) filetype_table[(mode >> 12) & 15]

int ftfs_bstore_meta_readdir(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
                             struct dir_context *ctx)
{
	int ret, r;
	char *child_meta_key;
	struct ftfs_metadata meta;
	DBT child_meta_dbt, metadata_dbt;
	DBC *cursor;
	char *name;
	u64 ino;
	unsigned type;
	char indirect_meta_key[SIZEOF_CIRCLE_ROOT_META_KEY];
	DBT indirect_meta_dbt;

	if (ctx->pos == 2) {
		child_meta_key = kmalloc(META_KEY_MAX_LEN, GFP_KERNEL);
		if (child_meta_key == NULL)
			return -ENOMEM;
		dbt_setup_buf(&child_meta_dbt, child_meta_key, META_KEY_MAX_LEN);
		copy_child_meta_dbt_from_meta_dbt(&child_meta_dbt, meta_dbt, "");
		ctx->pos = (loff_t)(child_meta_key);
	} else {
		child_meta_key = (char *)ctx->pos;
		dbt_setup(&child_meta_dbt, child_meta_key, META_KEY_MAX_LEN);
		child_meta_dbt.size = SIZEOF_META_KEY(child_meta_key);
	}

	dbt_setup(&metadata_dbt, &meta, sizeof(meta));
	dbt_setup_buf(&indirect_meta_dbt, indirect_meta_key,
	              SIZEOF_CIRCLE_ROOT_META_KEY);
	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt, DB_SET_RANGE);
	while (!r) {
		if (!meta_key_is_child_of_meta_key(child_meta_key, meta_dbt->data)) {
			kfree(child_meta_key);
			ctx->pos = 3;
			break;
		}
		if (meta.type == FTFS_METADATA_TYPE_REDIRECT) {
			copy_meta_dbt_from_ino(&indirect_meta_dbt, meta.u.ino);
			r = ftfs_bstore_meta_get(meta_db, &indirect_meta_dbt,
			                         txn, &meta);
			if (r)
				break;
		}
		ino = meta.u.st.st_ino;
		type = ftfs_get_type(meta.u.st.st_mode);
		name = strrchr(ftfs_key_path(child_meta_key), '\x01') + 1;
		if (!dir_emit(ctx, name, strlen(name), ino, type))
			break;

		r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt,
		                  DB_NEXT);
	}

	if (r == DB_NOTFOUND) {
		kfree(child_meta_key);
		ctx->pos = 3;
		r = 0;
	}

	cursor->c_close(cursor);

	if (r)
		ret = r;

out:
	return ret;
}

int ftfs_bstore_get(DB *data_db, DBT *data_dbt, DB_TXN *txn, void *buf)
{
	int ret;
	DBT value;

	dbt_setup(&value, buf, FTFS_BSTORE_BLOCKSIZE);

	ret = data_db->get(data_db, txn, data_dbt, &value, DB_GET_FLAGS);
	if (!ret && value.size < FTFS_BSTORE_BLOCKSIZE)
		memset(buf + value.size, 0, FTFS_BSTORE_BLOCKSIZE - value.size);
	if (ret == DB_NOTFOUND)
		ret = -ENOENT;

	return ret;
}

// size of buf must be FTFS_BLOCK_SIZE
int ftfs_bstore_put(DB *data_db, DBT *data_dbt, DB_TXN *txn,
                    const void *buf, size_t len, int is_seq)
{
	int ret;
	DBT value;

	dbt_setup(&value, buf, len);

	ret = is_seq ?
	      data_db->seq_put(data_db, txn, data_dbt, &value, DB_PUT_FLAGS) :
	      data_db->put(data_db, txn, data_dbt, &value, DB_PUT_FLAGS);

	return ret;
}

int ftfs_bstore_update(DB *data_db, DBT *data_dbt, DB_TXN *txn,
                       const void *buf, size_t size, loff_t offset)
{
	int ret;
	DBT extra_dbt;
	struct block_update_cb_info *info;
	size_t info_size = sizeof(struct block_update_cb_info) + size;

	info = kmalloc(info_size, GFP_KERNEL);
	if (!info)
		return -ENOMEM;
	info->offset = offset;
	info->size = size;
	memcpy(info->buf, buf, size);

	dbt_setup(&extra_dbt, info, info_size);

	ret = data_db->update(data_db, txn, data_dbt, &extra_dbt, DB_UPDATE_FLAGS);

	kfree(info);
	return ret;
}

// delete all blocks that is beyond new_num
//  if offset == 0, delete block new_num as well
//  otherwise, truncate block new_num to size offset
int ftfs_bstore_trunc(DB *data_db, DBT *meta_dbt,
                      DB_TXN *txn, uint64_t new_num, uint64_t offset)
{
	int ret;
	struct block_update_cb_info info;
	DBT min_data_key_dbt, max_data_key_dbt, extra_dbt;

	ret = alloc_data_dbt_from_meta_dbt(&min_data_key_dbt, meta_dbt,
		(offset == 0) ? new_num : (new_num + 1));
	if (ret)
		return ret;
	ret = alloc_data_dbt_from_meta_dbt(&max_data_key_dbt, meta_dbt, FTFS_UINT64_MAX);
	if (ret) {
		dbt_destroy(&min_data_key_dbt);
		return ret;
	}

	ret = data_db->del_multi(data_db, txn,
	                         &min_data_key_dbt,
	                         &max_data_key_dbt,
	                         0, 0);

	if (!ret && offset) {
		info.offset = offset;
		info.size = 0;
		dbt_setup(&extra_dbt, &info, sizeof(info));
		ftfs_data_key_set_blocknum(((char *)min_data_key_dbt.data),
		                           min_data_key_dbt.size, new_num);
		ret = data_db->update(data_db, txn, &min_data_key_dbt,
		                      &extra_dbt, DB_UPDATE_FLAGS);
	}

	dbt_destroy(&max_data_key_dbt);
	dbt_destroy(&min_data_key_dbt);

	return ret;
}

int ftfs_bstore_scan_one_page(DB *data_db, DBT *meta_dbt, DB_TXN *txn, struct page *page)
{
	int ret;
	DBT data_dbt;
	void *buf;

	//// now data_db keys start from 1
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt, PAGE_TO_BLOCK_NUM(page));
	if (ret)
		return ret;

	buf = kmap(page);
	ret = ftfs_bstore_get(data_db, &data_dbt, txn, buf);
	if (ret == -ENOENT) {
		memset(buf, 0, FTFS_BSTORE_BLOCKSIZE);
		ret = 0;
	}
	kunmap(page);

	dbt_destroy(&data_dbt);

	return ret;
}

struct ftfs_scan_pages_cb_info {
	char *meta_key;
	struct ftio *ftio;
	int do_continue;
};

static int ftfs_scan_pages_cb(DBT const *key, DBT const *val, void *extra)
{
	char *data_key = key->data;
	struct ftfs_scan_pages_cb_info *info = extra;
	struct ftio *ftio = info->ftio;

	if (key_is_same_of_key(data_key, info->meta_key)) {
		struct page *page = ftio_current_page(ftio);
		uint64_t page_block_num = PAGE_TO_BLOCK_NUM(page);
		void *page_buf;

		while (page_block_num < ftfs_data_key_get_blocknum(data_key, key->size)) {
			page_buf = kmap(page);
			memset(page_buf, 0, PAGE_SIZE);
			kunmap(page);

			ftio_advance_page(ftio);
			if (ftio_job_done(ftio))
				break;
			page = ftio_current_page(ftio);
			page_block_num = PAGE_TO_BLOCK_NUM(page);
		}

		if (page_block_num == ftfs_data_key_get_blocknum(data_key, key->size)) {
			page_buf = kmap(page);
			if (val->size)
				memcpy(page_buf, val->data, val->size);
			if (val->size < PAGE_SIZE)
				memset(page_buf + val->size, 0,
				       PAGE_SIZE - val->size);
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
		memset(page_buf, 0, PAGE_SIZE);
		kunmap(page);
		ftio_advance_page(ftio);
	}
}

int ftfs_bstore_scan_pages(DB *data_db, DBT *meta_dbt, DB_TXN *txn, struct ftio *ftio)
{
	int ret, r;
	struct ftfs_scan_pages_cb_info info;
	DBT data_dbt;
	DBC *cursor;

	//ftfs_error(__func__, "meta key path =%s\n", meta_key->path);
	if (ftio_job_done(ftio))
		return 0;
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt,
			PAGE_TO_BLOCK_NUM(ftio_current_page(ftio)));
	if (ret)
		return ret;

	ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto free_out;

	info.meta_key = meta_dbt->data;
	info.ftio = ftio;

	r = cursor->c_getf_set_range(cursor, 0, &data_dbt, ftfs_scan_pages_cb, &info);
	while (info.do_continue && !r)
		r = cursor->c_getf_next(cursor, 0, ftfs_scan_pages_cb, &info);
	if (r && r != DB_NOTFOUND)
		ret = r;
	if (!ret)
		ftfs_bstore_fill_rest_page(ftio);

	r = cursor->c_close(cursor);
	BUG_ON(r);
free_out:
	dbt_destroy(&data_dbt);

	return ret;
}

struct ftfs_die_cb_info {
	char *meta_key;
	int *is_empty;
};

static int ftfs_die_cb(DBT const *key, DBT const *val, void *extra)
{
	struct ftfs_die_cb_info *info = extra;
	char *current_meta_key = key->data;

	*(info->is_empty) = !meta_key_is_child_of_meta_key(current_meta_key, info->meta_key);

	return 0;
}

int ftfs_dir_is_empty(DB *meta_db, DBT *meta_dbt, DB_TXN *txn, int *is_empty)
{
	int ret, r;
	struct ftfs_die_cb_info info;
	DBT start_meta_dbt;
	DBC *cursor;

	ret = alloc_child_meta_dbt_from_meta_dbt(&start_meta_dbt, meta_dbt, "");
	if (ret)
		return ret;

	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	info.meta_key = meta_dbt->data;
	info.is_empty = is_empty;
	ret = cursor->c_getf_set_range(cursor, 0, &start_meta_dbt, ftfs_die_cb, &info);
	if (ret == DB_NOTFOUND) {
		ret = 0;
		*is_empty = 1;
	}

	r = cursor->c_close(cursor);
	BUG_ON(r);
out:
	dbt_destroy(&start_meta_dbt);

	return ret;
}

static int
ftfs_bstore_move_copy(DB *meta_db, DB *data_db, DBT *old_meta_dbt,
		      DBT *new_meta_dbt, DB_TXN *txn,
                      enum ftfs_bstore_move_type type)
{
	int r, ret, rot;
	char *it_key[2], *new_key, *block_buf;
	DBT val_dbt, key_dbt[2], new_key_dbt, new_prefix_dbt, old_prefix_dbt;
	struct ftfs_metadata meta;
	DBC *cursor;

	ret = -ENOMEM;
	it_key[0] = it_key[1] = new_key = block_buf = NULL;
	dbt_init(&old_prefix_dbt);
	dbt_init(&new_prefix_dbt);
	if ((it_key[0] = kmalloc(KEY_MAX_LEN, GFP_KERNEL)) == NULL)
		goto out;
	if ((it_key[1] = kmalloc(KEY_MAX_LEN, GFP_KERNEL)) == NULL)
		goto free_out;
	if ((new_key = kmalloc(KEY_MAX_LEN, GFP_KERNEL)) == NULL)
		goto free_out;
	if ((block_buf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL)) == NULL)
		goto free_out;

	dbt_setup_buf(&key_dbt[0], it_key[0], KEY_MAX_LEN);
	dbt_setup_buf(&key_dbt[1], it_key[1], KEY_MAX_LEN);
	dbt_setup_buf(&new_key_dbt, new_key, KEY_MAX_LEN);
	if (type == FTFS_BSTORE_MOVE_DIR) {
		ret = alloc_meta_dbt_prefix(&old_prefix_dbt, old_meta_dbt);
		if (ret)
			goto free_out;
		ret = alloc_meta_dbt_prefix(&new_prefix_dbt, new_meta_dbt);
		if (ret)
			goto free_out;

		dbt_setup_buf(&val_dbt, &meta, sizeof(meta));
		rot = 0;
		copy_child_meta_dbt_from_meta_dbt(&key_dbt[rot], old_meta_dbt, "");

		ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
		if (ret)
			goto free_out;
		r = cursor->c_get(cursor, &key_dbt[rot], &val_dbt, DB_SET_RANGE);
		while (!r) {
			// is this key in the subtree ?
			if (!key_is_in_subtree_of_prefix(it_key[rot],
				old_prefix_dbt.data, old_prefix_dbt.size))
				break;

			copy_meta_dbt_movdir(&old_prefix_dbt, &new_prefix_dbt,
			                     &key_dbt[rot], &new_key_dbt);
			ret = meta_db->put(meta_db, txn, &new_key_dbt, &val_dbt,
			                   DB_PUT_FLAGS);
			if (ret) {
freak_out:
				cursor->c_close(cursor);
				goto free_out;
			}
			rot = 1 - rot;
			r = cursor->c_get(cursor, &key_dbt[rot], &val_dbt,
			                  DB_NEXT);
			ret = meta_db->del(meta_db, txn, &key_dbt[1 - rot],
			                   DB_DEL_FLAGS);
			if (ret)
				goto freak_out;
		}

		if (r && r != DB_NOTFOUND) {
			ret = r;
			goto freak_out;
		}

		cursor->c_close(cursor);

		dbt_setup_buf(&val_dbt, block_buf, FTFS_BSTORE_BLOCKSIZE);
		rot = 0;
		copy_child_data_dbt_from_meta_dbt(&key_dbt[rot], old_meta_dbt, "", 0);
		ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
		if (ret)
			goto free_out;
		r = cursor->c_get(cursor, &key_dbt[rot], &val_dbt, DB_SET_RANGE);
		while (!r) {
			if (!key_is_in_subtree_of_prefix(it_key[rot],
				old_prefix_dbt.data, old_prefix_dbt.size))
				break;

			copy_data_dbt_movdir(&old_prefix_dbt, &new_prefix_dbt,
			                     &key_dbt[rot], &new_key_dbt);
			ret = data_db->put(data_db, txn, &new_key_dbt, &val_dbt,
			                   DB_PUT_FLAGS);
			if (ret)
				goto freak_out;
			rot = 1 - rot;
			r = cursor->c_get(cursor, &key_dbt[rot], &val_dbt,
			                  DB_NEXT);
			ret = data_db->del(data_db, txn, &key_dbt[1 - rot],
			                   DB_DEL_FLAGS);
			if (ret)
				goto freak_out;
		}
	} else {
		// only need to move data if we are renaming a file
		char *old_meta_key = old_meta_dbt->data;

		dbt_setup_buf(&val_dbt, block_buf, FTFS_BSTORE_BLOCKSIZE);
		rot = 0;

		copy_data_dbt_from_meta_dbt(&key_dbt[rot], old_meta_dbt, 0);
		ret = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
		if (ret)
			goto free_out;
		r = cursor->c_get(cursor, &key_dbt[rot], &val_dbt, DB_SET_RANGE);
		while (!r) {
			if (!key_is_same_of_key(it_key[rot], old_meta_key))
				break;

			copy_data_dbt_movdir(old_meta_dbt, new_meta_dbt,
			                     &key_dbt[rot], &new_key_dbt);
			ret = data_db->put(data_db, txn, &new_key_dbt, &val_dbt,
			                   DB_PUT_FLAGS);
			if (ret)
				goto freak_out;

			rot = 1 - rot;
			r = cursor->c_get(cursor, &key_dbt[rot], &val_dbt,
			                  DB_NEXT);
			ret = data_db->del(data_db, txn, &key_dbt[1 - rot],
			                   DB_DEL_FLAGS);
			if (ret)
				goto freak_out;
		}

		if (r && r != DB_NOTFOUND) {
			ret = r;
			goto freak_out;
		}

		cursor->c_close(cursor);
	}

free_out:
	dbt_destroy(&new_prefix_dbt);
	dbt_destroy(&old_prefix_dbt);
	if (block_buf)
		kfree(block_buf);
	if (new_key)
		kfree(new_key);
	if (it_key[1])
		kfree(it_key[1]);
	if (it_key[0])
		kfree(it_key[0]);
out:
	return ret;
}

#ifdef FTFS_RANGE_RENAME /* RANGE RENAME */
int
ftfs_bstore_move_rr(DB *meta_db, DB *data_db, DBT *old_meta_dbt,
                    DBT *new_meta_dbt, DB_TXN *txn,
                    enum ftfs_bstore_move_type type)
{
	int ret;
	void *min_key_buf, *max_key_buf;
	DBT min_key_dbt, max_key_dbt, old_prefix_dbt, new_prefix_dbt;

	ret = -ENOMEM;
	min_key_buf = max_key_buf = NULL;
	dbt_init(&old_prefix_dbt);
	dbt_init(&new_prefix_dbt);
	if ((min_key_buf = kmalloc(KEY_MAX_LEN, GFP_KERNEL)) == NULL)
		goto out;
	if ((max_key_buf = kmalloc(KEY_MAX_LEN, GFP_KERNEL)) == NULL)
		goto free_out;

	dbt_setup_buf(&min_key_dbt, min_key_buf, KEY_MAX_LEN);
	dbt_setup_buf(&max_key_dbt, max_key_buf, KEY_MAX_LEN);
	if (type == FTFS_BSTORE_MOVE_DIR) {
		ret = alloc_meta_dbt_prefix(&old_prefix_dbt, old_meta_dbt);
		if (ret)
			goto free_out;
		ret = alloc_meta_dbt_prefix(&new_prefix_dbt, new_meta_dbt);
		if (ret)
			goto free_out;
		// posterity is bond by (src\x01\x01, src\x1\xff)
		copy_child_meta_dbt_from_meta_dbt(&min_key_dbt,
			old_meta_dbt, "");
		copy_subtree_max_meta_dbt_from_meta_dbt(&max_key_dbt,
			old_meta_dbt);
		ret = meta_db->rename(meta_db, txn, &min_key_dbt, &max_key_dbt,
		                      &old_prefix_dbt, &new_prefix_dbt,
		                      DB_RENAME_FLAGS);

		if (ret)
			goto free_out;

		// we can ignore blocknum here
		copy_child_data_dbt_from_meta_dbt(&min_key_dbt, old_meta_dbt,
		                                  "", 0);
		copy_subtree_max_data_dbt_from_meta_dbt(&max_key_dbt,
		                                        old_meta_dbt);
		ret = data_db->rename(data_db, txn, &min_key_dbt, &max_key_dbt,
		                      &old_prefix_dbt, &new_prefix_dbt,
		                      DB_RENAME_FLAGS);
	} else {
		copy_data_dbt_from_meta_dbt(&min_key_dbt, old_meta_dbt, 0);
		copy_data_dbt_from_meta_dbt(&max_key_dbt, old_meta_dbt,
			FTFS_UINT64_MAX);
		ret = data_db->rename(data_db, txn, &min_key_dbt, &max_key_dbt,
		                      old_meta_dbt, new_meta_dbt,
		                      DB_RENAME_FLAGS);
	}

free_out:
	dbt_destroy(&new_prefix_dbt);
	dbt_destroy(&old_prefix_dbt);
	if (max_key_buf)
		kfree(max_key_buf);
	if (min_key_buf)
		kfree(min_key_buf);
out:
	return ret;
}

int
ftfs_bstore_move(DB *meta_db, DB *data_db, DBT *old_meta_dbt, DBT *new_meta_dbt,
                 DB_TXN *txn, enum ftfs_bstore_move_type type)
{
	if (type == FTFS_BSTORE_MOVE_SMALL_FILE)
		return ftfs_bstore_move_copy(meta_db, data_db, old_meta_dbt,
					     new_meta_dbt, txn, type);
	return ftfs_bstore_move_rr(meta_db, data_db, old_meta_dbt,
	                           new_meta_dbt, txn, type);
}
#else /* FTFS_RANGE_RENAME */
int
ftfs_bstore_move(DB *meta_db, DB *data_db, DBT *old_meta_dbt, DBT *new_meta_dbt,
                 DB_TXN *txn, enum ftfs_bstore_move_type type)
{
	return ftfs_bstore_move_copy(meta_db, data_db, old_meta_dbt,
	                             new_meta_dbt, txn, type);
}
#endif /* FTFS_RANGE_RENAME */

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

int ftfs_bstore_dump_node(bool is_data, int64_t b) {

	int ret;
	DB * db;
	if (!XXX_data_db || !XXX_meta_db)
			return -EINVAL;

	db = is_data? XXX_data_db:XXX_meta_db;
	ret = db->dump_ftnode(db, b);
	ftfs_log(__func__, "returning %d", ret);

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
