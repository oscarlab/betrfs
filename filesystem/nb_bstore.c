/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the "northbound" interface.
 *
 * It most directly implements the key-value operations on the
 * key-value store, such as get and put.  These are called by
 * VFS-level hooks in other files --- most commonly ftfs_super.c
 */

#include <linux/kernel.h>
#include <linux/slab.h>

#include "ftfs_northbound.h"

size_t db_cachesize;

#define DB_ENV_PATH "/db"
#define DATA_DB_NAME "ftfs_data"
#define META_DB_NAME "ftfs_meta"

// XXX: delete these 2 variables once southbound dependency is solved
static DB_ENV *XXX_db_env;
static DB *XXX_data_db;
static DB *XXX_meta_db;

static char ino_key[] = "next_ino";

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
#define IS_META_DESC(dbt) (((char *)dbt.data)[0] == 'm')  // to see if it matches meta_desc_buf[] = "meta"
#define IS_DATA_DESC(dbt) (((char *)dbt.data)[0] == 'd')  // to see if it matches data_desc_buf[] = "data"

void
dbt_copy_data_from_meta(DBT *data_dbt, DBT *meta_dbt, uint64_t block_num)
{
	char *meta_key = meta_dbt->data;
	char *data_key = data_dbt->data;
	size_t size;

	size = meta_dbt->size + DATA_META_KEY_SIZE_DIFF;
	BUG_ON(size > data_dbt->ulen);
	strcpy(KEY_PATH(data_key), KEY_PATH(meta_key));
	nb_data_key_set_blocknum(data_key, size, block_num);

	data_dbt->size = size;
}

int
dbt_alloc_data_from_meta(DBT *data_dbt, DBT *meta_dbt, uint64_t block_num)
{
	char *meta_key = meta_dbt->data;
	char *data_key;
	size_t size;

	size = meta_dbt->size + DATA_META_KEY_SIZE_DIFF;
	data_key = sb_malloc(size);
	if (data_key == NULL)
		return -ENOMEM;
	strcpy(KEY_PATH(data_key), KEY_PATH(meta_key));
	nb_data_key_set_blocknum(data_key, size, block_num);

	dbt_setup(data_dbt, data_key, size);
	return 0;
}

void
dbt_copy_child_meta_from_meta(DBT *dbt, DBT *parent_dbt, const char *name)
{
	char *parent_key = parent_dbt->data;
	char *meta_key = dbt->data;
	size_t size;
	char *last_slash;

	if ((KEY_PATH(parent_key))[0] == '\x00')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	BUG_ON(size > dbt->ulen);
	if ((KEY_PATH(parent_key))[0] == '\x00') {
		sprintf(KEY_PATH(meta_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(KEY_PATH(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(KEY_PATH(meta_key), KEY_PATH(parent_key),
		       last_slash - KEY_PATH(parent_key));
		sprintf(KEY_PATH(meta_key) + (last_slash - KEY_PATH(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
	}

	dbt->size = size;
}

int
dbt_alloc_child_meta_from_meta(DBT *dbt, DBT *parent_dbt, const char *name)
{
	char *parent_key = parent_dbt->data;
	char *meta_key;
	size_t size;
	char *last_slash;

	if ((KEY_PATH(parent_key))[0] == '\x00')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	meta_key = kmalloc(size, GFP_KERNEL);
	if (meta_key == NULL)
		return -ENOMEM;
	if ((KEY_PATH(parent_key))[0] == '\x00') {
		sprintf(KEY_PATH(meta_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(KEY_PATH(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(KEY_PATH(meta_key), KEY_PATH(parent_key),
		       last_slash - KEY_PATH(parent_key));
		sprintf(KEY_PATH(meta_key) + (last_slash - KEY_PATH(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
	}

	dbt_setup(dbt, meta_key, size);
	return 0;
}

void
dbt_copy_child_data_from_meta(DBT *dbt, DBT *parent_dbt, const char *name,
                              uint64_t block_num)
{
	char *parent_key = parent_dbt->data;
	char *data_key = dbt->data;
	size_t size;
	char *last_slash;

	if ((KEY_PATH(parent_key))[0] == '\x00')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	size += DATA_META_KEY_SIZE_DIFF;
	BUG_ON(size > dbt->ulen);
	nb_data_key_set_blocknum(data_key, size, block_num);
	if ((KEY_PATH(parent_key))[0] == '\x00') {
		sprintf(KEY_PATH(data_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(KEY_PATH(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(KEY_PATH(data_key), KEY_PATH(parent_key),
		       last_slash - KEY_PATH(parent_key));
		sprintf(KEY_PATH(data_key) + (last_slash - KEY_PATH(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
	}

	dbt->size = size;
}

static inline void
dbt_copy_subtree_max_meta_from_meta(DBT *dbt, DBT *parent_dbt)
{
	dbt_copy_child_meta_from_meta(dbt, parent_dbt, "");
	*((char *)(dbt->data + dbt->size - 2)) = '\xff';
}

static inline void
dbt_copy_subtree_max_data_from_meta(DBT *dbt, DBT *parent_dbt)
{
	dbt_copy_child_data_from_meta(dbt, parent_dbt, "", FTFS_UINT64_MAX);
	*((char *)(dbt->data + dbt->size - sizeof(uint64_t) - 2)) = '\xff';
}

int
dbt_alloc_prefix_from_meta(DBT *prefix_dbt, DBT *meta_dbt)
{
	char *meta_key;
	char *prefix_key;
	size_t size;
	char *last_slash;

	meta_key = meta_dbt->data;
	if ((KEY_PATH(meta_key))[0] == '\x00') {
		dbt_init(prefix_dbt);
		return 0;
	}
	// remove 1 slash 1 endofstring
	size = meta_dbt->size - 2;
	prefix_key = sb_malloc(size);
	if (!prefix_key) {
		return -ENOMEM;
	}
	last_slash = strrchr(KEY_PATH(meta_key), '\x01');
	BUG_ON(!last_slash);
	memcpy(prefix_key, KEY_PATH(meta_key),
	       last_slash - KEY_PATH(meta_key));
	memcpy(prefix_key + (last_slash - KEY_PATH(meta_key)),
	       last_slash + 1,
	       meta_dbt->size - (last_slash - KEY_PATH(meta_key)) - 2);

	dbt_setup(prefix_dbt, prefix_key, size);

	return 0;
}

static inline int
dbt_is_in_subtree_of_prefix(DBT *dbt, DBT *prefix)
{
	return !memcmp(KEY_PATH(dbt->data), KEY_PATH(dbt->data), prefix->size)
	       && KEY_PATH(dbt->data)[prefix->size] == '\x01';
}

int dbt_alloc_meta_movdir(DBT *old_prefix_dbt, DBT *new_prefix_dbt,
                          DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	new_key = sb_malloc(size);
	if (new_key == NULL)
		return -ENOMEM;

	memcpy(KEY_PATH(new_key), KEY_PATH(new_prefix_key),
	       new_prefix_dbt->size);
	strcpy(KEY_PATH(new_key) + new_prefix_dbt->size,
	       KEY_PATH(old_key) + old_prefix_dbt->size);

	dbt_setup(new_dbt, new_key, size);
	return 0;
}

static inline void
copy_subtree_max_data_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt)
{
	dbt_copy_child_data_from_meta(dbt, parent_dbt, "", 0);
	*((char *)(dbt->data + dbt->size - sizeof(uint64_t) - 2)) = '\xff';
}

void
dbt_copy_meta_movdir(DBT *old_prefix_dbt, DBT *new_prefix_dbt,
                     DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key = new_dbt->data;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	BUG_ON(size > new_dbt->ulen);
	memcpy(KEY_PATH(new_key), KEY_PATH(new_prefix_key),
	       new_prefix_dbt->size);
	strcpy(KEY_PATH(new_key) + new_prefix_dbt->size,
	       KEY_PATH(old_key) + old_prefix_dbt->size);

	new_dbt->size = size;
}

void
dbt_copy_data_movdir(const DBT *old_prefix_dbt, const DBT *new_prefix_dbt,
                     const DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key = new_dbt->data;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	BUG_ON(size > new_dbt->ulen);
	memcpy(KEY_PATH(new_key), KEY_PATH(new_prefix_key),
	       new_prefix_dbt->size);
	strcpy(KEY_PATH(new_key) + new_prefix_dbt->size,
	       KEY_PATH(old_key) + old_prefix_dbt->size);
	nb_data_key_set_blocknum(new_key, size,
		nb_data_key_get_blocknum(old_key, old_dbt->size));
	new_dbt->size = size;
}

static int
meta_key_is_child_of_meta_key(char *child_key, char *parent_key)
{
	char *last_slash;
	size_t first_part, second_part;

	if (KEY_PATH(parent_key)[0] == '\x00') {
		if (KEY_PATH(child_key)[0] != '\x01' ||
		    KEY_PATH(child_key)[1] != '\x01')
			return 0;
	} else {
		last_slash = strrchr(KEY_PATH(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		first_part = last_slash - KEY_PATH(parent_key);
		if (memcmp(KEY_PATH(parent_key), KEY_PATH(child_key), first_part))
			return 0;
		second_part = strlen(KEY_PATH(parent_key)) - first_part - 1;
		if (memcmp(KEY_PATH(child_key) + first_part,
		           KEY_PATH(parent_key) + first_part + 1, second_part))
			return 0;
		if (KEY_PATH(child_key)[first_part + second_part] != '\x01' ||
		    KEY_PATH(child_key)[first_part + second_part + 1] != '\x01')
			return 0;
	}

	return 1;
}

// get the ino_num counting stored in meta_db
// for a brand new DB, it will init ino_num in meta_db (so it may write)
int nb_bstore_get_ino(DB *meta_db, DB_TXN *txn, ino_t *ino)
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
int nb_bstore_update_ino(DB *meta_db, DB_TXN *txn, ino_t ino)
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
	char *sa, *sb, *s;
	size_t i, size;
	int is_data;
	DBT pivot;
	char *buf;

	if (IS_DATA_DESC(db->cmp_descriptor->dbt)) {
		is_data = 1;
	} else if (IS_META_DESC(db->cmp_descriptor->dbt)) {
		is_data = 0;
	} else {
		BUG();
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
	for (; i + 8 <= size; i += 8) {
		if (*((uint64_t *)(sa + i)) != *((uint64_t *)(sb + i)))
			break;
	}
	for (; i < size; i++) {
		if (sa[i] != sb[i])
			break;
	}
end_counting:
	if (set_key == NULL)
		return i;

	if (is_data) {
		if (a->size > i + 8) {
			if ((s = strchr(sa + i, '\x01')) != NULL &&
			    !(s == sa + i && i > 0 && sa[i - 1] == '\x01')
			   ) {
				size = (s - sa) + 11;
				buf = sb_malloc(size);
				if (buf == NULL)
					goto no_advice;
				memcpy(buf, sa, size - 10);
				buf[size - 10] = '\xff';
				buf[size - 9] = '\x00';
				nb_data_key_set_blocknum(buf, size, FTFS_UINT64_MAX);
			} else {
				size = a->size;
				buf = sb_malloc(size);
				if (buf == NULL)
					goto no_advice;
				memcpy(buf, sa, size - 8);
				nb_data_key_set_blocknum(buf, size, FTFS_UINT64_MAX);
			}
		} else if (b->size > i + 8) {
			if ((s = strchr(sb + i, '\x01')) != NULL) {
				size = (s - sb) + 11;
				buf = sb_malloc(size);
				if (buf == NULL)
					goto no_advice;
				memcpy(buf, sb, size - 10);
				buf[size - 10] = '\x01';
				buf[size - 9] = '\x00';
				nb_data_key_set_blocknum(buf, size, 0);
			} else {
				size = b->size;
				buf = sb_malloc(size);
				if (buf == NULL)
					goto no_advice;
				memcpy(buf, sb, size - 8);
				nb_data_key_set_blocknum(buf, size, 0);
			}
		} else {
			goto no_advice;
		}
	} else {
		if (a->size > i && (s = strchr(sa + i, '\x01')) != NULL &&
				!(s == sa + i && i > 0 && sa[i - 1] == '\x01')) {
			size = (s - sa) + 3;
			buf = sb_malloc(size);
			if (buf == NULL)
				goto no_advice;
			memcpy(buf, sa, size - 2);
			buf[size - 2] = '\xff';
			buf[size - 1] = '\x00';
		} else if (b->size > i && (s = strchr(sb + i, '\x01')) != NULL) {
			size = (s - sb) + 3;
			buf = sb_malloc(size);
			if (buf == NULL)
				goto no_advice;
			memcpy(buf, sb, size - 2);
			buf[size - 2] = '\x01';
			buf[size - 1] = '\x00';
		} else {
			goto no_advice;
		}
	}

	dbt_setup(&pivot, buf, size);
	set_key(&pivot, set_extra);
	sb_free(buf);
no_advice:
	return i;
}
#endif /* FTFS_PFSPLIT */

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
		        	         nb_key_get_ino(meta_key),
		                	 nb_key_path(meta_key),
					 key->size);
		} else {

			printk(KERN_INFO "ftfs_env_keypnt: meta_key (%llu)%s\n, size=%u\n",
		        	         nb_key_get_ino(meta_key),
		                	 nb_key_path(meta_key),
					 key->size);
		}
	} else if (IS_DATA_KEY_DBT(key)) {
		char *data_key = key->data;
		if(is_trace_printable) {
			trace_printk(KERN_INFO "ftfs_env_keypnt: data_key (%llu)%s:%llu\n",
		        	         nb_key_get_ino(data_key),
		                	 nb_key_path(data_key),
		                 	nb_data_key_get_blocknum(data_key, key->size));
		} else {

			printk(KERN_INFO "ftfs_env_keypnt: data_key (%llu)%s:%llu\n",
		        	         nb_key_get_ino(data_key),
		                	 nb_key_path(data_key),
		                 	nb_data_key_get_blocknum(data_key, key->size));
		}
	} else {
		BUG();
	}
#endif
}

static int
env_keylift(DB *db, const DBT *lpivot, const DBT *rpivot,
            void (*set_lift)(const DBT *lift, void *set_extra), void *set_extra)
{
	uint32_t cmp_len, lift_len, i;
	char *lift_key;
	DBT lift_dbt;

	cmp_len = (lpivot->size < rpivot->size) ? lpivot->size : rpivot->size;

	if (cmp_len == 0) {
		lift_len = 0;
		goto set_lift;
	}

	if (((char *)db->cmp_descriptor->dbt.data)[0] == 'd') {
		cmp_len -= 8;
	} else {
		BUG_ON(((char *)db->cmp_descriptor->dbt.data)[0] != 'm');
	}

	lift_len = 0;
	for (i = 0; i < cmp_len; i++) {
		if (((char *)lpivot->data)[i] != ((char *)rpivot->data)[i])
			break;
		if (((char *)lpivot->data)[i] == '\x01' ||
			((char *)lpivot->data)[i] == '\x00'
		   ) {
			lift_len = i + 1;
		}
	}

set_lift:
	lift_key = (lift_len == 0) ? NULL : lpivot->data;
	dbt_setup(&lift_dbt, lift_key, lift_len);
	set_lift(&lift_dbt, set_extra);

	return 0;
}

static int
env_liftkey(const DBT *key, const DBT *lifted,
            void (*set_key)(const DBT *new_key, void *set_extra),
            void *set_extra)
{
	uint32_t new_key_len;
	char *new_key;
	DBT new_key_dbt;

	if (lifted->size > key->size ||
	    memcmp(key->data, lifted->data, lifted->size) != 0
	   ) {
		return -EINVAL;
	}

	new_key_len = key->size - lifted->size;
	new_key = (new_key_len == 0) ? NULL : key->data + lifted->size;
	dbt_setup(&new_key_dbt, new_key, new_key_len);
	set_key(&new_key_dbt, set_extra);

	return 0;
}

static int
env_unliftkey(const DBT *key, const DBT *lifted,
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
		new_key = sb_malloc(new_key_len);
		if (new_key == NULL)
			return -ENOMEM;
		memcpy(new_key, lifted->data, lifted->size);
		memcpy(new_key + lifted->size, key->data, key->size);
	}
	dbt_setup(&new_key_dbt, new_key, new_key_len);
	set_key(&new_key_dbt, set_extra);
	if (new_key_len != 0)
		sb_free(new_key);

	return 0;
}

static struct toku_db_key_operations ftfs_key_ops = {
	.keycmp       = env_keycmp,
#ifdef FTFS_PFSPLIT
	.keypfsplit   = env_keypfsplit,
#else
	.keypfsplit   = NULL,
#endif
	.keyprint     = env_keyprint,
	.keylift      = env_keylift,
	.liftkey      = env_liftkey,
	.unliftkey    = env_unliftkey,
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
			newval = sb_malloc(newval_size);
			if (!newval)
				return -ENOMEM;
			memcpy(newval, old_val->data, newval_size);
		}
	} else {
		// update [info->offset, info->offset + info->size) to info->buf
		newval_size = info->offset + info->size;
		if (old_val && old_val->size > newval_size)
			newval_size = old_val->size;
		newval = sb_malloc(newval_size);
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
	sb_free(newval);

	return 0;
}

/*
 * Set up DB environment.
 */
int nb_bstore_env_open(struct ftfs_sb_info *sbi)
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
	if (r != 0) {
		printk(KERN_ERR "nb_bstore_open: Failed to set the cachesize %d\n", r);
		goto err;
	}
	r = db_env->set_key_ops(db_env, &ftfs_key_ops);
	if (r != 0) {
		printk(KERN_ERR "nb_bstore_open: Failed to set the key_ops %d\n", r);
		goto err;
	}

	db_env->set_update(db_env, env_update_cb);

	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
	               DB_INIT_LOCK | DB_RECOVER | DB_INIT_LOG | DB_INIT_TXN;

	r = db_env->open(db_env, DB_ENV_PATH, db_env_flags, 0755);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to open the db %d\n", r);
		r = -ENOENT;
		goto err;
	}

	db_flags = DB_CREATE | DB_THREAD;
	r = db_create(&sbi->data_db, db_env, 0);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to create the data db %d\n", r);
		goto err_close_env;
	}
	r = db_create(&sbi->meta_db, db_env, 0);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to create the meta db %d\n", r);
		goto err_close_env;
	}

	r = nb_bstore_txn_begin(db_env, NULL, &txn, 0);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to bstore txn begin %d\n", r);
		goto err_close_env;
	}

	if (!sbi->is_rotational) {
		r = sbi->data_db->set_compression_method(sbi->data_db, TOKU_NO_COMPRESSION);
		if (r) {
			printk(KERN_ERR "nb_bstore_open: Failed to set data db compression method %d\n", r);
			nb_bstore_txn_abort(txn);
			goto err_close_env;
		}
	}

	r = sbi->data_db->open(sbi->data_db, txn, DATA_DB_NAME, NULL,
	                       DB_BTREE, db_flags, 0644);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to open the data db %d\n", r);
		nb_bstore_txn_abort(txn);
		goto err_close_env;
	}
	r = sbi->data_db->change_descriptor(sbi->data_db, txn, &data_desc, DB_UPDATE_CMP_DESCRIPTOR);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to change the data db descriptor %d\n", r);
		nb_bstore_txn_abort(txn);
		goto err_close_env;
	}

	if (!sbi->is_rotational) {
		r = sbi->meta_db->set_compression_method(sbi->meta_db, TOKU_NO_COMPRESSION);
		if (r) {
			printk(KERN_ERR "nb_bstore_open: Failed to set meta db compression method %d\n", r);
			nb_bstore_txn_abort(txn);
			goto err_close_env;
		}
	}

	r = sbi->meta_db->open(sbi->meta_db, txn, META_DB_NAME, NULL,
	                       DB_BTREE, db_flags, 0644);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to open the meta db %d\n", r);
		nb_bstore_txn_abort(txn);
		goto err_close_env;
	}

	r = sbi->meta_db->change_descriptor(sbi->meta_db, txn, &meta_desc, DB_UPDATE_CMP_DESCRIPTOR);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to change the meta db descriptor %d\n", r);
		nb_bstore_txn_abort(txn);
		goto err_close_env;
	}
	r = nb_bstore_txn_commit(txn, DB_TXN_SYNC);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to bstore txn commit %d\n", r);
		goto err_close_env;
	}

	/* set the cleaning and checkpointing thread periods */
	db_env_flags = 60; /* 60 s */
	r = db_env->checkpointing_set_period(db_env, db_env_flags);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to set the checkpoint period %d\n", r);
		goto err_close;
	}
	db_env_flags = 1; /* 1s */
	r = db_env->cleaner_set_period(db_env, db_env_flags);
	if (r) {
		printk(KERN_ERR "nb_bstore_open: Failed to set the cleaner period %d\n", r);
		goto err_close;
	}
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
int nb_bstore_env_close(struct ftfs_sb_info *sbi)
{
	int ret;

	ret = nb_bstore_flush_log(sbi->db_env);
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

int nb_bstore_meta_get(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
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

int nb_bstore_meta_put(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
                         struct ftfs_metadata *metadata)
{
	DBT value;

	dbt_setup(&value, metadata, sizeof(*metadata));

	return meta_db->put(meta_db, txn, meta_dbt, &value, DB_PUT_FLAGS);
}

int nb_bstore_meta_del(DB *meta_db, DBT *meta_dbt, DB_TXN *txn)
{
	return meta_db->del(meta_db, txn, meta_dbt, DB_DEL_FLAGS);
}

static unsigned char filetype_table[] = {
	DT_UNKNOWN, DT_FIFO, DT_CHR, DT_UNKNOWN,
	DT_DIR, DT_UNKNOWN, DT_BLK, DT_UNKNOWN,
	DT_REG, DT_UNKNOWN, DT_LNK, DT_UNKNOWN,
	DT_SOCK, DT_UNKNOWN, DT_WHT, DT_UNKNOWN
};

#define nb_get_type(mode) filetype_table[(mode >> 12) & 15]

int nb_bstore_meta_readdir(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
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

	if (ctx->pos == 2) {
		child_meta_key = sb_malloc(META_KEY_MAX_LEN);
		if (child_meta_key == NULL)
			return -ENOMEM;
		dbt_setup_buf(&child_meta_dbt, child_meta_key, META_KEY_MAX_LEN);
		dbt_copy_child_meta_from_meta(&child_meta_dbt, meta_dbt, "");
		ctx->pos = (loff_t)(child_meta_key);
	} else {
		child_meta_key = (char *)ctx->pos;
		dbt_setup(&child_meta_dbt, child_meta_key, META_KEY_MAX_LEN);
		child_meta_dbt.size = SIZEOF_META_KEY(child_meta_key);
	}

	dbt_setup(&metadata_dbt, &meta, sizeof(meta));
	ret = meta_db->cursor(meta_db, txn, &cursor, DB_SEQ_READ);
	if (ret)
		goto out;

	r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt, DB_SET_RANGE);
	while (!r) {
		if (!meta_key_is_child_of_meta_key(child_meta_key, meta_dbt->data)) {
			sb_free(child_meta_key);
			ctx->pos = 3;
			break;
		}
		ino = meta.st.st_ino;
		type = nb_get_type(meta.st.st_mode);
		name = strrchr(KEY_PATH(child_meta_key), '\x01') + 1;
		if (!dir_emit(ctx, name, strlen(name), ino, type))
			break;

		r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt,
		                  DB_NEXT);
	}

	if (r == DB_NOTFOUND) {
		sb_free(child_meta_key);
		ctx->pos = 3;
		r = 0;
	}

	cursor->c_close(cursor);

	if (r)
		ret = r;

out:
	return ret;
}

int nb_bstore_get(DB *data_db, DBT *data_dbt, DB_TXN *txn, void *buf)
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

/* Use a point query for prefetch */
static int nb_bstore_prefetch(DB *data_db, DBT *data_dbt, DB_TXN *txn, void *buf)
{
	int ret;
	DBT value;

	dbt_setup(&value, buf, FTFS_BSTORE_BLOCKSIZE);

	ret = data_db->get(data_db, txn, data_dbt, &value, DB_SEQ_READ);
	if (!ret && value.size < FTFS_BSTORE_BLOCKSIZE)
		memset(buf + value.size, 0, FTFS_BSTORE_BLOCKSIZE - value.size);
	if (ret == DB_NOTFOUND)
		ret = -ENOENT;

	return ret;
}


// size of buf must be FTFS_BLOCK_SIZE
int nb_bstore_put(DB *data_db, DBT *data_dbt, DB_TXN *txn,
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

int nb_bstore_update(DB *data_db, DBT *data_dbt, DB_TXN *txn,
		     const void *buf, size_t size, loff_t offset)
{
	int ret;
	DBT extra_dbt;
	struct block_update_cb_info *info;
	size_t info_size = sizeof(struct block_update_cb_info) + size;

	info = sb_malloc(info_size);
	if (!info)
		return -ENOMEM;
	info->offset = offset;
	info->size = size;
	memcpy(info->buf, buf, size);

	dbt_setup(&extra_dbt, info, info_size);

	ret = data_db->update(data_db, txn, data_dbt, &extra_dbt, DB_UPDATE_FLAGS);

	sb_free(info);
	return ret;
}

// delete all blocks that is beyond new_num
//  if offset == 0, delete block new_num as well
//  otherwise, truncate block new_num to size offset
int nb_bstore_trunc(DB *data_db, DBT *meta_dbt,
		    DB_TXN *txn, uint64_t new_num, uint64_t offset)
{
	int ret;
	struct block_update_cb_info *info;
	DBT min_data_key_dbt, max_data_key_dbt, extra_dbt, last_data_key_dbt;
	size_t lastblocksize, info_size;

	ret = dbt_alloc_data_from_meta(&min_data_key_dbt, meta_dbt,
		                       (offset == 0) ? (new_num) :
	                                               (new_num + 1));
	if (ret)
		return ret;
	ret = dbt_alloc_data_from_meta(&max_data_key_dbt, meta_dbt,
	                               FTFS_UINT64_MAX);
	if (ret) {
		dbt_destroy(&min_data_key_dbt);
		return ret;
	}

	ret = data_db->rd(data_db, txn, &min_data_key_dbt, &max_data_key_dbt, 0);
	dbt_destroy(&max_data_key_dbt);
	dbt_destroy(&min_data_key_dbt);

	if (!ret && offset) {
		lastblocksize = FTFS_BSTORE_BLOCKSIZE - offset;
		info_size = sizeof(struct block_update_cb_info) + lastblocksize;
		info = sb_malloc(info_size);
		if (!info){
		    return -ENOMEM;
		}
		ret = dbt_alloc_data_from_meta(&last_data_key_dbt, meta_dbt, new_num);
		if (ret) {
		    sb_free(info);
		    return ret;
		}
		info->offset = offset;
		info->size = lastblocksize;
		memset(info->buf, 0, lastblocksize);
		dbt_setup(&extra_dbt, info, info_size);
		// now wipe (overwrite 0 to) the data in dbt, the rest of data after offset should be all 0
		ret = data_db->update(data_db, txn, &last_data_key_dbt,
		                      &extra_dbt, DB_UPDATE_FLAGS);
		dbt_destroy(&last_data_key_dbt);
		sb_free(info);
	}
	return ret;
}

// This function is called by nb_rmdir
// It issues a range delete in data_db for the directory referenced by meta_dbt
int dir_delete(DBT *meta_dbt, DB *data_db, DB_TXN *txn) {
	int ret;
	void *max_key_buf;
	DBT min_data_key_dbt, max_data_key_dbt;

	dbt_init(&min_data_key_dbt);
	dbt_init(&max_data_key_dbt);

	ret = -ENOMEM;
	max_key_buf = NULL;
	if ((max_key_buf = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto out;

	dbt_setup_buf(&max_data_key_dbt, max_key_buf, KEY_MAX_LEN);

	ret = dbt_alloc_data_from_meta(&min_data_key_dbt, meta_dbt, 0);
	if (ret) goto out;

	copy_subtree_max_data_dbt_from_meta_dbt(&max_data_key_dbt, meta_dbt);

	ret = data_db->rd(data_db, txn,
	                  &min_data_key_dbt,
	                  &max_data_key_dbt,
	                  DB_RD_FLAGS);
out:
	dbt_destroy(&max_data_key_dbt);
	dbt_destroy(&min_data_key_dbt);
	return ret;
}

int nb_bstore_scan_one_page(DB *data_db, DBT *meta_dbt, DB_TXN *txn, struct page *page)
{
	int ret;
	DBT data_dbt;
	void *buf;

	//// now data_db keys start from 1
	ret = dbt_alloc_data_from_meta(&data_dbt, meta_dbt,
	                               PAGE_TO_BLOCK_NUM(page));
	if (ret)
		return ret;

	buf = kmap(page);
	ret = nb_bstore_get(data_db, &data_dbt, txn, buf);
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

		while (page_block_num < nb_data_key_get_blocknum(data_key, key->size)) {
			page_buf = kmap(page);
			memset(page_buf, 0, PAGE_CACHE_SIZE);
			kunmap(page);

			ftio_advance_page(ftio);
			if (ftio_job_done(ftio))
				break;
			page = ftio_current_page(ftio);
			page_block_num = PAGE_TO_BLOCK_NUM(page);
		}

		if (page_block_num == nb_data_key_get_blocknum(data_key, key->size)) {
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

int nb_bstore_scan_pages(DB *data_db, DBT *meta_dbt, DB_TXN *txn, struct ftio *ftio)
{
	int ret, r;
	struct ftfs_scan_pages_cb_info info;
	DBT data_dbt;
	DBC *cursor;
	void *buf = NULL;
	unsigned long curr_offset;
	unsigned int cursor_flags;

	//ftfs_error(__func__, "meta key path =%s\n", meta_key->path);
	if (ftio_job_done(ftio))
		return 0;
	// Step 1: Seq read detection
	curr_offset = ftio_first_page(ftio)->index;
	if ((curr_offset == 0 && ftio->last_offset == ULONG_MAX) || curr_offset == ftio->last_offset + 1) {
		cursor_flags = DB_SEQ_READ;
	} else {
		cursor_flags = DB_CURSOR_FLAGS;
	}
	// Step 2: issue a point query before range query
	ret = dbt_alloc_data_from_meta(&data_dbt, meta_dbt,
			PAGE_TO_BLOCK_NUM(ftio_current_page(ftio)));
	if (ret)
		return ret;
	// FIXME: The point query has the effect of doing some prefetching.
	// Do we need to change it?
	buf = sb_malloc(FTFS_BSTORE_BLOCKSIZE);
	if (!buf) {
		return 0;
	}
	if (cursor_flags == DB_SEQ_READ) {
		ret = nb_bstore_prefetch(data_db, &data_dbt, txn, buf);
	} else {
		ret = nb_bstore_get(data_db, &data_dbt, txn, buf);
	}
	sb_free(buf);
	if (ret == -ENOENT) {
		ret = 0;
		ftfs_bstore_fill_rest_page(ftio);
		goto free_out;
	}
	// Step 3: Do the range query
	ret = data_db->cursor(data_db, txn, &cursor, cursor_flags);
	if (ret)
		goto free_out;

	info.meta_key = meta_dbt->data;
	info.ftio = ftio;

	r = cursor->c_getf_set_range(cursor, 0, &data_dbt, ftfs_scan_pages_cb, &info);
	while (info.do_continue && !r){
		r = cursor->c_getf_next(cursor, 0, ftfs_scan_pages_cb, &info);
	}
	if (r && r != DB_NOTFOUND)
		ret = r;
	if (!ret){
		ftfs_bstore_fill_rest_page(ftio);
	}

	r = cursor->c_close(cursor);
	BUG_ON(r);
free_out:
	dbt_destroy(&data_dbt);

	return ret;
}

struct betrfs_die_cb_info {
	char *meta_key;
	int *is_empty;
};

static int nb_die_cb(DBT const *key, DBT const *val, void *extra)
{
	struct betrfs_die_cb_info *info = extra;
	char *current_meta_key = key->data;
	*(info->is_empty) = !meta_key_is_child_of_meta_key(current_meta_key, info->meta_key);

	return 0;
}

int nb_dir_is_empty(DB *meta_db, DBT *meta_dbt, DB_TXN *txn, int *is_empty)
{
	int ret, r;
	struct betrfs_die_cb_info info;
	DBT start_meta_dbt;
	DBC *cursor;

	ret = dbt_alloc_child_meta_from_meta(&start_meta_dbt, meta_dbt, "");
	if (ret)
		return ret;

	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto out;

	info.meta_key = meta_dbt->data;
	info.is_empty = is_empty;

	ret = cursor->c_getf_set_range(cursor, 0, &start_meta_dbt, nb_die_cb, &info);
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

int
nb_bstore_dir_clone(DB *meta_db, DB *data_db, DBT *src_key, DBT *dst_key,
                      DB_TXN *txn, bool delete_src)
{
	int ret;
	void *min_key_buf, *max_key_buf;
	DBT min_key_dbt, max_key_dbt, src_prefix, dst_prefix;

	ret = -ENOMEM;
	min_key_buf = max_key_buf = NULL;
	dbt_init(&src_prefix);
	dbt_init(&dst_prefix);
	if ((min_key_buf = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto out;
	if ((max_key_buf = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto free_out;

	dbt_setup_buf(&min_key_dbt, min_key_buf, KEY_MAX_LEN);
	dbt_setup_buf(&max_key_dbt, max_key_buf, KEY_MAX_LEN);

	ret = dbt_alloc_prefix_from_meta(&src_prefix, src_key);
	if (ret)
		goto free_out;
	ret = dbt_alloc_prefix_from_meta(&dst_prefix, dst_key);
	if (ret)
		goto free_out;
	// posterity is bound by (src\x01\x01, src\x1\xff)
	dbt_copy_child_meta_from_meta(&min_key_dbt, src_key, "");
	dbt_copy_subtree_max_meta_from_meta(&max_key_dbt, src_key);
	ret = meta_db->clone(meta_db, txn, &min_key_dbt, &max_key_dbt,
			     &src_prefix, &dst_prefix,
			     delete_src ? DB_MOVE_FLAGS : DB_CLONE_FLAGS);

	if (ret)
		goto free_out;

	// we can ignore blocknum here
	dbt_copy_child_data_from_meta(&min_key_dbt, src_key, "", 0);
	dbt_copy_subtree_max_data_from_meta(&max_key_dbt, src_key);
	ret = data_db->clone(data_db, txn, &min_key_dbt, &max_key_dbt,
	                     &src_prefix, &dst_prefix,
	                     delete_src ? DB_MOVE_FLAGS : DB_CLONE_FLAGS);

free_out:
	dbt_destroy(&src_prefix);
	dbt_destroy(&dst_prefix);
	if (max_key_buf)
		sb_free(max_key_buf);
	if (min_key_buf)
		sb_free(min_key_buf);
out:
	return ret;
}

int
nb_bstore_file_clone(DB *data_db, DBT *src_key, DBT *dst_key,
                       DB_TXN *txn, bool delete_old)
{
	int r;
	DBT min_key, max_key;

	dbt_init(&min_key);
	dbt_init(&max_key);

	r = dbt_alloc_data_from_meta(&min_key, src_key, 0);
	if (r) {
		return r;
	}
	r = dbt_alloc_data_from_meta(&max_key, src_key, FTFS_UINT64_MAX);
	if (r) {
		goto free_out;
	}

	r = data_db->clone(data_db, txn, &min_key, &max_key, src_key, dst_key,
	                   delete_old ? DB_MOVE_FLAGS : DB_CLONE_FLAGS);


free_out:
	dbt_destroy(&min_key);
	dbt_destroy(&max_key);

	return r;
}

int
nb_bstore_file_clone_copy(DB *data_db, DBT *src_key, DBT *dst_key,
                            DB_TXN *txn, bool delete_src)
{
	int r;
	char *buf;
	DBT val_dbt, old_key, new_key;
	DBC *cursor;

	dbt_init(&old_key);
	dbt_init(&new_key);
	dbt_init(&val_dbt);
	r = -ENOMEM;
	// val
	buf = kmalloc(FTFS_BSTORE_BLOCKSIZE, GFP_KERNEL);
	if (!buf) {
		goto out;
	}
	dbt_setup_buf(&val_dbt, buf, FTFS_BSTORE_BLOCKSIZE);
	// old_key
	buf = kmalloc(KEY_MAX_LEN, GFP_KERNEL);
	if (!buf) {
		goto out;
	}
	dbt_setup_buf(&old_key, buf, KEY_MAX_LEN);
	dbt_copy_data_from_meta(&old_key, src_key, 0);

	// new_key
	r = dbt_alloc_data_from_meta(&new_key, dst_key, 0);
	if (r) {
		goto out;
	}

	r = data_db->cursor(data_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (r) {
		goto out;
	}
	r = cursor->c_get(cursor, &old_key, &val_dbt, DB_SET_RANGE);
	while (!r) {
		if (!key_is_same_of_key(old_key.data, src_key->data))
			break;

		nb_data_key_set_blocknum(new_key.data, new_key.size,
		nb_data_key_get_blocknum(old_key.data, old_key.size));
		r = data_db->put(data_db, txn, &new_key, &val_dbt, DB_PUT_FLAGS);
		if (r) {
			goto cursor_out;
		}
		if (delete_src) {
			r = data_db->del(data_db, txn, &old_key, DB_DEL_FLAGS);
			if (r) {
				goto cursor_out;
			}
		}

		r = cursor->c_get(cursor, &old_key, &val_dbt, DB_NEXT);
	}
	if (r == DB_NOTFOUND) {
		r = 0;
	}
cursor_out:
	cursor->c_close(cursor);
out:
	dbt_destroy(&old_key);
	dbt_destroy(&new_key);
	dbt_destroy(&val_dbt);
	return r;
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

int nb_bstore_dump_node(bool is_data, int64_t b) {

	int ret;
	DB * db;
	if (!XXX_data_db || !XXX_meta_db)
			return -EINVAL;

	db = is_data? XXX_data_db:XXX_meta_db;
	ret = db->dump_ftnode(db, b);
	ftfs_log(__func__, "returning %d", ret);

	return ret;
}
void nb_print_engine_status(void)
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
	buff = (char *)sb_malloc(sizeof(char) * buff_size);
	if (buff == NULL)
		return;

	XXX_db_env->get_engine_status_text(XXX_db_env, buff, buff_size);
	sb_free(buff);
}
