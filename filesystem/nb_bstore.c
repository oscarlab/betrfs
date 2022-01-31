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
#include <linux/bitmap.h>
#include <linux/namei.h>
#include <linux/dcache.h>

#include "ftfs_northbound.h"
#include "ftfs_indirect.h"

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

#ifdef FTFS_XATTR
#define START_XATTR_BUFFER_SIZE 256
/* Extended Attribute Schema:
 *  Extended attributes (xattrs) are stored in the meta index.
 *  Xattr keys have a special delimiter NB_BSTORE_XATTR_DELIMITER
 */

int
copy_xattr_dbt_from_meta_dbt(DBT *xattr_dbt, DBT *meta_dbt, const char *name, bool alloc)
{
	char *meta_key = meta_dbt->data;
	char *xattr_key;
	size_t size;

	if ((nb_key_path(meta_key))[0] == '\0') {
		size = meta_dbt->size + strlen(name) + 1;
	} else {
		size = meta_dbt->size + strlen(name) + 2;
	}

	if (alloc) {
		xattr_key = kmalloc(size, GFP_KERNEL);
		if (xattr_key == NULL) {
			return -ENOMEM;
		}
	} else {
		BUG_ON(size > xattr_dbt->ulen);
		xattr_key = xattr_dbt->data;
	}

	if ((nb_key_path(meta_key))[0] == '\0') {
		sprintf(nb_key_path(xattr_key), "%c%s", NB_BSTORE_XATTR_DELIMITER, name);
	} else {
		sprintf(nb_key_path(xattr_key), "%s%c%s", nb_key_path(meta_key),
			NB_BSTORE_XATTR_DELIMITER, name);
	}

	if (alloc) {
		dbt_setup(xattr_dbt, xattr_key, size);
	} else {
		xattr_dbt->size = size;
	}

	return 0;
}

/* This function adjusts an xattr key (dbt) after a rename.
* XXX: It is identical to copy_dbt_move; do we actually need a separate
* function?
*/
static void
copy_xattr_dbt_movdir(const DBT *old_prefix_dbt, const DBT *new_prefix_dbt,
                     const DBT *old_dbt, DBT *new_dbt)
{
	char *new_prefix_key = new_prefix_dbt->data;
	char *old_key = old_dbt->data;
	char *new_key = new_dbt->data;
	size_t size;

	size = old_dbt->size - old_prefix_dbt->size + new_prefix_dbt->size;
	BUG_ON(size > new_dbt->ulen);
	sprintf(nb_key_path(new_key), "%s%s", nb_key_path(new_prefix_key),
	        old_key + old_prefix_dbt->size - 1);

	new_dbt->size = size;
}

static int
key_is_xattr_of_meta_key(char *xattr_key, char *parent_key)
{
	if (nb_key_path(parent_key)[0] == '\0') {
		if (nb_key_path(xattr_key)[0] != NB_BSTORE_XATTR_DELIMITER)
			return 0;
	} else {
		size_t parent_len = strlen(nb_key_path(parent_key));
		if (memcmp(nb_key_path(xattr_key), nb_key_path(parent_key), parent_len))
			return 0;
		return nb_key_path(xattr_key)[parent_len] == NB_BSTORE_XATTR_DELIMITER;
	}
	return 1;
}

#endif /* End of XATTR */

static void
copy_child_meta_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt, const char *name)
{
	char *parent_key = parent_dbt->data;
	char *meta_key = dbt->data;
	size_t size;
	char *last_slash;

	if ((nb_key_path(parent_key))[0] == '\x00')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	BUG_ON(size > dbt->ulen);
	if ((nb_key_path(parent_key))[0] == '\x00') {
		sprintf(nb_key_path(meta_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(nb_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(nb_key_path(meta_key), nb_key_path(parent_key),
		       last_slash - nb_key_path(parent_key));
		sprintf(nb_key_path(meta_key) + (last_slash - nb_key_path(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
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

	if ((nb_key_path(parent_key))[0] == '\x00')
		size = parent_dbt->size + strlen(name) + 2;
	else
		size = parent_dbt->size + strlen(name) + 1;
	size += DATA_META_KEY_SIZE_DIFF;
	BUG_ON(size > dbt->ulen);
	nb_data_key_set_blocknum(data_key, size, block_num);
	if ((nb_key_path(parent_key))[0] == '\x00') {
		sprintf(nb_key_path(data_key), "\x01\x01%s", name);
	} else {
		last_slash = strrchr(nb_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		memcpy(nb_key_path(data_key), nb_key_path(parent_key),
		       last_slash - nb_key_path(parent_key));
		sprintf(nb_key_path(data_key) + (last_slash - nb_key_path(parent_key)),
		        "%s\x01\x01%s", last_slash + 1, name);
	}

	dbt->size = size;
}

static inline void
copy_subtree_max_meta_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt)
{
	copy_child_meta_dbt_from_meta_dbt(dbt, parent_dbt, "");
	*((char *)(dbt->data + dbt->size - 2)) = '\xff';
}

static inline void
copy_subtree_max_data_dbt_from_meta_dbt(DBT *dbt, DBT *parent_dbt)
{
	copy_child_data_dbt_from_meta_dbt(dbt, parent_dbt, "", 0);
	*((char *)(dbt->data + dbt->size - sizeof(uint64_t) - 2)) = '\xff';
}

/* This function adjusts a meta key (dbt) after a rename */
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
	sprintf(nb_key_path(new_key), "%s%s", nb_key_path(new_prefix_key),
	        old_key + old_prefix_dbt->size - 1);

	new_dbt->size = size;
}

/* This function adjusts a data key (dbt) after a rename */
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
	sprintf(nb_key_path(new_key), "%s%s", nb_key_path(new_prefix_key),
	        old_key + old_prefix_dbt->size - 1);
	nb_data_key_set_blocknum(new_key, size,
		nb_data_key_get_blocknum(old_key, old_dbt->size));

	new_dbt->size = size;
}


static int
meta_key_is_child_of_meta_key(char *child_key, char *parent_key)
{
	char *last_slash;
	size_t first_part, second_part;

	if (nb_key_path(parent_key)[0] == '\x00') {
		if (nb_key_path(child_key)[0] != '\x01' ||
		    nb_key_path(child_key)[1] != '\x01')
			return 0;
	} else {
		last_slash = strrchr(nb_key_path(parent_key), '\x01');
		BUG_ON(last_slash == NULL);
		first_part = last_slash - nb_key_path(parent_key);
		if (memcmp(nb_key_path(parent_key), nb_key_path(child_key), first_part))
			return 0;
		second_part = strlen(nb_key_path(parent_key)) - first_part - 1;
		if (memcmp(nb_key_path(child_key) + first_part,
		           nb_key_path(parent_key) + first_part + 1, second_part))
			return 0;
		if (nb_key_path(child_key)[first_part + second_part] != '\x01' ||
		    nb_key_path(child_key)[first_part + second_part + 1] != '\x01')
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
					!(s == sa + i && i > 0 && sa[i - 1] == '\x01')) {
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

/* env_keyrename: This is the callback for a range rename.
 */
static int
env_keyrename(DB *db, const DBT *old_prefix,
              const DBT *new_prefix, const DBT *old_dbt,
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
	new_key = sb_malloc(new_len);
	if (!new_key)
		return -ENOMEM;

	dbt_setup_buf(&new_key_dbt, new_key, new_len);
	if (IS_META_DESC(db->cmp_descriptor->dbt))
		copy_meta_dbt_movdir(old_prefix, new_prefix,
		                     old_dbt, &new_key_dbt);
	else if (IS_DATA_DESC(db->cmp_descriptor->dbt))
		copy_data_dbt_movdir(old_prefix, new_prefix,
		                     old_dbt, &new_key_dbt);
	else
		BUG();

	set_key(&new_key_dbt, set_extra);
	sb_free(new_key);

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


/*
 * Function: env_keylift
 * ----------------------------
 *   Do the key lifting based on lpivot and rpivot
 *
 *   Caller function: toku_ft_lift()
 *
 *   db     : a FAKE_DB with cmp_descriptor copied, to check whether it is data db or meta db.
 *   lpivot : the left pivot (childkey)
 *   rpivot : the right pivot (childkey)
 *   set_lift: the function for setting the lifting result, i.e setkey_func()
 *   set_extra: the destination for set_key
 *
 *   return : always 0 successful
 */
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

    if (IS_DATA_DESC(db->cmp_descriptor->dbt)) {
        cmp_len -= 8;
    } else {
        BUG_ON(!IS_META_DESC(db->cmp_descriptor->dbt));
    }

    lift_len = 0;
    for (i = 0; i < cmp_len; i++) {
        if (((char *)lpivot->data)[i] != ((char *)rpivot->data)[i])
            break;
        if (((char *)lpivot->data)[i] == FTFS_SLASH_CHAR ||
            ((char *)lpivot->data)[i] == FTFS_NULL_CHAR
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


/*
 * Function: env_keyliftkey
 * ----------------------------
 *   Construct a new lifted key
 *
 *   Caller function: toku_ft_lift_key()
 *
 *   key    : the input key
 *   lifted : the lifted part
 *   set_key: the function for setting the result, i.e setkey_func()
 *   set_extra: the destination for set_key
 *
 *   returns: 0 (sucessful) or -EINVAL
 */
static int
env_keyliftkey(const DBT *key, const DBT *lifted,
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
	.keyrename    = env_keyrename,
	.keyprint     = env_keyprint,
	.keylift      = env_keylift,
	.keyliftkey   = env_keyliftkey,
	.keyunliftkey = env_keyunliftkey,
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

/*
 * Merge an update insert with an indirect value from the basement
 * node. `new_page` is the output page and return value is the new
 * size of the new indirect value.
 *
 * `new_page` is a page which is not yet inserted into the page cache.
 */
static size_t update_indirect_old_val(const DBT *old_val,
				   struct page **new_page,
				   const struct block_update_cb_info *info)
{
	struct ftfs_indirect_val msg_val;
	struct page *ft_page;
	struct page *page;
	unsigned long pfn;
	char *src;
	char *dest;
	size_t newval_size;

	/* Verify the size */
	if (sizeof(struct ftfs_indirect_val) != old_val->size) {
		printk("%s:old_val->size=%d\n", __func__, old_val->size);
		toku_dump_hex("old_val:", old_val->data, old_val->size);
		BUG();
	}
	/* Copy the value and get the pfn */
	memcpy(&msg_val, old_val->data, sizeof(struct ftfs_indirect_val));
	pfn = msg_val.pfn;
	if (!pfn_valid(pfn)) {
		printk("%s:flags=%d, pfn=%lu\n", __func__, old_val->flags, pfn);
		BUG();
	}
	ft_page = pfn_to_page(pfn);
	if (!PageUptodate(ft_page)) {
		sb_wait_read_page(msg_val.pfn);
	}
	page = __ftfs_page_cache_alloc(FTFS_READ_PAGE_GFP);
	/* copy data from ft page first */
	preempt_disable();
	src = kmap_atomic(ft_page);
	dest = kmap_atomic(page);
	memcpy(dest, src, msg_val.size);

	/* Calculate the size of new message */
	newval_size = info->offset + info->size;
	if (msg_val.size > newval_size)
		newval_size = msg_val.size;
	/* zero out the bytes that not covered by update message */
	if (info->offset > msg_val.size)
		memset(dest + msg_val.size, 0,
			info->offset - msg_val.size);
	/* copy data from update message */
	memcpy(dest + info->offset, info->buf, info->size);
	/* unmap the pages */
	kunmap_atomic(src);
	kunmap_atomic(dest);
	preempt_enable();
	*new_page = page;
	return newval_size;
}

/* `newval_size` is the size of data in the page.
 * This function is to set up a new DBT for indirect value.
 * DBT->data := new_msg_val and DBT->size := sizeof(*new_msg_val)
 * Make sure DBT->type is DB_DBT_INDIRECT.
 */
void update_setup_new_val(DBT *val, struct page *new_page, size_t newval_size)
{
	struct ftfs_indirect_val *new_msg_val;
	int new_size = newval_size;

	BUG_ON(new_page == NULL);
	new_msg_val = ftfs_alloc_msg_val();
	BUG_ON(new_msg_val == NULL);
	ftfs_inc_page_private_count(new_page);
	ftfs_fill_msg_val(new_msg_val, page_to_pfn(new_page), new_size);
	dbt_setup(val, new_msg_val, sizeof(*new_msg_val));
	val->flags = DB_DBT_INDIRECT;
}

/*
 * This is called to apply an upsert to a non-leaf node.
 * key is the key, old_val is what is in the basement node
 * extra is a DBT with the packed update message
 *
 * set_val is the function passed from ft code. It is used
 * to insert the newly created value to the basement node.
 */
static int
env_update_cb(DB *db, const DBT *key, const DBT *old_val, const DBT *extra,
              void (*set_val)(const DBT *newval, void *set_extra),
              void *set_extra)
{
	DBT val;
	size_t newval_size = 0;
	void *newval = NULL;
	const struct block_update_cb_info *info = extra->data;
	struct page *page = NULL;
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
		if (old_val && old_val->flags == DB_DBT_INDIRECT) {
			newval_size = update_indirect_old_val(old_val, &page, info);
		} else {
			// Normal handling path when old val is null or regular value
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
	}

	if (old_val && old_val->flags == DB_DBT_INDIRECT) {
		/* newval_size is assigned to
		 * the size field of ftfs_indirect_val
		 */
		update_setup_new_val(&val, page, newval_size);
		BUG_ON(val.flags != DB_DBT_INDIRECT);
	} else {
		dbt_setup(&val, newval, newval_size);
	}
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

int
nb_bstore_meta_lookup_create(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
                             struct ftfs_metadata *metadata)
{
	int ret;
	DBT value;
	dbt_setup(&value, metadata, sizeof(*metadata));

	ret = meta_db->get(meta_db, txn, meta_dbt, &value, DB_GET_FLAGS | DB_LOOKUP_CREATE);
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

int nb_bstore_meta_readdir(struct super_block *sb, struct dentry *parent,
			   DB *meta_db, DBT *meta_dbt,
			   DB_TXN *txn, struct dir_context *ctx)
{
	int ret, r;
	char *child_meta_key;
	struct ftfs_metadata meta;
	struct inode *inode;
	struct dentry *child_dentry;
	DBT child_meta_dbt, metadata_dbt;
	DBC *cursor;
	char *name;
	u64 ino;
	unsigned type;
	struct qstr this;
	bool dentry_cached;

	if (ctx->pos == 2) {
		child_meta_key = sb_malloc(META_KEY_MAX_LEN);
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
	ret = meta_db->cursor(meta_db, txn, &cursor, DB_SEQ_READ);

	if (ret)
		goto out;

	/* child_meta_dbt is the key; metadata_dbt is the value */
	r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt, DB_SET_RANGE);

	while (!r) {
		if (!meta_key_is_child_of_meta_key(child_meta_key, meta_dbt->data)) {
			sb_free(child_meta_key);
			ctx->pos = 3;
			break;
		}
#ifdef FTFS_XATTR
		if (strchr(nb_key_path(child_meta_key), NB_BSTORE_XATTR_DELIMITER) != NULL) {
			r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt, DB_NEXT);
			continue;
		}
#endif /* End of FTFS_XATTR */
		ino = meta.st.st_ino;
		type = nb_get_type(meta.st.st_mode);
		name = strrchr(nb_key_path(child_meta_key), '\x01') + 1;

		dentry_cached = true;
		this.name = name;
		this.len = strlen(name);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
		this.hash = full_name_hash(parent, name, this.len);
#else
		this.hash = full_name_hash(name, this.len);
#endif
		child_dentry = d_lookup(parent, &this);
		if (!child_dentry) {
			/* Alloc a new dentry */
			child_dentry = d_alloc(parent, &this);
			if (!child_dentry) {
				printk("%s: d_alloc failed\n", __func__);
				BUG();
			}
			dentry_cached = false;
		}

		if (!child_dentry->d_inode) {
			/* Copy child's meta key */
			DBT copy_meta_dbt;
			r = dbt_alloc(&copy_meta_dbt, child_meta_dbt.size);
			copy_meta_dbt.size = child_meta_dbt.size;
			BUG_ON(r != 0);
			memcpy(copy_meta_dbt.data, child_meta_dbt.data, child_meta_dbt.size);
			/*
			 * Setup inode for the child: either get it from inode cache
			 * or allocate a new one
			 */
			inode = nb_setup_inode(sb, &copy_meta_dbt, &meta);
			if (IS_ERR(inode)) {
				printk("%s: nb_setup_inode failed\n", __func__);
				BUG();
			}
			d_add(child_dentry, inode);
			/* Make sure child_dentry is hashed */
			BUG_ON(d_unhashed(child_dentry));
			dput(child_dentry);
		} else {
			/* Do nothing */
		}
		/*
		 * If the dentry was in dcache
		 * do not change the refcount
		 */
		if (dentry_cached) {
			dput(child_dentry);
		}
		if (!dir_emit(ctx, name, strlen(name), ino, type))
			break;
		r = cursor->c_get(cursor, &child_meta_dbt, &metadata_dbt, DB_NEXT);
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

#ifdef FTFS_XATTR
ssize_t nb_bstore_xattr_get(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
			    void *buf, size_t size) {
	int r;
	ssize_t ret;
	DBT value;

	dbt_setup(&value, buf, size);

	r = meta_db->get(meta_db, txn, meta_dbt, &value, DB_GET_FLAGS);
	ret = value.size;
	if (!r && value.size < size)
		memset(buf + value.size, 0, size - value.size);
	if (r == DB_NOTFOUND)
		ret = -ENODATA;

	return ret;
}

int nb_bstore_xattr_put(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
			const void *buf, size_t len) {
	int ret;
	DBT value;

	dbt_setup(&value, buf, len);

	ret = meta_db->put(meta_db, txn, meta_dbt, &value, DB_PUT_FLAGS);

	return ret;
}

/*
 * This function is used to convert the listxattr VFS function into KV ops,
 * essentially placing the list of xattr keys in the buffer.
 *
 * It also does double-duty to query whether additional xattrs exist
 *   (i.e., when buffer is null and size is zero).
 *
 * The behavior is to only fill up to buffer with data and return -ERANGE
 * if the buffer cannot accept additional keys.
 */
int nb_bstore_xattr_list(DB *meta_db, DBT *meta_dbt, DB_TXN *txn,
			 char *buffer, size_t size, ssize_t* offset)
{
	int r, name_len;
	char *xattr_key;
	DBT xattr_dbt, value_dbt;
	DBC *cursor;
	char *name;
	// Buffer for the value
	// XXX: This would be much simpler if c_get() accepted
	//      a NULL value pointer.  Since we dont' expect xattrs
	//      to be performance critical, let's just allocate a ginormous
	//      scratch buffer for now, to keep things as simple as possible.
	char *xattr_value = sb_malloc_sized(XATTR_SIZE_MAX, false);

	// Temporary buffer for the key
	xattr_key = sb_malloc(META_KEY_MAX_LEN);
	if (xattr_key == NULL)
		return -ENOMEM;
	dbt_setup_buf(&xattr_dbt, xattr_key, META_KEY_MAX_LEN);
	copy_xattr_dbt_from_meta_dbt(&xattr_dbt, meta_dbt, "", false);

	dbt_setup(&value_dbt, xattr_value, XATTR_SIZE_MAX);
	r = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (r) {
		goto out;
	}

	// XXX: would be much cleaner if we can get a key without getting the value
	for (r = cursor->c_get(cursor, &xattr_dbt, &value_dbt, DB_SET_RANGE);
	     !r;
	     r = cursor->c_get(cursor, &xattr_dbt, &value_dbt, DB_NEXT)) {

		// If we hit a key that is not an extended attribute key for the same file,
		//  we are on the next file
		if (!key_is_xattr_of_meta_key(xattr_key, meta_dbt->data)) {
			break;
		}

		name = strrchr(nb_key_path(xattr_key), NB_BSTORE_XATTR_DELIMITER) + 1;
		name_len = strlen(name) + 1;
		// We can have a case where buffer is NULL, and we just want to know roughly how many bytes of keys
		//  we have
		if (buffer) {
			if (*offset + name_len < size) {
				memcpy(buffer + *offset, name, name_len);
			} else {
				// No more space
				r = -ERANGE;
				break;
			}
		}
		// Still update the offset even if buffer is NULL
		(*offset) += name_len;
	}

	if (r == DB_NOTFOUND) {
		r = 0;
	}

	cursor->c_close(cursor);
out:
	sb_free_sized(xattr_value, XATTR_SIZE_MAX);
	sb_free(xattr_key);
	return r;
}

int nb_bstore_xattr_move(DB *meta_db, DBT *old_meta_dbt,
			 DBT *new_meta_dbt, DB_TXN *txn)
{
	int r, ret;
	char *it_key, *new_key;
	// Just start with the max size, for simplicity.
	// We don't expect heavy or performance-critical use of xattrs in general
	size_t xattr_buf_size = XATTR_SIZE_MAX;
	char *xattr_buf = sb_malloc_sized(xattr_buf_size, false);
	DBT val_dbt, key_dbt, new_key_dbt;
	DBC *cursor;

	if (!xattr_buf) return -ENOMEM;

	// setup DBTs for move
	ret = -ENOMEM;
	it_key = new_key = NULL;
	if ((it_key = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto out;
	if ((new_key = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto free_out;

	dbt_setup_buf(&key_dbt, it_key, KEY_MAX_LEN);
	dbt_setup_buf(&new_key_dbt, new_key, KEY_MAX_LEN);
	dbt_setup_buf(&val_dbt, xattr_buf, xattr_buf_size);

	copy_xattr_dbt_from_meta_dbt(&key_dbt, old_meta_dbt, "", false);
	ret = meta_db->cursor(meta_db, txn, &cursor, DB_CURSOR_FLAGS);
	if (ret)
		goto free_out;

	r = cursor->c_get(cursor, &key_dbt, &val_dbt, DB_SET_RANGE);

	while (!r) {
		if (!key_is_xattr_of_meta_key(key_dbt.data, old_meta_dbt->data)) {
			break;
		}

		copy_xattr_dbt_movdir(old_meta_dbt, new_meta_dbt,
				     &key_dbt, &new_key_dbt);
		ret = meta_db->put(meta_db, txn, &new_key_dbt, &val_dbt,
				   DB_PUT_FLAGS);
		if (ret)
			goto freak_out;

		/* XXX: Maybe use a range delete afterward instead
		 *      of deleting one by one.
		 */
		ret = meta_db->del(meta_db, txn, &key_dbt,
				   DB_DEL_FLAGS);
		if (ret)
			goto freak_out;

		r = cursor->c_get(cursor, &key_dbt, &val_dbt,
				  DB_NEXT);
	}

	if (r && r != DB_NOTFOUND) {
		ret = r;
		goto freak_out;
	}

freak_out:
	cursor->c_close(cursor);

free_out:
	sb_free_sized(xattr_buf, xattr_buf_size);
	if (new_key)
		sb_free(new_key);
	if (it_key)
		sb_free(it_key);
out:
	return ret;
}

/* Deletes all extended attributes for given meta_dbt */
int nb_bstore_xattr_del(DB *meta_db, DBT *meta_dbt, DB_TXN *txn)
{
	int ret;
	char *min_key;
	char *max_key;
	DBT min_key_dbt, max_key_dbt;

	min_key = sb_malloc(META_KEY_MAX_LEN);
	if (min_key == NULL)
		return -ENOMEM;
	max_key = sb_malloc(META_KEY_MAX_LEN);
	if (max_key == NULL) {
		sb_free(min_key);
		return -ENOMEM;
	}

	dbt_setup_buf(&min_key_dbt, min_key, META_KEY_MAX_LEN);
	dbt_setup_buf(&max_key_dbt, max_key, META_KEY_MAX_LEN);

	copy_xattr_dbt_from_meta_dbt(&min_key_dbt, meta_dbt, "", false);
	copy_xattr_dbt_from_meta_dbt(&max_key_dbt, meta_dbt, "\xff", false);

	ret = meta_db->del_multi(meta_db, txn,
	                         &min_key_dbt,
	                         &max_key_dbt,
	                         0, 0);

	// These calls will free min_key and max_key
	dbt_destroy(&min_key_dbt);
	dbt_destroy(&max_key_dbt);
	return ret;
}
#endif /* End of FTFS_XATTR */

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

/**
 * Called in ftfs_read_page(). The returned value is of type ftfs_indirect_val.
 * The returned value is copied to buf.
 */
int ftfs_bstore_get_msg_val(DB *data_db, DBT *data_dbt, DB_TXN *txn, void *buf)
{
	int ret;
	DBT value;

	dbt_setup(&value, buf, FTFS_BSTORE_BLOCKSIZE);

	ret = data_db->get(data_db, txn, data_dbt, &value, DB_GET_FLAGS);
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

/* For page sharing read path */
struct ftfs_scan_msg_val_cb_info {
	char *meta_key;
	struct address_space *mapping;
	pgoff_t *page_offsets;
	unsigned long *pfn_arr;
	unsigned long nr_to_read;
	int index;
	int do_continue;
};

/*
 * This function is to insert a shared page into page cache.
 * this is called by the range query callback, while a read is in progress,
 * to add the pages to the page cache. The read may not be complete yet,
 * and the page will remain locked (ftfs_lock_page) until the read
 * finishes (and the page is marked up to date by SFS in the read callback.
 *
 * ftfs_lock_page is held while a page I/O is in progress, and will be
 * checked by subsequent readers to avoid duplicate read requests.
 *
 * We insert the page denoted by the PFN in the indirect value.
 * The content in the page is not necessarily valid.
 * VFS needs to wait for the data to be up-to-date.
 * Up-to-date flag will be turned on by SFS callback function.
 * Here we just insert the page to page cache
 */
static inline void ftfs_insert_shared_page(struct address_space *mapping,
		pgoff_t offset, struct page *page)
{
	struct ftfs_page_private *priv = FTFS_PAGE_PRIV(page);
	int ret;
	int g_count;
	int i_count;
	/* lock_page before we add it to page cache. page->index and
	 * page->mapping, page->_count are changed when being added to
	 * page cache. The lock is to protect them.
	 * Also, page->priv, page->flag are changed in this function.
	 * Use lock_page to protect them as well.
	 */
	lock_page(page);
	/*
	 * This function is used to add a page to the pagecache.
	 * It must be locked.
	 */
	ret = add_to_page_cache_locked(page, mapping, offset, GFP_KERNEL);
	BUG_ON(ret);

	g_count = ftfs_read_page_count(page);
	i_count = atomic_read(&priv->_count);
	/*
	 * Sanity Check: the page count should be 2: one from page cache
	 * and one from the ft tree
	 */
	if (g_count != 2) {
		printk("%s: page->_count=%d\n", __func__, g_count);
		BUG();
	}
	/*
	 * Sanity Check: the internal count should be 1 or 2 unless
	 * something unexpected happend, which we need to understand.
	 */
	if (i_count != 2 && i_count != 1) {
		printk("%s: priv->_count=%d\n", __func__, i_count);
		BUG();
	}
	SetPagePrivate(page);
	ftfs_set_page_private(page_to_pfn(page), FT_MSG_VAL_NB_READ);
	unlock_page(page);
}

/*
 * During a range query, this callback is called on
 * every key/value pair, passed in as an argument
 * along with an extra (pointer to a ftfs_cb_scan_info).
 * This is used to either copy the values to a buffer,
 * or insert them into the page cache.
 *
 * For indirect blocks, the read may not have finished and
 * may be ongoing when this is called. The uptodate flag
 * on the page is set when the read is finished.
 * FTFS_READAHEAD_INDEX_SKIP is used to indicate that this page
 * is not needed because it is already in cache.
 */
static int ftfs_scan_msg_val_cb(DBT const *key, DBT const *val, void *extra)
{
	char *data_key = key->data;
	struct ftfs_scan_msg_val_cb_info *info = extra;
	pgoff_t *page_offsets = info->page_offsets;
	struct ftfs_indirect_val *msg_val = val->data;
	int i = info->index;
	struct page *page;
	pgoff_t key_blk_num;
	int ret;
	/* Test data_key belongs to the file denoted by the meta_key.
	 * Note that meta_key always ends with 0x00,
	 * data_key = meta_key :: blk_id.
	 * We can use strcmp to test whether a data_key belongs to
	 * a file.
	 * If the data_key does not belong to the file we are
	 * accessing now, we set do_continue to false.
	 */
	if (!key_is_same_of_key(data_key, info->meta_key)) {
		info->do_continue = 0;
		return 0;
	}

	key_blk_num = nb_data_key_get_blocknum(data_key, key->size);
	/* If the page read from ft layer has index smaller
	 * than the one needed by page cache, we should
	 * continue the cursor reading. Some pages in
	 * the requested ranges may be already in page cache.
	 * we should skip them.
	 */
	if (key_blk_num - 1 < page_offsets[i]) {
		info->do_continue = i < info->nr_to_read;
	} else if (i < info->nr_to_read && page_offsets[i] == key_blk_num - 1) {
		if (val->flags != DB_DBT_INDIRECT) {
			char *buf;
			page = __ftfs_page_cache_alloc(FTFS_READ_PAGE_GFP);
			preempt_disable();
			buf = kmap_atomic(page);
			memcpy(buf, val->data, val->size);
			kunmap_atomic(buf);
			preempt_enable();
			/*
			 * We copied the data to this newly-allocated page. The data
			 * must be valid. We need to set it to up-to-date.
			 * Also, we don't need to call lock_page because it is just
			 * allocated here and no one is referencing it.
			*/
			SetPageUptodate(page);
			/* Insert this newly allocated page to page cache */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
			__SetPageLocked(page);
#else /* LINUX_VERSION_CODE */
			__set_page_locked(page);
#endif
			ret = add_to_page_cache_locked(page, info->mapping, page_offsets[i], GFP_KERNEL);
			BUG_ON(ret);
			put_page(page);
			unlock_page(page);
		} else {
			BUG_ON(!pfn_valid(msg_val->pfn));
			page = pfn_to_page(msg_val->pfn);
			ftfs_insert_shared_page(info->mapping, page_offsets[i], page);
#ifdef FT_INDIRECT_DEBUG
			{
				char *buf = kmap(page);
				pgoff_t stored_idx;
				memcpy(&stored_idx, buf, sizeof(stored_idx));
				if (stored_idx != page->index) {
					printk("stored_idx=%lu, page->index=%lu\n",
						stored_idx, page->index);
					BUG();
				}
				kunmap(page);
			}
#endif
		}
		/*
		 * This is returned to the caller of the range query with
		 * all of the page frame number.
		 */
		info->pfn_arr[i] = page_to_pfn(page);
		/* move forward the index of page offset */
		i++;
		/* If we are skipping a page in the file, we set
		 * that entry in page_offsets to FTFS_READAHEAD_INDEX_SKIP (not zero).
		 * info->index and info>do_continue indicate that
		 * we have not finished the range. The range query
		 * callback is responsible to indicate that it has
		 * seen the last thing in the range.
		 *
		 * Make sure the ith is not zero.
		*/
		while (i < info->nr_to_read && info->page_offsets[i] == FTFS_READAHEAD_INDEX_SKIP)
			i++;
		info->index = i;
		info->do_continue = i < info->nr_to_read;
	}
	return 0;
}

/* return the number of msg_val read from ft code.
 * This is in support of read-ahead. A range query is issued here
 * and this function fills individual pages from the range query
 * results (calling next in this function)
 *
 * The page offsets are typically contiguous, but there can be
 * individual pages already in cache that should be skipped.
 */
int ftfs_bstore_scan_msg_vals(DB *data_db, DBT *meta_dbt, DB_TXN *txn,
		pgoff_t *page_offsets, unsigned long *pfn_arr,
		unsigned long nr_to_read, struct address_space *mapping,
		unsigned long last_offset)
{
	int ret, r;
	struct ftfs_scan_msg_val_cb_info info;
	DBT data_dbt;
	DBC *cursor;
	int i = 0;
	unsigned long curr_offset;
	unsigned int cursor_flags;

	i = 0;
	while (i < nr_to_read && page_offsets[i] == FTFS_READAHEAD_INDEX_SKIP)
		i++;
	if (i == nr_to_read) {
		return 0;
	}

	// Step 1: Seq read detection
	curr_offset = page_offsets[i];
	if ((curr_offset == 0 && last_offset == ULONG_MAX) || curr_offset == last_offset + 1) {
		cursor_flags = DB_SEQ_READ;
	} else {
		cursor_flags = DB_CURSOR_FLAGS;
	}

	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt, page_offsets[i]+1);
	if (ret)
		return ret;

	// Step 2: Issue a range query
	ret = data_db->cursor(data_db, txn, &cursor, cursor_flags);
	if (ret)
		goto free_out;

	info.mapping = mapping;
	info.meta_key = meta_dbt->data;
	info.page_offsets = page_offsets;
	info.pfn_arr = pfn_arr;
	info.nr_to_read = nr_to_read;
	info.index = i;

	r = cursor->c_getf_set_range(cursor, 0, &data_dbt, ftfs_scan_msg_val_cb, &info);
	while (info.do_continue && !r) {
		r = cursor->c_getf_next(cursor, 0, ftfs_scan_msg_val_cb, &info);
	}
	if (r && r != DB_NOTFOUND)
		ret = r;

	r = cursor->c_close(cursor);
	BUG_ON(r);
free_out:
	dbt_destroy(&data_dbt);
	return ret;
}

/* Use db->seq_put to insert indirect value to the tree */
int nb_bstore_put_indirect_page(DB *data_db, DBT *data_dbt, DB_TXN *txn,
                    struct ftfs_indirect_val *msg_val)
{
	int ret;
	DBT value;

	dbt_setup(&value, msg_val, sizeof(*msg_val));

	ret = data_db->seq_put(data_db, txn, data_dbt, &value, DB_PUT_FLAGS);

	return ret;
}
/* Page sharing code end */

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
	dbt_destroy(&max_data_key_dbt);
	dbt_destroy(&min_data_key_dbt);

	if (!ret && offset) {
		lastblocksize = FTFS_BSTORE_BLOCKSIZE - offset;
		info_size = sizeof(struct block_update_cb_info) + lastblocksize;
		info = sb_malloc(info_size);
		if (!info){
		    return -ENOMEM;
		}
		ret = alloc_data_dbt_from_meta_dbt(&last_data_key_dbt, meta_dbt, new_num);
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

	ret = alloc_data_dbt_from_meta_dbt(&min_data_key_dbt, meta_dbt, 0);
	if (ret) goto out;

	copy_subtree_max_data_dbt_from_meta_dbt(&max_data_key_dbt, meta_dbt);

	ret = data_db->del_multi(data_db, txn,
	                         &min_data_key_dbt,
	                         &max_data_key_dbt,
	                         0, 0);
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
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt, PAGE_TO_BLOCK_NUM(page));
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
			preempt_disable();
			page_buf = kmap_atomic(page);
			memset(page_buf, 0, PAGE_SIZE);
			kunmap_atomic(page_buf);
			preempt_enable();

			ftio_advance_page(ftio);
			if (ftio_job_done(ftio))
				break;
			page = ftio_current_page(ftio);
			page_block_num = PAGE_TO_BLOCK_NUM(page);
		}

		if (page_block_num == nb_data_key_get_blocknum(data_key, key->size)) {
			preempt_disable();
			page_buf = kmap_atomic(page);
			if (val->size)
				memcpy(page_buf, val->data, val->size);
			if (val->size < PAGE_SIZE)
				memset(page_buf + val->size, 0,
				       PAGE_SIZE - val->size);
			kunmap_atomic(page_buf);
			preempt_enable();
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
		preempt_disable();
		page_buf = kmap_atomic(page);
		memset(page_buf, 0, PAGE_SIZE);
		kunmap_atomic(page_buf);
		preempt_enable();
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
	ret = alloc_data_dbt_from_meta_dbt(&data_dbt, meta_dbt,
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

#ifdef FTFS_EMPTY_DIR_VERIFY
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

	ret = alloc_child_meta_dbt_from_meta_dbt(&start_meta_dbt, meta_dbt, "");
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
#endif

static int
nb_bstore_move_copy(DB *meta_db, DB *data_db, DBT *old_meta_dbt,
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
	if ((it_key[0] = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto out;
	if ((it_key[1] = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto free_out;
	if ((new_key = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto free_out;
	if ((block_buf = sb_malloc(FTFS_BSTORE_BLOCKSIZE)) == NULL)
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
		sb_free(block_buf);
	if (new_key)
		sb_free(new_key);
	if (it_key[1])
		sb_free(it_key[1]);
	if (it_key[0])
		sb_free(it_key[0]);
out:
	return ret;
}

/*
 * nb_bstore_move_express():
 * This function uses clone to move (rename) file with
 * size larger than FTFS_BSTORE_MOVE_LARGE_THRESHOLD
 */
static int
nb_bstore_move_express(DB *meta_db, DB *data_db, DBT *old_meta_dbt,
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
	if ((min_key_buf = sb_malloc(KEY_MAX_LEN)) == NULL)
		goto out;
	if ((max_key_buf = sb_malloc(KEY_MAX_LEN)) == NULL)
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
		// posterity is bound by (src\x01\x01, src\x1\xff)
		copy_child_meta_dbt_from_meta_dbt(&min_key_dbt,
			old_meta_dbt, "");
		copy_subtree_max_meta_dbt_from_meta_dbt(&max_key_dbt,
			old_meta_dbt);
#ifdef FTFS_SMART_PIVOT
		ret = meta_db->clone(meta_db, txn, &min_key_dbt, &max_key_dbt,
		                     &old_prefix_dbt, &new_prefix_dbt,
		                     DB_CLONE_FLAGS);
		if (!ret) {
			ret = meta_db->del_multi(meta_db, txn,
			                         &min_key_dbt, &max_key_dbt,
			                         0, 0);
		}
#else /* FTFS_SMART_PIVOT */
		ret = meta_db->rename(meta_db, txn, &min_key_dbt, &max_key_dbt,
		                      &old_prefix_dbt, &new_prefix_dbt,
		                      DB_RENAME_FLAGS);
#endif /* FTFS_SMART_PIVOT */

		if (ret)
			goto free_out;

		// we can ignore blocknum here
		copy_child_data_dbt_from_meta_dbt(&min_key_dbt, old_meta_dbt,
		                                  "", 0);
		copy_subtree_max_data_dbt_from_meta_dbt(&max_key_dbt,
		                                        old_meta_dbt);
#ifdef FTFS_SMART_PIVOT
		ret = data_db->clone(data_db, txn, &min_key_dbt, &max_key_dbt,
		                     &old_prefix_dbt, &new_prefix_dbt,
		                     DB_CLONE_FLAGS);
		if (!ret) {
			ret = data_db->del_multi(data_db, txn,
			                         &min_key_dbt, &max_key_dbt,
			                         0, 0);
		}
#else /* FTFS_SMART_PIVOT */
		ret = data_db->rename(data_db, txn, &min_key_dbt, &max_key_dbt,
		                      &old_prefix_dbt, &new_prefix_dbt,
		                      DB_RENAME_FLAGS);
#endif /* FTFS_SMART_PIVOT */
	} else {
		copy_data_dbt_from_meta_dbt(&min_key_dbt, old_meta_dbt, 0);
		copy_data_dbt_from_meta_dbt(&max_key_dbt, old_meta_dbt,
		                            FTFS_UINT64_MAX);
#ifdef FTFS_SMART_PIVOT
		ret = data_db->clone(data_db, txn, &min_key_dbt, &max_key_dbt,
		                     old_meta_dbt, new_meta_dbt,
		                     DB_CLONE_FLAGS);
		if (!ret) {
			ret = data_db->del_multi(data_db, txn,
			                         &min_key_dbt, &max_key_dbt,
			                         0, 0);
		}
#else
		ret = data_db->rename(data_db, txn, &min_key_dbt, &max_key_dbt,
		                      old_meta_dbt, new_meta_dbt,
		                      DB_RENAME_FLAGS);
#endif /* FTFS_SMART_PIVOT */
	}

free_out:
	dbt_destroy(&new_prefix_dbt);
	dbt_destroy(&old_prefix_dbt);
	if (max_key_buf)
		sb_free(max_key_buf);
	if (min_key_buf)
		sb_free(min_key_buf);
out:
	return ret;
}


/*
 * nb_bstore_move()
 * The caller of this function is nb_rename(),
 * for command mv -- move (rename) files
 */
int
nb_bstore_move(DB *meta_db, DB *data_db, DBT *old_meta_dbt, DBT *new_meta_dbt,
	       DB_TXN *txn, enum ftfs_bstore_move_type type)
{
	if (type == FTFS_BSTORE_MOVE_SMALL_FILE)
		return nb_bstore_move_copy(meta_db, data_db, old_meta_dbt,
                                     new_meta_dbt, txn, type);
	return nb_bstore_move_express(meta_db, data_db, old_meta_dbt,
	                                new_meta_dbt, txn, type);
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
