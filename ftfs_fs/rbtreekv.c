/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef RBTREE_KV_H
#define RBTREE_KV_H

#include "tokudb.h"
#include "ftfs_fs.h"

/*
 * Create one db as a rbtree. One db_env has a list of rbtrees, each
 * has its own rbtree struct;
 */
struct rb_kv_node {
	DBT key, val;
	struct rb_node node;
};
struct __toku_db_internal {
	/* maybe we need a point back to db here */
	struct rb_root kv;
	struct list_head rbtree_list;
};
struct __toku_db_env_internal {
	struct list_head rbtree_list;
	int (*update_cb)(DB *, const DBT *key, const DBT *old_val, const DBT *extra, void (*set_val)(const DBT *new_val, void *set_extra), void *set_extra);
	int (*bt_compare)(DB *, const DBT *, const DBT *);
};
/*
 * __toku_dbc doesn't offer us anything like xx_internal to store our own data,
 * thus I try to wrap things into bigger data structure
 */
struct __toku_dbc_wrap {
	struct rb_kv_node *node;
	DBT *left, *right;
	struct __toku_dbc dbc;
};

static int dbt_alloc_and_copy(DBT **to, const DBT *from)
{
	*to = kmalloc(sizeof(DBT), GFP_KERNEL);
	if (*to == NULL) {
		return -ENOMEM;
	}
	memcpy(*to, from, sizeof(DBT));
	(*to)->data = kmalloc(from->size, GFP_KERNEL);
	if ((*to)->data == NULL) {
		kfree(*to);
		*to = NULL;
		return -ENOMEM;
	}
	memcpy((*to)->data, from->data, from->size);
	return 0;
}
static int dbt_copy(DBT *to, const DBT *from) {
	memcpy(to, from, sizeof(DBT));
	to->data = kmalloc(from->size, GFP_KERNEL);
	if (to->data == NULL) {
		return -ENOMEM;
	}
	memcpy(to->data, from->data, from->size);
	return 0;
}

static int db_env_set_cachesize(DB_ENV *env, uint32_t gb, uint32_t b, int ncache)
{
	return 0;
}

static void db_env_set_update(DB_ENV *env, int (*update_function)(DB *, const DBT *key, const DBT *old_val, const DBT *extra, void (*set_val)(const DBT *new_val, void *set_extra), void *set_extra))
{
	env->i->update_cb = update_function;

	return;
}

static int db_env_set_default_bt_compare(DB_ENV *env, int (*bt_compare)(DB *, const DBT *, const DBT *))
{
	env->i->bt_compare = bt_compare;

	return 0;
}

static int db_env_open(DB_ENV *env, const char *db_home, uint32_t flags, int mode)
{
	return 0;
}

static int db_env_close(DB_ENV *env, uint32_t flag)
{
	return 0;
}

static int db_open(DB *db, DB_TXN *txnid, const char *file, const char *database, DBTYPE type, uint32_t flags, int mode)
{
	return 0;
}

static struct rb_kv_node *find_val_with_key(DB *db, const DBT *key)
{
	struct rb_node *node = db->i->kv.rb_node;

	while (node) {
		struct rb_kv_node *tmp = container_of(node, struct rb_kv_node, node);
		int result;

		result = db->dbenv->i->bt_compare(db, key, &(tmp->key));

		if (result < 0)
			node = node->rb_left;
		else if (result > 0)
			node = node->rb_right;
		else {
			return tmp;
		}
	}

	return NULL;
}

static struct rb_kv_node *find_val_with_key_ge(DB *db, const DBT *key)
{
	struct rb_node *node = db->i->kv.rb_node;

	while (node) {
		struct rb_kv_node *tmp = container_of(node, struct rb_kv_node, node);
		int result;

		result = db->dbenv->i->bt_compare(db, key, &(tmp->key));
		if (result < 0) {
			/* node key is bigger */
			struct rb_kv_node *next = tmp;
			while ((node = rb_prev(node)) != NULL) {
				next = tmp;
				tmp = container_of(node, struct rb_kv_node, node);
				result = db->dbenv->i->bt_compare(db, key, &(tmp->key));
				if (result == 0) {
					return tmp;
				} else if (result > 0) {
					return next;
				}
			}
			return tmp;
		}
		else if (result > 0)
			node = node->rb_right;
		else {
			return tmp;
		}
	}

	return NULL;
}

static int db_get(DB *db, DB_TXN *txnid, DBT *key, DBT *data, uint32_t flags)
{
	struct rb_kv_node *node = find_val_with_key(db, key);
	if (node == NULL) {
		return DB_NOTFOUND;
	}
	memcpy(data->data, node->val.data, data->size);

	return 0;
}

static int db_del(DB *db, DB_TXN *txnid, DBT *key, uint32_t flags)
{
	struct rb_kv_node *node = find_val_with_key(db, key);
	if (node == NULL) {
		return DB_NOTFOUND;
	}
	rb_erase(&node->node, &db->i->kv);
	kfree(node->key.data);
	kfree(node->val.data);
	kfree(node);

	return 0;
}

struct set_val_info {
	DB *db;
	const DBT *key;
	struct rb_kv_node *node;
};

static int rb_kv_insert(DB *db, struct rb_kv_node *node)
{
	struct rb_node **new = &(db->i->kv.rb_node), *parent = NULL;

	while (*new) {
		struct rb_kv_node *this = container_of(*new, struct rb_kv_node, node);
		int result = db->dbenv->i->bt_compare(db, &node->key, &this->key);
		parent = *new;
		if (result < 0)
			new = &((*new)->rb_left);
		else if (result > 0)
			new = &((*new)->rb_right);
		else
			return -1;
	}

	rb_link_node(&node->node, parent, new);
	rb_insert_color(&node->node, &(db->i->kv));

	return 0;
}

static void db_set_val(const DBT *new_val, void *set_extra)
{
	struct set_val_info *info = set_extra;

	if (new_val == NULL) {
		/* delete */
		BUG_ON(info->node == NULL);
		rb_erase(&info->node->node, &info->db->i->kv);
		kfree(info->node->key.data);
		kfree(info->node->val.data);
		kfree(info->node);
	} else {
		/* replace/insert */
		if (info->node == NULL) {
			int ret;
			struct rb_kv_node *new_node = kmalloc(sizeof(struct rb_kv_node), GFP_KERNEL);
			BUG_ON(new_node == NULL);
			ret = dbt_copy(&new_node->key, info->key);
			BUG_ON(ret != 0);
			ret = dbt_copy(&new_node->val, new_val);
			BUG_ON(ret != 0);
			ret = rb_kv_insert(info->db, new_node);
			BUG_ON(ret != 0);
		} else {
			int ret;
			kfree(info->node->val.data);
			ret = dbt_copy(&info->node->val, new_val);
			BUG_ON(ret != 0);
		}
	}
}

static int db_update(DB *db, DB_TXN *txnid, const DBT *key, const DBT *extra, uint32_t flags)
{
	struct rb_kv_node *node = find_val_with_key(db, key);
	struct set_val_info info;
	int ret;
	DBT *val = (node == NULL) ? NULL : &node->val;

	info.db = db;
	info.key = key;
	info.node = node;
	ret = db->dbenv->i->update_cb(db, key, val, extra, db_set_val, &info);

	return ret;
}

static void free_rb_tree(struct rb_node *node)
{
	struct rb_kv_node *kv_node = container_of(node, struct rb_kv_node, node);
	if (!node)
		return;
	if (node->rb_left)
	  free_rb_tree(node->rb_left);
	if (node->rb_right)
	  free_rb_tree(node->rb_right);
	kfree(kv_node->key.data);
	kfree(kv_node->val.data);
	kfree(kv_node);
}

static int db_close(DB *db, uint32_t flag)
{
	free_rb_tree(db->i->kv.rb_node);
	kfree(db->i);
	kfree(db);

	return 0;
}

static int db_put(DB *db, DB_TXN *txnid, DBT *key, DBT *data, uint32_t flags)
{
	/* flags are not used in ftfs */
	struct rb_kv_node *node;
	int ret;

	node = find_val_with_key(db, key);
	if (node != NULL) {
		kfree(node->val.data);
		ret = dbt_copy(&node->val, data);
		BUG_ON(ret != 0);
		return ret;
	}

	node = kmalloc(sizeof(struct rb_kv_node), GFP_KERNEL);

	ret = dbt_copy(&node->key, key);
	BUG_ON(ret != 0);
	ret = dbt_copy(&node->val, data);
	BUG_ON(ret != 0);
	ret = rb_kv_insert(db, node);
	BUG_ON(ret != 0);

	return ret;
}

static int dbc_c_getf_set_range(DBC *c, uint32_t flag, DBT *key, YDB_CALLBACK_FUNCTION f, void *extra)
{
	/* all things in ftfs use flag = 0, ignore it */
	struct __toku_dbc_wrap *wrap;
	struct rb_kv_node *node = find_val_with_key_ge(c->dbp, key);
	if (node == NULL) {
		return DB_NOTFOUND;
	}
	wrap = container_of(c, struct __toku_dbc_wrap, dbc);
	wrap->node = node;
	f(&node->key, &node->val, extra);

	return 0;
}

static int dbc_c_getf_next(DBC *c, uint32_t flag, YDB_CALLBACK_FUNCTION f, void *extra)
{
	/* again, flag is ignored */
	struct __toku_dbc_wrap *wrap = container_of(c, struct __toku_dbc_wrap, dbc);
	struct rb_node *node= &(wrap->node->node);
	node = rb_next(node); /* logical next */
	if (node == NULL) {
		wrap->node = NULL;
		return DB_NOTFOUND;
	}
	/* since we only consider right bound in ftfs, simply do this */
	if (wrap->right != NULL) {
		struct rb_kv_node *kv_node = container_of(node, struct rb_kv_node, node);
		if (c->dbp->dbenv->i->bt_compare(c->dbp, &kv_node->key, wrap->right) > 0) {
			wrap->node = NULL;
			return DB_NOTFOUND;
		}
	}
	wrap->node = container_of(node, struct rb_kv_node, node);
	f(&wrap->node->key, &wrap->node->val, extra);

	return 0;
}

static void free_dbt(DBT *dbt) {
	kfree(dbt->data);
	kfree(dbt);
}

static int dbc_c_close(DBC *c) {
	struct __toku_dbc_wrap *wrap = container_of(c, struct __toku_dbc_wrap, dbc);
	if (wrap->left) {
		free_dbt(wrap->left);
	}
	if (wrap->right) {
		free_dbt(wrap->right);
	}
	kfree(wrap);
	return 0;
}

static int dbc_c_set_bounds(DBC *dbc, const DBT *left_key, const DBT *right_key, bool pre_acquire, int out_of_range_error)
{
	struct __toku_dbc_wrap *wrap = container_of(dbc, struct __toku_dbc_wrap, dbc);
	int ret;

	ret = dbt_alloc_and_copy(&wrap->left, left_key);
	BUG_ON(ret != 0);
	ret = dbt_alloc_and_copy(&wrap->right, right_key);
	BUG_ON(ret != 0);

	return 0;
}

static int dbc_c_getf_current(DBC *c, uint32_t flag, YDB_CALLBACK_FUNCTION f, void *extra)
{
	int r;
	struct __toku_dbc_wrap *wrap = container_of(c, struct __toku_dbc_wrap, dbc);

	BUG_ON(wrap->node == NULL);
	r= f(&wrap->node->key, &wrap->node->val, extra);

	return r;
}

static int db_cursor(DB *db, DB_TXN *txnid, DBC **cursorp, uint32_t flags)
{
	struct __toku_dbc_wrap *wrap = kmalloc(sizeof(struct __toku_dbc_wrap), GFP_KERNEL);
	if (wrap == NULL) {
		return -ENOMEM;
	}

	wrap->node = NULL;
	*cursorp = &wrap->dbc;
	wrap->left = NULL;
	wrap->right = NULL;

	(*cursorp)->dbp = db;
	(*cursorp)->c_getf_set_range = dbc_c_getf_set_range;
	(*cursorp)->c_getf_next = dbc_c_getf_next;
	(*cursorp)->c_close = dbc_c_close;
	(*cursorp)->c_set_bounds = dbc_c_set_bounds;
	(*cursorp)->c_getf_current = dbc_c_getf_current;

	return 0;
}

int db_env_create(DB_ENV **envp, uint32_t flags)
{
	*envp = kmalloc(sizeof(DB_ENV), GFP_KERNEL);
	if (*envp == NULL) {
		return -ENOMEM;
	}
	(*envp)->i = kmalloc(sizeof(struct __toku_db_env_internal), GFP_KERNEL);
	if ((*envp)->i == NULL) {
		kfree(*envp);
		return -ENOMEM;
	}
	INIT_LIST_HEAD(&((*envp)->i->rbtree_list));
	/* redirect some operations in use */
	(*envp)->set_cachesize = db_env_set_cachesize;
	(*envp)->set_update = db_env_set_update;
	(*envp)->set_default_bt_compare = db_env_set_default_bt_compare;
	(*envp)->open = db_env_open;
	(*envp)->close = db_env_close;

	return 0;
}

int db_create(DB **db, DB_ENV *env, uint32_t flags)
{
	*db = kmalloc(sizeof(DB), GFP_KERNEL);
	if (*db == NULL) {
		return -ENOMEM;
	}
	(*db)->i = kmalloc(sizeof(struct __toku_db_internal), GFP_KERNEL);
	if ((*db)->i == NULL) {
		kfree(*db);
		return -ENOMEM;
	}
	INIT_LIST_HEAD(&(*db)->i->rbtree_list);
	list_add(&(*db)->i->rbtree_list, &env->i->rbtree_list);
	(*db)->i->kv = RB_ROOT;
	(*db)->dbenv = env;

	(*db)->open = db_open;
	(*db)->get = db_get;
	(*db)->update = db_update;
	(*db)->cursor = db_cursor;
	(*db)->close = db_close;
	(*db)->put = db_put;
	(*db)->del = db_del;

	return 0;
}

#endif /* RBTREE_KV_H */
