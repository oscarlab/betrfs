/*
 * tmem.h
 *
 * Transcendent memory
 *
 * Copyright (c) 2009-2012, Dan Magenheimer, Oracle Corp.
 */

#ifndef _TMEM_H_
#define _TMEM_H_

#include <linux/types.h>
#include <linux/highmem.h>
#include <linux/hash.h>
#include <linux/atomic.h>

/*
 * These are defined by the Xen<->Linux ABI so should remain consistent
 */
#define TMEM_POOL_PERSIST		1
#define TMEM_POOL_SHARED		2
#define TMEM_POOL_PRECOMPRESSED		4
#define TMEM_POOL_PAGESIZE_SHIFT	4
#define TMEM_POOL_PAGESIZE_MASK		0xf
#define TMEM_POOL_RESERVED_BITS		0x00ffff00

/*
 * sentinels have proven very useful for debugging but can be removed
 * or disabled before final merge.
 */
#undef SENTINELS
#ifdef SENTINELS
#define DECL_SENTINEL uint32_t sentinel;
#define SET_SENTINEL(_x, _y) (_x->sentinel = _y##_SENTINEL)
#define INVERT_SENTINEL(_x, _y) (_x->sentinel = ~_y##_SENTINEL)
#define ASSERT_SENTINEL(_x, _y) WARN_ON(_x->sentinel != _y##_SENTINEL)
#define ASSERT_INVERTED_SENTINEL(_x, _y) WARN_ON(_x->sentinel != ~_y##_SENTINEL)
#else
#define DECL_SENTINEL
#define SET_SENTINEL(_x, _y) do { } while (0)
#define INVERT_SENTINEL(_x, _y) do { } while (0)
#define ASSERT_SENTINEL(_x, _y) do { } while (0)
#define ASSERT_INVERTED_SENTINEL(_x, _y) do { } while (0)
#endif

#define ASSERT_SPINLOCK(_l)	lockdep_assert_held(_l)

/*
 * A pool is the highest-level data structure managed by tmem and
 * usually corresponds to a large independent set of pages such as
 * a filesystem.  Each pool has an id, and certain attributes and counters.
 * It also contains a set of hash buckets, each of which contains an rbtree
 * of objects and a lock to manage concurrency within the pool.
 */

#define TMEM_HASH_BUCKET_BITS	8
#define TMEM_HASH_BUCKETS	(1<<TMEM_HASH_BUCKET_BITS)

struct tmem_hashbucket {
	struct rb_root obj_rb_root;
	spinlock_t lock;
};

struct tmem_pool {
	void *client; /* "up" for some clients, avoids table lookup */
	struct list_head pool_list;
	uint32_t pool_id;
	bool persistent;
	bool shared;
	atomic_t obj_count;
	atomic_t refcount;
	struct tmem_hashbucket hashbucket[TMEM_HASH_BUCKETS];
	DECL_SENTINEL
};

#define is_persistent(_p)  (_p->persistent)
#define is_ephemeral(_p)   (!(_p->persistent))

/*
 * An object id ("oid") is large: 192-bits (to ensure, for example, files
 * in a modern filesystem can be uniquely identified).
 */

struct tmem_oid {
	uint64_t oid[3];
};

static inline void tmem_oid_set_invalid(struct tmem_oid *oidp)
{
	oidp->oid[0] = oidp->oid[1] = oidp->oid[2] = -1UL;
}

static inline bool tmem_oid_valid(struct tmem_oid *oidp)
{
	return oidp->oid[0] != -1UL || oidp->oid[1] != -1UL ||
		oidp->oid[2] != -1UL;
}

static inline int tmem_oid_compare(struct tmem_oid *left,
					struct tmem_oid *right)
{
	int ret;

	if (left->oid[2] == right->oid[2]) {
		if (left->oid[1] == right->oid[1]) {
			if (left->oid[0] == right->oid[0])
				ret = 0;
			else if (left->oid[0] < right->oid[0])
				ret = -1;
			else
				return 1;
		} else if (left->oid[1] < right->oid[1])
			ret = -1;
		else
			ret = 1;
	} else if (left->oid[2] < right->oid[2])
		ret = -1;
	else
		ret = 1;
	return ret;
}

static inline unsigned tmem_oid_hash(struct tmem_oid *oidp)
{
	return hash_long(oidp->oid[0] ^ oidp->oid[1] ^ oidp->oid[2],
				TMEM_HASH_BUCKET_BITS);
}

#if defined(CONFIG_RAMSTER) || defined(CONFIG_RAMSTER_MODULE)
struct tmem_xhandle {
	uint8_t client_id;
	uint8_t xh_data_cksum;
	uint16_t xh_data_size;
	uint16_t pool_id;
	struct tmem_oid oid;
	uint32_t index;
	void *extra;
};

static inline struct tmem_xhandle tmem_xhandle_fill(uint16_t client_id,
					struct tmem_pool *pool,
					struct tmem_oid *oidp,
					uint32_t index)
{
	struct tmem_xhandle xh;
	xh.client_id = client_id;
	xh.xh_data_cksum = (uint8_t)-1;
	xh.xh_data_size = (uint16_t)-1;
	xh.pool_id = pool->pool_id;
	xh.oid = *oidp;
	xh.index = index;
	return xh;
}
#endif


/*
 * A tmem_obj contains an identifier (oid), pointers to the parent
 * pool and the rb_tree to which it belongs, counters, and an ordered
 * set of pampds, structured in a radix-tree-like tree.  The intermediate
 * nodes of the tree are called tmem_objnodes.
 */

struct tmem_objnode;

struct tmem_obj {
	struct tmem_oid oid;
	struct tmem_pool *pool;
	struct rb_node rb_tree_node;
	struct tmem_objnode *objnode_tree_root;
	unsigned int objnode_tree_height;
	unsigned long objnode_count;
	long pampd_count;
#if defined(CONFIG_RAMSTER) || defined(CONFIG_RAMSTER_MODULE)
	/*
	 * for current design of ramster, all pages belonging to
	 * an object reside on the same remotenode and extra is
	 * used to record the number of the remotenode so a
	 * flush-object operation can specify it
	 */
	void *extra; /* for private use by pampd implementation */
#endif
	DECL_SENTINEL
};

#define OBJNODE_TREE_MAP_SHIFT 6
#define OBJNODE_TREE_MAP_SIZE (1UL << OBJNODE_TREE_MAP_SHIFT)
#define OBJNODE_TREE_MAP_MASK (OBJNODE_TREE_MAP_SIZE-1)
#define OBJNODE_TREE_INDEX_BITS (8 /* CHAR_BIT */ * sizeof(unsigned long))
#define OBJNODE_TREE_MAX_PATH \
		(OBJNODE_TREE_INDEX_BITS/OBJNODE_TREE_MAP_SHIFT + 2)

struct tmem_objnode {
	struct tmem_obj *obj;
	DECL_SENTINEL
	void *slots[OBJNODE_TREE_MAP_SIZE];
	unsigned int slots_in_use;
};

struct tmem_handle {
	struct tmem_oid oid; /* 24 bytes */
	uint32_t index;
	uint16_t pool_id;
	uint16_t client_id;
};


/* pampd abstract datatype methods provided by the PAM implementation */
struct tmem_pamops {
	void (*create_finish)(void *, bool);
	int (*get_data)(char *, size_t *, bool, void *, struct tmem_pool *,
				struct tmem_oid *, uint32_t);
	int (*get_data_and_free)(char *, size_t *, bool, void *,
				struct tmem_pool *, struct tmem_oid *,
				uint32_t);
	void (*free)(void *, struct tmem_pool *,
				struct tmem_oid *, uint32_t, bool);
#if defined(CONFIG_RAMSTER) || defined(CONFIG_RAMSTER_MODULE)
	void (*new_obj)(struct tmem_obj *);
	void (*free_obj)(struct tmem_pool *, struct tmem_obj *, bool);
	void *(*repatriate_preload)(void *, struct tmem_pool *,
					struct tmem_oid *, uint32_t, bool *);
	int (*repatriate)(void *, void *, struct tmem_pool *,
				struct tmem_oid *, uint32_t, bool, void *);
	bool (*is_remote)(void *);
	int (*replace_in_obj)(void *, struct tmem_obj *);
#endif
};
extern void tmem_register_pamops(struct tmem_pamops *m);

/* memory allocation methods provided by the host implementation */
struct tmem_hostops {
	struct tmem_obj *(*obj_alloc)(struct tmem_pool *);
	void (*obj_free)(struct tmem_obj *, struct tmem_pool *);
	struct tmem_objnode *(*objnode_alloc)(struct tmem_pool *);
	void (*objnode_free)(struct tmem_objnode *, struct tmem_pool *);
};
extern void tmem_register_hostops(struct tmem_hostops *m);

/* core tmem accessor functions */
extern int tmem_put(struct tmem_pool *, struct tmem_oid *, uint32_t index,
			bool, void *);
extern int tmem_get(struct tmem_pool *, struct tmem_oid *, uint32_t index,
			char *, size_t *, bool, int);
extern int tmem_flush_page(struct tmem_pool *, struct tmem_oid *,
			uint32_t index);
extern int tmem_flush_object(struct tmem_pool *, struct tmem_oid *);
extern int tmem_destroy_pool(struct tmem_pool *);
extern void tmem_new_pool(struct tmem_pool *, uint32_t);
#if defined(CONFIG_RAMSTER) || defined(CONFIG_RAMSTER_MODULE)
extern int tmem_replace(struct tmem_pool *, struct tmem_oid *, uint32_t index,
			void *);
extern void *tmem_localify_get_pampd(struct tmem_pool *, struct tmem_oid *,
				   uint32_t index, struct tmem_obj **,
				   void **);
extern void tmem_localify_finish(struct tmem_obj *, uint32_t index,
				 void *, void *, bool);
#endif
#endif /* _TMEM_H */
