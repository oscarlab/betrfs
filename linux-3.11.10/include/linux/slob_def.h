#ifndef __LINUX_SLOB_DEF_H
#define __LINUX_SLOB_DEF_H

#include <linux/numa.h>

void *kmem_cache_alloc_node(struct kmem_cache *, gfp_t flags, int node);

static __always_inline void *kmem_cache_alloc(struct kmem_cache *cachep,
					      gfp_t flags)
{
	return kmem_cache_alloc_node(cachep, flags, NUMA_NO_NODE);
}

void *__kmalloc_node(size_t size, gfp_t flags, int node);

static __always_inline void *kmalloc_node(size_t size, gfp_t flags, int node)
{
	return __kmalloc_node(size, flags, node);
}

static __always_inline void *kmalloc(size_t size, gfp_t flags)
{
	return __kmalloc_node(size, flags, NUMA_NO_NODE);
}

static __always_inline void *__kmalloc(size_t size, gfp_t flags)
{
	return kmalloc(size, flags);
}

#endif /* __LINUX_SLOB_DEF_H */
