/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/slab.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include "ftfs.h"
#include "ftfs_malloc.h"

typedef struct memory_status {
    uint64_t malloc_count;    // number of malloc operations
    uint64_t free_count;      // number of free operations
    uint64_t realloc_count;   // number of realloc operations
    uint64_t malloc_fail;     // number of malloc operations that failed
    uint64_t realloc_fail;    // number of realloc operations that failed
    uint64_t requested;       // number of bytes requested
    uint64_t used;            /* number of bytes used (requested +
			       * overhead), obtained from
			       * malloc_usable_size() */
    uint64_t freed;           // number of bytes freed;
    volatile uint64_t max_in_use;      /* maximum memory footprint
					* (used - freed), approximate
					* (not worth threadsafety
					* overhead for exact) */
    const char *mallocator_version;
    uint64_t mmap_threshold;
} LOCAL_MEMORY_STATUS_S, *LOCAL_MEMORY_STATUS;

static const char *mallocator = "linux";

static LOCAL_MEMORY_STATUS_S status;

typedef void *(*malloc_fun_t)(size_t);
typedef void  (*free_fun_t)(void*);
typedef void *(*realloc_fun_t)(void*,size_t);

/*
static malloc_fun_t  t_malloc  = 0;
static malloc_fun_t  t_xmalloc = 0;
static realloc_fun_t t_realloc = 0;
static realloc_fun_t t_xrealloc = 0;
static free_fun_t    t_free    = 0;
*/

void ftfs_memory_debug_break(void) {
#ifdef FTFS_MEM_DEBUG
	ftfs_error(__func__, "ftfs_kmalloc_in_use: %ld",
		   atomic64_read(&ftfs_kmalloc_in_use));
	ftfs_error(__func__, "ftfs_vmalloc_in_use: %ld",
		   atomic64_read(&ftfs_vmalloc_in_use));
#endif
	ftfs_error(__func__, "debug");
}

void toku_memory_get_status(LOCAL_MEMORY_STATUS s) {
	*s = status;
}

struct kmem_cache *ule_cache;

int init_ule_cache(size_t size, unsigned long flags, void (*ctor)(void *))
{
#ifdef ULE_DEBUG
	printk(KERN_ALERT "initializing ULE cache");
#endif
	ule_cache = kmem_cache_create("ule_cache", size, 0, flags, ctor);
	if (ule_cache == NULL) {
		ftfs_error(__func__, "kmem_cache_create(\"ule_cache\")");
		return -ENOMEM;
	}

	return 0;
}

void destroy_ule_cache(void)
{
	kmem_cache_destroy(ule_cache);
}

void *toku_ule_get(void)
{
#ifdef ULE_DEBUG
	void *ule = kmem_cache_alloc(ule_cache, GFP_KERNEL);
	pr_devel("ule get %p\n", ule);
	return ule;
#else
	return kmem_cache_alloc(ule_cache, GFP_KERNEL);
#endif
}

void toku_ule_put(void * ule)
{
#ifdef ULE_DEBUG
	pr_devel("ule put %p\n", ule);
#endif
	kmem_cache_free(ule_cache, ule);
}

/**
 * we overwrote portability/memory.cc's
 * toku_memory_startup|shutdown(), but we need the ft code to pass in
 * object sizes/constructors for kmem_cache creation.  Solution: we
 * call to ft code and they call back to us (using knowledge from
 * appropriate headers) with all of the oject information... see
 * portability/portability.cc
 */
extern int init_ftfs_kernel_caches(void);

int toku_memory_startup(void)
{
#ifdef FTFS_MEM_DEBUG
	atomic64_set(&ftfs_kmalloc_in_use, 0);
	atomic64_set(&ftfs_vmalloc_in_use, 0);
#endif
	memset(&status, 0, sizeof(status));
	status.mallocator_version = mallocator;
	status.mmap_threshold = FTFS_KMALLOC_MAX_SIZE;
	return init_ftfs_kernel_caches();
}
void toku_memory_shutdown(void)
{
	destroy_ule_cache();
}

void *toku_calloc(size_t nmemb, size_t size) {
	size_t alloc_size = nmemb * size;
	if (alloc_size > FTFS_KMALLOC_MAX_SIZE)
		return vzalloc(alloc_size);

	return kcalloc(nmemb, size, GFP_KERNEL);

}

void *toku_xcalloc(size_t nmemb, size_t size) {
	void *p = toku_calloc(nmemb, size);
	if (p == NULL) {
		ftfs_error(__func__, "toku_calloc(%d, %d) returned NULL",
			   nmemb, size);
		ftfs_memory_debug_break();
		BUG();
	}
	return p;
}

void *toku_malloc(size_t size) {
	return ftfs_malloc(size);
}

void *toku_xmalloc(size_t size) {
	void *p = toku_malloc(size);
	if (p == NULL) {
		ftfs_error(__func__, "toku_malloc(%d) returned NULL", size);
		ftfs_memory_debug_break();
		BUG();
	}
	return p;
}

void *toku_malloc_aligned(size_t alignment, size_t size) {
	return ftfs_malloc_aligned(alignment, size);
}

void *toku_xmalloc_aligned(size_t alignment, size_t size) {
	void * p = toku_malloc_aligned(alignment, size);
	if (p == NULL) {
		ftfs_error(__func__, "toku_malloc_alignment(%d) returned NULL", size);
		ftfs_memory_debug_break();
		BUG();
	}
	return p;
}

void *toku_realloc(void *old, size_t size) {
	return ftfs_realloc(old, size);
}

void *toku_xrealloc(void *old, size_t size) {
	void *p = toku_realloc(old, size);
	if (p == NULL) {
		ftfs_error(__func__, "toku_xrealloc(%p, %d) returned NULL",
			   old, size);
		ftfs_memory_debug_break();
		BUG();
	}
	return p;
}

void toku_free(void *p) {
	if(p)
		ftfs_free(p);
}

void *toku_realloc_aligned(size_t alignment, void *p, size_t size) {
	return ftfs_realloc_aligned(alignment, p, size);
}

void *toku_memdup (const void *v, size_t len) {
	return ftfs_memdup(v, len);
}

char *toku_strdup (const char *s) {
	return ftfs_strdup(s);
}

void *toku_xmemdup (const void *v, size_t len) {
	void *p = toku_memdup(v, len);
	if (p == NULL) {
		ftfs_error(__func__, "toku_memdup(%p, %d) returned NULL",
			   v, len);
		ftfs_memory_debug_break();
		BUG();
	}
	return p;
}

char *toku_xstrdup (const char *s) {
	char *t = toku_strdup(s);
	if (t == NULL) {
		ftfs_error(__func__, "toku_strdup(%s) returned NULL",
			   s);
		ftfs_memory_debug_break();
		BUG();
	}
	return t;
}

size_t toku_memory_footprint(void * p, size_t touched) {
	if (p)
		return ftfs_allocsize(p);
	return 0;
}

/* set these to bug until we understand our memory leak */
void
toku_set_func_malloc(malloc_fun_t f) {
	BUG();
	//t_malloc = f;
	//t_xmalloc = f;
}

void
toku_set_func_realloc(realloc_fun_t f) {
	BUG();
	//t_realloc = f;
	//t_xrealloc = f;
}

void
toku_set_func_free(free_fun_t f) {
	BUG();
	//t_free = f;
}
