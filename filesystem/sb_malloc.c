/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound file system (really
 * klibc).  It implements memory management APIs the ft
 * code requires, using kernel APIs.
 *
 * Some of these functions have a "toku_" prefix, but the usage
 * is only for supporting the key-value store; not for use internally
 * by the file system.
 */

#include <linux/slab.h>
#include <linux/kallsyms.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include "ftfs.h"
#include "sb_malloc.h"
#include "sb_error.h"

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

typedef struct vm_struct *(*find_vm_area_t)(const void *);

DECLARE_SYMBOL_FTFS(find_vm_area);

int resolve_sb_malloc_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(find_vm_area);
	return 0;
}

#ifdef TOKU_MEMLEAK_DETECT
#include <linux/hashtable.h>
#include <linux/stacktrace.h>
size_t sb_allocsize(void *p);

#define MAX_TRACE 128
struct obj_trace {
	struct hlist_node node;
	unsigned long trace[MAX_TRACE];
	unsigned int trace_len;
	uint64_t key;
	size_t size;
	size_t alloc_size;
	bool is_vmalloc;
};

/*
 * Save stack trace to the given array of MAX_TRACE size.
 */
static int ftfs_save_stack_trace(unsigned long *trace)
{
	struct stack_trace stack_trace;

	stack_trace.max_entries = MAX_TRACE;
	stack_trace.nr_entries = 0;
	stack_trace.entries = trace;
	stack_trace.skip = 2;
	save_stack_trace(&stack_trace);

	return stack_trace.nr_entries;
}

static void ftfs_print_stack_trace(unsigned long *trace, int nr_entries)
{
	struct stack_trace stack_trace;

	stack_trace.max_entries = MAX_TRACE;
	stack_trace.nr_entries = nr_entries;
	stack_trace.entries = trace;
	stack_trace.skip = 2;
	print_stack_trace(&stack_trace, 2);
	printk(KERN_ALERT "========================\n");
}

/*
 * init a hash list to store the various objects allocated and freed
 * the stacktrace will be stored as the value
 */
DEFINE_HASHTABLE(memtrace, 17);
static DEFINE_SPINLOCK(ht_lock);

void init_mem_trace(void) {
	hash_init(memtrace);
	spin_lock_init(&ht_lock);
}

void free_mem_trace(void) {
	int tmp;
	struct obj_trace* cursor;
	struct hlist_node* tmp_obj;
	spin_lock(&ht_lock);
	if (hash_empty(memtrace) == true) {
		printk(KERN_ALERT "No memory leak detected\n");
		goto out;
	}
	printk(KERN_ALERT "The hashtable is not empty\n");
	hash_for_each_safe(memtrace, tmp, tmp_obj, cursor, node) {
		hash_del(&cursor->node);
		printk(KERN_ALERT "ptr=%p\n", (void*)cursor->key);
		printk(KERN_ALERT "size=%ld\n", cursor->size);
		printk(KERN_ALERT "alloc_size=%ld\n", cursor->alloc_size);
		printk(KERN_ALERT "is_vmalloc=%d\n", cursor->is_vmalloc);
		ftfs_print_stack_trace(cursor->trace, cursor->trace_len);
		kfree(cursor);
	}
out:
	spin_unlock(&ht_lock);
}
#endif /* TOKU_MEMLEAK_DETECT */

#ifdef FTFS_MEM_DEBUG
atomic64_t ftfs_kmalloc_in_use;
atomic64_t ftfs_vmalloc_in_use;

size_t sb_allocsize(void *p)
{
	if (!p)
		return 0;

	if (is_vmalloc_addr(p)) {
		FTFS_DEBUG_ON(!is_vmalloc_addr(p));
		return ftfs_find_vm_area(p)->nr_pages << PAGE_SHIFT;
	}

	return ksize(p);
}

void debug_ftfs_alloc(void *p, size_t size)
{
#ifdef TOKU_MEMLEAK_DETECT
	struct obj_trace *data;
#endif
#if (defined FTFS_DEBUG_PTRS) || (defined TOKU_MEMLEAK_DETECT)
	size_t alloc_size = sb_allocsize(p);
#endif
	if (is_vmalloc_addr(p))
		atomic64_add(size, &ftfs_vmalloc_in_use);
	else
		atomic64_add(size, &ftfs_kmalloc_in_use);

#ifdef FTFS_DEBUG_PTRS
	printk(KERN_ALERT "alloc ptr:%p size:%ld\n", p, size);
	if (size != alloc_size)
		printk(KERN_ALERT "\tallocsize: %ld, size: %ld\n",
		       alloc_size, size);
#endif
#ifdef TOKU_MEMLEAK_DETECT
	data = kmalloc(sizeof(struct obj_trace), GFP_KERNEL);
	data->key = (uint64_t)p;
	data->size = size;
	data->alloc_size = alloc_size;
	data->is_vmalloc = is_vmalloc_addr(p);
	data->trace_len = ftfs_save_stack_trace(data->trace);
	spin_lock(&ht_lock);
	hash_add(memtrace, &data->node, (uint64_t)p);
	spin_unlock(&ht_lock);
#endif
}

void debug_ftfs_free(void *p)
{
#ifdef TOKU_MEMLEAK_DETECT
	struct obj_trace* cursor;
	bool found = false;
#endif
	size_t size = sb_allocsize(p);

	if (is_vmalloc_addr(p))
		atomic64_sub(size, &ftfs_vmalloc_in_use);
	else
		atomic64_sub(size, &ftfs_kmalloc_in_use);

#ifdef FTFS_DEBUG_PTRS
	printk(KERN_ALERT "free ptr:%p size:%lu\n", p, size);
#endif

#ifdef TOKU_MEMLEAK_DETECT
	if (p == NULL)
		return;
	spin_lock(&ht_lock);
	hash_for_each_possible(memtrace, cursor,  node, (u64)p) {
		if(cursor->key == (uint64_t)p) {
			hash_del(&cursor->node);
			kfree(cursor);
			found = true;
			break;
		}
	}
	spin_unlock(&ht_lock);
	if (found == false) {
		printk("possible double free:%s p=%p, size=%lu\n", __func__, p, size);
		dump_stack();
	}
#endif
}
#endif /* FTFS_MEM_DEBUG */


/*
 * vmalloc() is slow, so we cache some commonly used pointer sizes.
 * This greatly speeds up some workloads, but harms none.
 *
 * A long-term fix would be to change the memory allocation functions
 * in the ft code.
 */
#define FTFS_VMALLOC_SMALL  98304UL
#define FTFS_VMALLOC_LARGE 163840UL
#define FTFS_VMALLOC_SMALL_COUNT 32

static inline int size_in_vmalloc_cache_range(size_t size) {
	return (size >= FTFS_VMALLOC_SMALL
		&& size <= FTFS_VMALLOC_LARGE);
}

struct list_head __percpu *vcache_percpu_alloc;
struct list_head __percpu *vcache_percpu_freed;

struct ftfs_cached_ptr {
	struct list_head list;
	void *p;
};

int init_sb_vmalloc_cache(void)
{
	int i, c;
	int num_elts;

	/* don't care about rounding. choose better FTFS_VMALLOC_SMALL_COUNT */
	num_elts = FTFS_VMALLOC_SMALL_COUNT / num_online_cpus();

	vcache_percpu_alloc = alloc_percpu(struct list_head);
	if (!vcache_percpu_alloc) {
		ftfs_error(__func__, "could not initialize per_cpu var");
		BUG();
	}

	vcache_percpu_freed = alloc_percpu(struct list_head);
	if (!vcache_percpu_freed) {
		ftfs_error(__func__, "could not initialize per_cpu var");
		BUG();
	}

	for_each_possible_cpu(c) {
		INIT_LIST_HEAD(per_cpu_ptr(vcache_percpu_alloc, c));
		INIT_LIST_HEAD(per_cpu_ptr(vcache_percpu_freed, c));
	}

	for_each_online_cpu(c) {
		for (i = 0; i < num_elts; i++) {
			struct ftfs_cached_ptr *ptr = kmalloc(sizeof(*ptr),
							      GFP_KERNEL);
			if (!ptr)
				BUG();
			INIT_LIST_HEAD(&ptr->list);
			ptr->p = vmalloc(FTFS_VMALLOC_LARGE);
			if (!ptr->p)
				BUG();
			list_add(&ptr->list,
				 per_cpu_ptr(vcache_percpu_alloc, c));
		}
	}

	return 0;
}

int destroy_sb_vmalloc_cache(void) {

	int i, c;
	int num_elts;
	struct list_head * entry, * q,  * alloc_head;
	struct ftfs_cached_ptr *ptr;

	num_elts = FTFS_VMALLOC_SMALL_COUNT / num_online_cpus();

	for_each_online_cpu(c) {
		alloc_head = per_cpu_ptr(vcache_percpu_alloc, c);
		i = 0;
		list_for_each_safe(entry, q, alloc_head) {
                	ptr = list_entry(entry, struct ftfs_cached_ptr, list);
			vfree(ptr->p);
			list_del(entry);
			kfree(ptr);
			i++;
		}
	}
	free_percpu(vcache_percpu_alloc);
        free_percpu(vcache_percpu_freed);
	return 0;
}

void sb_memory_debug_break(void) {
#ifdef FTFS_MEM_DEBUG
	ftfs_error(__func__, "ftfs_kmalloc_in_use: %ld",
		   atomic64_read(&ftfs_kmalloc_in_use));
	ftfs_error(__func__, "ftfs_vmalloc_in_use: %ld",
		   atomic64_read(&ftfs_vmalloc_in_use));
#endif
	ftfs_error(__func__, "debug");
}

// This is a special interface for common object sizes.  Do not
// mix and match with malloc.  For performance, we need to statically
// specify which is which
void *sb_cache_alloc(size_t size, bool abort_on_fail)
{
	void *p;
	int cpu;
	struct ftfs_cached_ptr *ptr;
	struct list_head *alloced;
	struct list_head *freed;

	BUG_ON(!size_in_vmalloc_cache_range(size));

	// Disable preemption; return this CPU id
	cpu = get_cpu();
	alloced = per_cpu_ptr(vcache_percpu_alloc, cpu);
	if (list_empty(alloced)) {
		put_cpu();
		// XXX: We should probably grow and shrink the caches properly
		p = vmalloc(FTFS_VMALLOC_LARGE);
	} else {
		freed = per_cpu_ptr(vcache_percpu_freed, cpu);

		ptr = list_first_entry(alloced, struct ftfs_cached_ptr, list);
		p = ptr->p;
		BUG_ON(!p);
		ptr->p = NULL;
		list_move(&ptr->list, freed);
		put_cpu();
	}

	if (abort_on_fail && !p) {
		ftfs_error(__func__, "sb_cache_alloc(%ld) returned NULL", size);
		sb_memory_debug_break();
		BUG();
	}

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, size);
#endif

	return p;
}

static
void *_sb_malloc(size_t size, bool abort_on_fail)
{
	void *p;

	// dep 1/1/20: kmalloc and kmemdup do NOT like being passed a
	// zero length and return 0x10 as an error value, which
	// doesn't get caught as a NULL. I _think_ the FT code expects
	// this code to follow the malloc(0) convention which either
	// returns a NULL or a pointer that can be passed to free.
	// 0x10 cannot be passed to free() and seems to be causing some
	// subtle memory corruptions.
	//
	// XXX: Arguably, the right thing to do here is just return
	// NULL on a zero length, but I think there is some toku code
	// that expects a pointer value to be returned. I'm not 100%
	// certain on this, but the code paths here are used somewhat
	// infrequently.  It would be a good task for someone to
	// try just returning NULL as a separate optimization.
	if (size == 0)
		size = 1;

	if (size > FTFS_VMALLOC_LARGE) {
		p = vmalloc(size);
	} else if (size >= FTFS_VMALLOC_SMALL) {
		// Objects in this size range should use
		// the cached alloc/free interface
		printk(KERN_ALERT "sb_malloc allocation using size appropriate for cache allocator: %lu requested, min is %lu, max is %lu\n",
		       size, FTFS_VMALLOC_SMALL, FTFS_VMALLOC_LARGE);

		BUG();
	} else if (size > FTFS_KMALLOC_MAX_SIZE) {
		p = vmalloc(size);
	} else {
		p = kmalloc(size, GFP_KERNEL);
	}

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, size);
#endif
	if (p == NULL || p == (void *) 0x20)
		printk(KERN_ERR "An sb_malloc is gonna fail\n");

	if (abort_on_fail && p == NULL) {
		ftfs_error(__func__, "sb_malloc(%lu) returning NULL", size);
		sb_memory_debug_break();
		BUG();
	}

	return p;
}

void *sb_malloc(size_t size)
{
	return _sb_malloc(size, false);
}


// The ft code uses toku_malloc and malloc
void *toku_malloc(size_t size) __attribute__((alias ("sb_malloc")));
void *malloc(size_t size) __attribute__((alias ("sb_malloc")));

void sb_cache_free(void* p)
{
	int cpu;
	struct ftfs_cached_ptr *ptr;
	struct list_head *alloced;
	struct list_head *freed;

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_free(p);
#endif

	cpu = get_cpu();
	freed = per_cpu_ptr(vcache_percpu_freed, cpu);
	if (list_empty(freed)) {
		// XXX: We could be smarter about resizing the list
		put_cpu();
		vfree(p);
		return;
	}
	alloced = per_cpu_ptr(vcache_percpu_alloc, cpu);
	ptr = list_first_entry(freed, struct ftfs_cached_ptr, list);
	BUG_ON(ptr->p);
	ptr->p = p;
	list_move(&ptr->list, alloced);
	put_cpu();
}

void sb_free(void *p)
{
	if (p) {
#ifdef FTFS_MEM_DEBUG
		debug_ftfs_free(p);
#endif
		if (is_vmalloc_addr(p))
			vfree(p);
		else
			kfree(p);
	}
}
// The FT code needs these symbols
void free(void *p) __attribute__((alias ("sb_free")));
void toku_free(void *p) __attribute__((alias ("sb_free")));


/* The malloc and free "sized" variants are for cases
 * where the caller is tracking the size of the allocation.
 * We can then avoid tracking this ourselves, or expensive
 * queries for the size.
 *
 * Using malloc_sized() is a "contract" that the caller will
 * pass the size to free.
 *
 * A significant portion of the toku code does not handle allocation
 * failures, and instead uses an "xmalloc", which just blows
 * an assert.  The abort_on_fail arugment keeps this behavior.
 */
void *sb_malloc_sized(size_t size, bool abort_on_fail)
{
	if (size_in_vmalloc_cache_range(size)) {
		return sb_cache_alloc(size, abort_on_fail);
	} else {
		return _sb_malloc(size, abort_on_fail);
	}
}

void sb_free_sized(void* ptr, size_t size)
{
	if (size_in_vmalloc_cache_range(size)) {
		return sb_cache_free(ptr);
	} else {
		return sb_free(ptr);
	}
}


/* dumb copy for now. what this should do is unmap the pages, alloc a
 * few more, and then remap them as in vmap */
static void *vm_enlarge_remap(void *p, size_t old_size, size_t new_size)
{
	void *n = vmalloc(new_size);
	if (!n) {
		ftfs_error(__func__, "vmalloc failed.");
		return NULL;
	}

	FTFS_DEBUG_ON(old_size > new_size);
	FTFS_DEBUG_ON(!is_vmalloc_addr(p));

	memcpy(n, p, old_size);
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(n, new_size);
	// The free will be recorded in sb_free sized
#endif
	BUG_ON(size_in_vmalloc_cache_range(old_size));
	sb_free_sized(p, old_size);
	return n;
}

static void *vmalloc_realloc(void *p, size_t old_size, size_t new_size)
{
	if (new_size <= old_size ||
	    (size_in_vmalloc_cache_range(old_size)
	     && size_in_vmalloc_cache_range(new_size))
	    ) {
		/* nothing to do here... I am ok with ignoring
		 * wasted space.  We can also increase sizes
		 * within the vmalloc cache for "free".
		 * XXX: Should probably push this information up
		 *      one level and just use the full buffer.
		 */
		return p;
	} else if ((size_in_vmalloc_cache_range(new_size) && !size_in_vmalloc_cache_range(old_size))
		   || (!size_in_vmalloc_cache_range(new_size) && size_in_vmalloc_cache_range(old_size))) {

		// We need to move to/from a cache allocation, rather than remapping the vamlloc region
		void * new_pointer = sb_malloc_sized(new_size, false);
		memcpy(new_pointer, p, old_size);
		sb_free_sized(p, old_size);
		return new_pointer;
	}

	return vm_enlarge_remap(p, old_size, new_size);
}

static void *kmalloc_to_vmalloc(void *old_p, size_t new_size)
{
	size_t old_size = ksize(old_p);
	void *new_p = vmalloc(new_size);
	if (!new_p) {
		ftfs_error(__func__, "vmalloc failed.");
		return NULL;
	}

	FTFS_DEBUG_ON(old_size > new_size);
	FTFS_DEBUG_ON(old_p && !PageSlab(virt_to_head_page(old_p)) &&
		      !PageCompound(virt_to_head_page(old_p)));

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(new_p, new_size);
#endif
	memcpy(new_p, old_p, old_size);
	kfree(old_p);
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_free(old_p);
#endif
	return new_p;
}

void *sb_realloc(void *ptr, size_t old_size, size_t new_size)
{
	void *p;
	// If size is zero, we should just return NULL and free
	// ptr, similar to libc's realloc.
	if (new_size == 0) {
		sb_free(ptr);
		p = NULL;
		goto done;
	}
	if (ptr == NULL) {
		p = sb_malloc(new_size);
		goto done;
	}
	if (is_vmalloc_addr(ptr)) {
		return vmalloc_realloc(ptr, old_size, new_size);
	} else if (new_size > FTFS_KMALLOC_MAX_SIZE) {
		return kmalloc_to_vmalloc(ptr, new_size);
	} else {
#ifdef FTFS_MEM_DEBUG
		debug_ftfs_free(ptr);
#endif
		// XXX: 2x check that krealloc is smart about same power of 2
		p = krealloc(ptr, new_size, GFP_KERNEL);
#ifdef FTFS_MEM_DEBUG
		debug_ftfs_alloc(p, new_size);
#endif
	}
done:
	return p;
}
void *realloc(void *ptr, size_t old_size, size_t new_size) __attribute__((alias ("sb_realloc")));
void *toku_realloc(void *ptr, size_t old_size, size_t new_size) __attribute__((alias ("sb_realloc")));

void *toku_memdup(const void *v, size_t _len) {
	int len = _len;
	void *p;

	// dep 1/1/20: kmalloc and kmemdup do NOT like being passed a
	// zero length and return 0x10 as an error value, which
	// doesn't get caught as a NULL. I _think_ the FT code expects
	// this code to follow the malloc(0) convention which either
	// returns a NULL or a pointer that can be passed to free.
	// 0x10 cannot be passed to free() and seems to be causing some
	// subtle memory corruptions.
	//
	// XXX: Arguably, the right thing to do here is just return
	// NULL on a zero length, but I think there is some toku code
	// that expects a pointer value to be returned. I'm not 100%
	// certain on this, but the code paths here are used somewhat
	// infrequently.  It would be a good task for someone to
	// try just returning NULL as a separate optimization.

	if(_len == 0)
		len = 1;

	if(is_vmalloc_addr(v)) {
		// dp 1/13/15: Just because v is vmalloc'ed doesn't mean p needs to be
		p = sb_malloc(len);
		if (p)
			memcpy(p, v, _len);
	} else {
		p = kmemdup(v, len, GFP_KERNEL);
	}

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, len);
#endif

	if (IS_ERR(p)) {
		p = NULL;
	}


	return p;
}

char *toku_strdup(const char *s) {
	char *p;

	if(is_vmalloc_addr(s)) {
		int len = strlen(s) + 1;
		p = sb_malloc(len);
		if (p)
			memcpy(p, s, len);
		return p;
	} else {
		p = kstrdup(s, GFP_KERNEL);
	}

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, strlen(s+1));
#endif

	return p;
}

/* Borrowed from musl-musl memalign is based on the glibc bookkeeping
 * not working for kmalloc. Using vmalloc for time being */
void * toku_malloc_aligned(size_t alignment, size_t size) {
	void * p;
	if ((alignment & -alignment) != alignment) {
		sb_set_errno(EINVAL);
		return NULL;
	}
	if (size > SIZE_MAX - alignment) {
		sb_set_errno(ENOMEM);
		return NULL;
	}
	/* dp 1/13/15: I think the issue was 512 vs 4K alignment for direct I/O.
	 *             kmalloc seems ok now.
	 */
	//p = ftfs_malloc(size);
	/* wkj: 2/2/15 regardless of the issue with direct I/O, kmalloc will
	 *  not return an aligned pointer... reverting to vmalloc */
	p = vmalloc(size);
	if(IS_ERR(p)) {
		sb_set_errno(PTR_ERR(p));
		p = NULL;
		//note: errno is not needed to set in errno.
#ifdef FTFS_MEM_DEBUG
	} else {
		debug_ftfs_alloc(p, size);
#endif
	}
	return p;
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
	void *ptr;
	size_t alloc_size = nmemb * size;
	if (alloc_size > FTFS_KMALLOC_MAX_SIZE)
		ptr = vzalloc(alloc_size);
	else
		ptr = kcalloc(nmemb, size, GFP_KERNEL);
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(ptr, alloc_size);
#endif
	return ptr;
}

void *toku_xcalloc(size_t nmemb, size_t size) {
	void *p = toku_calloc(nmemb, size);
	if (p == NULL) {
		ftfs_error(__func__, "toku_calloc(%d, %d) returned NULL",
			   nmemb, size);
		sb_memory_debug_break();
		BUG();
	}
	return p;
}

void *toku_xmalloc(size_t size) {
	return _sb_malloc(size, true);
}

void *toku_xmalloc_aligned(size_t alignment, size_t size) {
	void * p = toku_malloc_aligned(alignment, size);
	if (p == NULL) {
		ftfs_error(__func__, "toku_malloc_alignment(%d) returned NULL", size);
		sb_memory_debug_break();
		BUG();
	}
	return p;
}

void *toku_xrealloc(void *old, size_t old_size, size_t new_size) {
	void *p = toku_realloc(old, old_size, new_size);

	// The toku_x* functions cannot fail.  As in, rather than
	// write error handlers on out-of-memory conditions, the code
	// assumes things will blow up if the function fails.
	// However, NULL is a valid return value for a zero size (re)alloc.
	// So let's try to return NULL here and see what happens.
	if (p == NULL && new_size != 0) {
		ftfs_error(__func__, "toku_xrealloc(%p, %d) returned NULL",
			   old, new_size);
		sb_memory_debug_break();
		BUG();
	}
	return p;
}

void *toku_xmemdup (const void *v, size_t len) {
	void *p;
	if (v == NULL && len == 0)
		return NULL;
	p = toku_memdup(v, len);
	if (p == NULL) {
		ftfs_error(__func__, "toku_memdup(%p, %d) returned NULL",
		           v, len);
		sb_memory_debug_break();
		BUG();
	}
	return p;
}

char *toku_xstrdup (const char *s) {
	char *t = toku_strdup(s);
	if (t == NULL) {
		ftfs_error(__func__, "toku_strdup(%s) returned NULL",
			   s);
		sb_memory_debug_break();
		BUG();
	}
	return t;
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
