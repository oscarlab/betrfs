/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/uaccess.h>
#include <linux/kallsyms.h>
#include <linux/page-flags.h>
#include <linux/mm_types.h>
#include <linux/mm.h>
#include "ftfs_malloc.h"
#include "ftfs_error.h"
#include "ftfs.h"

#include <linux/hashtable.h>

typedef struct vm_struct *(*find_vm_area_t)(const void *);

DECLARE_SYMBOL_FTFS(find_vm_area);

int resolve_ftfs_malloc_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(find_vm_area);
	return 0;
}

#ifdef TOKU_MEMLEAK_DETECT

struct obj_trace {
	struct hlist_node node;
	unsigned long trace[MAX_TRACE];
	unsigned int trace_len;
	uint64_t key;
	size_t size;
}

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


/*
 * init a hash list to store the various objects allocated and freed
 * the stacktrace will be stored as the value
 */
void init_mem_trace() {
	DEFINE_HASHTABLE(memtrace, 12);
	hash_init(memtrace);
}

#endif /* TOKU_MEMLEAK_DETECT */

#ifdef FTFS_MEM_DEBUG
atomic64_t ftfs_kmalloc_in_use;
atomic64_t ftfs_vmalloc_in_use;

void debug_ftfs_alloc(void *p, size_t size)
{
#ifdef TOKU_MEMLEAK_DETECT
	struct obj_trace *data;
#endif
	size_t alloc_size = ftfs_allocsize(p);
	if (is_vmalloc_addr(p))
		atomic64_add(alloc_size, &ftfs_vmalloc_in_use);
	else
		atomic64_add(alloc_size, &ftfs_kmalloc_in_use);

#ifdef FTFS_DEBUG_PTRS
	printk(KERN_ALERT "alloc ptr:%p size:%ld\n", p, size);
	if (size != alloc_size)
		printk(KERN_ALERT "\tallocsize: %ld, size: %ld\n",
		       alloc_size, size);
#endif
#ifdef TOKU_MEMLEAK_DETECT
	data = kmalloc(sizeof(struct obj_trace), GFP_KERNEL);
	data->key = p;
	data->size = size;
	data->trace_len = ftfs_save_stack_trace(data->trace);
	hash_add(memtrace, &data->node, p);
#endif
}

void debug_ftfs_free(void *p)
{
#ifdef TOKU_MEMLEAK_DETECT
	struct obj_trace* cursor;
#endif
	size_t size = ftfs_allocsize(p);
	if (is_vmalloc_addr(p))
		atomic64_sub(size, &ftfs_vmalloc_in_use);
	else
		atomic64_sub(size, &ftfs_kmalloc_in_use);

#ifdef FTFS_DEBUG_PTRS
	printk(KERN_ALERT "free ptr:%p size:%ld\n", p, size);
#endif
#ifdef TOKU_MEMLEAK_DETECT
	hash_for_each_possible(memtrace, cursor,  node, p) {
		if(cursor->key == p) {
			hash_del(&cursor->node);
			kfree(cursor);
			break;
		}
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
#define FTFS_VMALLOC_SMALL 98304
#define FTFS_VMALLOC_LARGE 163840
#define FTFS_VMALLOC_SMALL_COUNT 32

struct list_head __percpu *vcache_percpu_alloc;
struct list_head __percpu *vcache_percpu_freed;

struct ftfs_cached_ptr {
	struct list_head list;
	void *p;
};

int init_ftfs_vmalloc_cache(void)
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
//		ftfs_error(__func__, "num_elts = %d", num_elts);
	}

	return 0;
}

int destroy_ftfs_vmalloc_cache(void) {

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
		//if(i!=num_elts) {
//			ftfs_error(__func__, "error:upon unloading there are still in-use vmalloc cache. memleak warning already freed %d, alloc-ed %d", i, num_elts);
		//	return -1;
	//	}
		
	}
	free_percpu(vcache_percpu_alloc);
        free_percpu(vcache_percpu_freed);
	return 0;
}
void *ftfs_alloc_small(size_t size)
{
	int cpu;
	struct ftfs_cached_ptr *ptr;
	struct list_head *alloced;
	struct list_head *freed;
	void *p;

	cpu = get_cpu();
	alloced = per_cpu_ptr(vcache_percpu_alloc, cpu);
	if (list_empty(alloced)) {
		put_cpu();
		return vmalloc(size);
	}
	freed = per_cpu_ptr(vcache_percpu_freed, cpu);

	ptr = list_first_entry(alloced, struct ftfs_cached_ptr, list);
	p = ptr->p;
	ptr->p = NULL;
	list_move(&ptr->list, freed);
	put_cpu();
	return p;
}

void *ftfs_large_alloc(size_t size)
{
	if (size < FTFS_VMALLOC_SMALL) {
		return vmalloc(size);
	} else if (size < FTFS_VMALLOC_LARGE) {
		return ftfs_alloc_small(size);
	}
	return vmalloc(size);
}

void *ftfs_malloc(size_t size)
{
	void *p;

	if (size > FTFS_KMALLOC_MAX_SIZE)
		p = ftfs_large_alloc(size);
	else
		p = kmalloc(size, GFP_KERNEL);

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, size);
#endif

	return p;
}

void *malloc(size_t size)
{
	return ftfs_malloc(size);
}

void ftfs_free_small(void *p)
{
	int cpu;
	struct ftfs_cached_ptr *ptr;
	struct list_head *alloced;
	struct list_head *freed;

	cpu = get_cpu();
	freed = per_cpu_ptr(vcache_percpu_freed, cpu);
	if (list_empty(freed)) {
		put_cpu();
		vfree(p);
		return;
	}
	alloced = per_cpu_ptr(vcache_percpu_alloc, cpu);
	ptr = list_first_entry(freed, struct ftfs_cached_ptr, list);
	ptr->p = p;
	list_move(&ptr->list, alloced);
	put_cpu();
}

static inline size_t vmalloc_allocsize(void *p)
{
	FTFS_DEBUG_ON(!is_vmalloc_addr(p));
	return ftfs_find_vm_area(p)->nr_pages << PAGE_SHIFT;
}

void ftfs_vfree(void *p)
{
	size_t size = vmalloc_allocsize(p);
	if (FTFS_VMALLOC_LARGE == size) {
		ftfs_free_small(p);
	} else {
		vfree(p);
	}
}

void ftfs_free(void *p)
{
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_free(p);
#endif
	if (is_vmalloc_addr(p))
		ftfs_vfree(p);
	else
		kfree(p);
}

void free(void *p)
{
	ftfs_free(p);
}

/* dumb copy for now. what this should do is unmap the pages, alloc a
 * few more, and then remap them as in vmap */
static void *vm_enlarge_remap(void *p, size_t new_size)
{
	size_t sz = vmalloc_allocsize(p);
	void *n = vmalloc(new_size);
	if (!n) {
		ftfs_error(__func__, "vmalloc failed.");
		return NULL;
	}

	FTFS_DEBUG_ON(sz > new_size);
	FTFS_DEBUG_ON(!is_vmalloc_addr(p));

	memcpy(n, p, sz);
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(n, new_size);
	debug_ftfs_free(p);
#endif
	ftfs_vfree(p);
	return n;
}


static void *vm_enlarge_in_place(void *p, size_t size)
{
	return NULL;
}

int vmalloc_has_room(void *p, size_t size)
{
	/* we will return 0 until enlarge_in_place is finished */
	return 0;
}

static void *vmalloc_realloc(void *p, size_t size)
{
	if (size <= vmalloc_allocsize(p)) {
		/* nothing to do here... I am ok with lying */
		return p;
	}

	/* check if we can just add pages */
	if (vmalloc_has_room(p, size))
		return vm_enlarge_in_place(p, size);

	return vm_enlarge_remap(p, size);
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
	debug_ftfs_free(old_p);
#endif

	memcpy(new_p, old_p, old_size);
	kfree(old_p);
	return new_p;
}

void *ftfs_realloc(void *ptr, size_t size)
{
	void *p;

	if (ptr == NULL) {
		p = ftfs_malloc(size);
		goto done;
	}

	if (size == 0) {
		ftfs_free(ptr);
		p = NULL;
	} else if (is_vmalloc_addr(ptr)) {
		p = vmalloc_realloc(ptr, size);
	} else if (size > FTFS_KMALLOC_MAX_SIZE) {
		return kmalloc_to_vmalloc(ptr, size);
	} else {
		p = krealloc(ptr, size, GFP_KERNEL);
	}

done:
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_free(ptr);
	debug_ftfs_alloc(p, size);
#endif

	return p;
}

void *realloc(void *ptr, size_t size)
{
	return ftfs_realloc(ptr, size);
}

size_t ftfs_allocsize(void *p)
{
	if (!p)
		return 0;

	if (is_vmalloc_addr(p))
		return vmalloc_allocsize(p);

	return ksize(p);
}

void *ftfs_memdup(const void *v, size_t _len) {
	int len = _len;
	void *p;

	if(_len == 0)
		len = 1;

	if(is_vmalloc_addr(v)) {
		// dp 1/13/15: Just because v is vmalloc'ed doesn't mean p needs to be
		p = ftfs_malloc(len);
		if (p)
			memcpy(p, v, _len);
		return p;
	} else {
		p = kmemdup(v, _len, GFP_KERNEL);
	}

#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, len);
#endif

	return p;
}

char *ftfs_strdup(const char *s) {
	char *p;

	if(is_vmalloc_addr(s)) {
		int len = strlen(s) + 1;
		p = ftfs_malloc(len);
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
int posix_memalign(void **res, size_t align, size_t len)
{
	/* unsigned char *mem, *new, *end;
	size_t header, footer;*/
	void * p;
	if ((align & -align) != align) return EINVAL;
	if (len > SIZE_MAX - align) return ENOMEM;
	/* dp 1/13/15: I think the issue was 512 vs 4K alignment for direct I/O.  
	 *             kmalloc seems ok now.
	 */
	//p = ftfs_malloc(len);
	/* wkj: 2/2/15 regardless of the issue with direct I/O, kmalloc will
	 *  not return an aligned pointer... reverting to vmalloc */
	p = vmalloc(len);
#ifdef FTFS_MEM_DEBUG
	debug_ftfs_alloc(p, len);
#endif
	if(IS_ERR(p)) {
		return PTR_ERR(p);
		//note: errno is not needed to set in errno.
	}
	* res = p;
	return 0;
/*  	if (align <= 4*sizeof(size_t)) {
		if (IS_ERR(mem = ftfs_malloc(len)))
			return PTR_ERR(mem);
		*res = mem;
		return 0;
	}

	if (IS_ERR(mem = ftfs_malloc(len + align-1)))
		return PTR_ERR(mem);
	
	header = ((size_t *)mem)[-1];
	end = mem + (header & -8);
	footer = ((size_t *)end)[-2];
	new = (void *)((uintptr_t)mem + ((align-1) & (-align)));

	if (!(header & 7)) {
		((size_t *)new)[-2] = ((size_t *)mem)[-2] + (new-mem);
		((size_t *)new)[-1] = ((size_t *)mem)[-1] - (new-mem);
		*res = new;
		return 0;
	}

	((size_t *)mem)[-1] = (header&7) | (new-mem);
	((size_t *)new)[-2] = (footer&7) | (new-mem);
	((size_t *)new)[-1] = (header&7) | (end-new);
	((size_t *)end)[-2] = (footer&7) | (end-new);


	if (new != mem) ftfs_free(mem);
	*res = new;
	return 0;
	*/
}

void * ftfs_malloc_aligned(size_t alignment, size_t size) {
	void * p;
	int r = posix_memalign(&p, alignment, size);
	if(r!=0) {
		set_errno(r);
		p = NULL;
	}
	return p;
}

void * ftfs_realloc_aligned(size_t alignment, void *p, size_t size) {
	if (p==NULL) {
		return ftfs_malloc_aligned(alignment, size);
	} else {
		void *newp = ftfs_realloc(p, size);
		if (0!=((long long)newp%alignment)) {
			void *newp2 = ftfs_malloc_aligned(alignment, size);
			memcpy(newp2, newp, size);
#ifdef FTFS_MEM_DEBUG
			debug_ftfs_free(p);
#endif
			free(newp);
			newp = newp2;
		}
		return newp;
	}
}

