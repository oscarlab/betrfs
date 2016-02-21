/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef FTFS_MALLOC_H
#define FTFS_MALLOC_H
#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus

extern void* malloc(size_t size);
extern void free(void *ptr);
extern void* realloc(void *ptr, size_t size);

}
#else

#warning "FTFS_KMALLOC_MAX_SHIFT hard coded"

#define FTFS_KMALLOC_MAX_SHIFT 12
#define FTFS_KMALLOC_MAX_SIZE (1UL << FTFS_KMALLOC_MAX_SHIFT)

#ifdef FTFS_MEM_DEBUG
extern atomic64_t ftfs_kmalloc_in_use;
extern atomic64_t ftfs_vmalloc_in_use;
#endif

int init_ftfs_vmalloc_cache(void);
int destroy_ftfs_vmalloc_cache(void);
int resolve_ftfs_malloc_symbols(void);
void *ftfs_malloc(size_t size);
void *ftfs_malloc_aligned(size_t,size_t);
void *ftfs_realloc_aligned(size_t, void *, size_t);
void ftfs_free(void* ptr);
void *ftfs_realloc(void *ptr, size_t size);
size_t ftfs_allocsize(void *p);
void * ftfs_memdup(const void *, size_t);
char * ftfs_strdup(const char *);
int posix_memalign(void **, size_t, size_t);
#endif /*__cplusplus */

#endif /* FTFS_MALLOC_H */
