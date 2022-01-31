/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef SB_MALLOC_H
#define SB_MALLOC_H

#warning "FTFS_KMALLOC_MAX_SHIFT hard coded"

#define FTFS_KMALLOC_MAX_SHIFT 12
#define FTFS_KMALLOC_MAX_SIZE (1UL << FTFS_KMALLOC_MAX_SHIFT)

#ifdef FTFS_MEM_DEBUG
extern atomic64_t ftfs_kmalloc_in_use;
extern atomic64_t ftfs_vmalloc_in_use;
#endif

int init_sb_vmalloc_cache(void);
int destroy_sb_vmalloc_cache(void);
int resolve_sb_malloc_symbols(void);
void *sb_malloc(size_t size);
void sb_free(void* ptr);
void *sb_realloc(void *ptr, size_t size);

#endif /* SB_MALLOC_H */
