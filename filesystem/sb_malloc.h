/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef SB_MALLOC_H
#define SB_MALLOC_H

#ifdef __KERNEL__
#define FTFS_KMALLOC_MAX_SIZE PAGE_SIZE
#else /* __KERNEL__ */
#include <stdlib.h>
#include <stdbool.h>
#endif /* __KERNEL__ */

int init_sb_vmalloc_cache(void);
int destroy_sb_vmalloc_cache(void);
int resolve_sb_malloc_symbols(void);
void *sb_malloc(size_t size);
void *sb_malloc_sized(size_t size, bool abort_on_fail);
void sb_free(void* ptr);
void sb_free_sized(void* ptr, size_t size);
void *sb_realloc(void *ptr, size_t old_size, size_t new_size);

#endif /* SB_MALLOC_H */
