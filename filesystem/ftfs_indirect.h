/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef FTFS_INDIRECT_H
#define FTFS_INDIRECT_H

#include <linux/list.h>
#include <linux/fs.h>
#include <linux/rwsem.h>
#include <linux/fscache.h>
#include <linux/list_sort.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/crc32.h>

#include "ftfs_indirect_val.h"

/* Locking order: we have two functions for locking
 * ftfs_lock_page and lock_page.
 * Any time both lock_page and ftfs_lock_page are called,
 * the expected ordering is ftfs_lock_page first.
 */

/* Attach every page allocated for read or write with this private data
 * `_count' is used for the refcounting of ft code.
 * `private' is to extend the flags field of page descriptor to
 * track the movement of the page. The 0th bit of it is set
 * when a unbound insert becomes unbound on nonleaf nodes.
 *
 * `_count` can be larger than 1 when a page is applied to answer
 * a query. It can also happen when ftnode_clone_callback is called.
 * If _count is >0, VFS page count is incremented by exactly 1,
 * regardless of how many ftfs-internal users there are.
 * The issue here is that we need to differentiate ftfs-internal
 * references from total references.
 *
 * check ftfs_indirect_val.h for more details of the bits for private.
 */
struct ftfs_page_private {
	atomic_t _count; /* internal count */
	unsigned long private; /* internal flags */
};

static inline struct ftfs_page_private *FTFS_PAGE_PRIV(struct page *page)
{
	return (struct ftfs_page_private *) page->private;
}

static inline bool ftfs_is_read_page(unsigned long priv)
{
	return (priv & (1 << FT_MSG_VAL_NB_READ)) != 0;
}

static inline bool ftfs_is_leaf_page(unsigned long priv)
{
	return (priv & (1 << FT_MSG_VAL_LEAF_BIT)) != 0;
}

static inline bool ftfs_is_nonleaf_page(unsigned long priv)
{
	return (priv & (1 << FT_MSG_VAL_NONLEAF_BIT)) != 0;
}

static inline int ftfs_read_page_count(struct page* page)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,7,0)
	return atomic_read(&page->_count);
#else /* LINUX_VERSION_CODE */
	return atomic_read(&page->_refcount);
#endif /* LINUX_VERSION_CODE */
}

/* implemented in sb_linkage.c */
void toku_dump_hex(const char *str_prefix, const void *buf, size_t len);

/* Implemented in nb_super.c */
struct ftfs_indirect_val *ftfs_alloc_msg_val(void);
void ftfs_free_msg_val(void *ptr);
void ftfs_fill_msg_val(struct ftfs_indirect_val *msg_val,
		       unsigned long pfn, unsigned int size);

/* implemented in sb_pfn.c */
void ftfs_print_page(unsigned long pfn);
void ftfs_set_page_private(unsigned long pfn, unsigned long bit);
void ftfs_clear_page_private(unsigned long pfn, unsigned long bit);
void ftfs_reset_page_private(unsigned long pfn);
struct page *__ftfs_page_cache_alloc(gfp_t gfp);
struct page* ftfs_cow_page(struct page*page, struct address_space *mapping,
			   pgoff_t index, gfp_t gfp_mask, gfp_t gfp_notmask);
void ftfs_inc_page_private_count(struct page *page);
void ftfs_lock_page(struct page *page);
void ftfs_unlock_page(struct page *page);
unsigned long ftfs_alloc_page(void);
void ftfs_set_page_private(unsigned long pfn, unsigned long bit);
void ftfs_lock_page(struct page *page);
int ftfs_lock_page_killable(struct page *page);
void ftfs_unlock_page(struct page *page);
void sb_wait_read_page(unsigned long pfn);
void ftfs_dec_page_private_count(struct page *page);

/* implemented in sb_sfs_ops.c */
void sb_wait_read_page(unsigned long pfn);

#endif /* FTFS_INDIRECT_H */
