/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef FTFS_MSG_VAL_H
#define FTFS_MSG_VAL_H


/* This is shared by betrfs and the ft code. There are some
 * structs being processed by both of them. The most obvious
 * example is the struct for the indirect value.
 * Another example is ftnode writeback where the ft node passes
 * a struct to betrfs.
 *
 * There are macros used by both parties as well.
 */

/* This is the definition of indirect value. During
 * betrfs writeback code, for each page being written
 * there is a such stuct for it. `pfn` is the PFN
 * for the page. `size` is only useful for the last
 * page of a file
 */
struct ftfs_indirect_val {
    unsigned long pfn;
    unsigned int size;
};

typedef struct ftfs_indirect_val indirect_msg;

#define IND_VAL_FROM_PTR(p) (struct ftfs_indirect_val *)((p))

/*
 * This defines an interface to the southbound that indicates
 * all of the crap in a node to be gathered up and written to disk.
 *
 * Layout1: Node=header::Array(partitions) and Partition=BST(k,v)::indirect_value.
 * Layout2: Node=header::Array(BST(k,v))::Array(indirect_value).
 * If we use Layout1, where the begin_pfns are only for the node header and node info.
 * Otherwise, begin_pfns also contains the index parts which are BSTs of <key, indirect value>.
 *
 * begin_pfns: the node header, only one for a node. It is an array of pfns
 * begin_pfn_cnt: the length of begin_pfns
 * bp_index_pfns_arr: the k-v part, per partition. It is an array of pfn arrarys.
 * bp_index_cnt_arr: It is an array of length for the pfn arrays of the bp_index_pfns_arr.
 * bp_pfns_arr: for indirect values with size 4kib, per partition. It is an array of pfn arrays.
 * bp_pfns_cnt_arr: it is an array of length for the pfn arrays of the bp_index_pfns_arr.
 * num_bp: number of partitions
 * fd: file descriptor
 */
struct bp_serialize_extra {
    unsigned long *begin_pfns;
    unsigned int begin_pfn_cnt;
    unsigned long **bp_index_pfns_arr;
    unsigned int *bp_index_cnt_arr;
    unsigned long **bp_pfns_arr;
    unsigned int *bp_cnt_arr;
    /* Use when compression is enabled */
    unsigned char **bp_compressed_ptr_arr;
    long *bp_compressed_size_arr;
    int num_bp;
    int fd;
};


#ifdef __KERNEL__
#include <linux/version.h>
#include <linux/gfp.h>

/* Use when allocate page for leaf or nonleaf
 * It uses the same gfp_mask as page cache alloc
 */
#if LINUX_VERSION_CODE == KERNEL_VERSION(3,11,10)
  #define FTFS_READ_PAGE_GFP (__GFP_WRITE | GFP_HIGHUSER_MOVABLE)
#elif LINUX_VERSION_CODE ==  KERNEL_VERSION(4,19,99) /* LINUX_VERSION_CODE */
  #define FTFS_READ_PAGE_GFP (__GFP_RECLAIM | __GFP_HARDWALL | __GFP_WRITE | __GFP_MOVABLE | __GFP_HIGHMEM)
#else /* LINUX_VERSION_CODE */
  #define FTFS_READ_PAGE_GFP (GFP_HIGHUSER_MOVABLE)
  #warning "You may need to redefine FTFS_READ_PAGE_GFP if page allocation failed!"
#endif /* LINUX_VERSION_CODE */

#endif /* __KERNEL__ */

#define FTFS_PAGE_SIZE (4096)
#define FTFS_COPY_VAL_THRESHOLD (512)
#define FTFS_PAGE_SIZE_SHIFT (12)
#define FTFS_MSG_VAL_SIZE (sizeof(struct ftfs_indirect_val))
#define FTFS_MSG_HEX_MAX (0xffffffffffffffff)
#define NR_BYTES_ULONG (8)
/* Skip this index when doing range query */
#define FTFS_READAHEAD_INDEX_SKIP  (ULONG_MAX)

///////////////////////////////////////
//////// Large Value Defines //////////
///////////////////////////////////////
#define FTFS_MSG_BITMAP_LEN (32)
#define FTFS_MSG_PAGE_SHIFT (0)
#define FTFS_MSG_KEY_MASK (FTFS_MSG_HEX_MAX - ((1UL << FTFS_MSG_PAGE_SHIFT) - 1))
#define FTFS_MSG_NR_PAGES (1 << FTFS_MSG_PAGE_SHIFT)

////////////////////////////////////////
// ftfs_page_private->private flags ////
////////////////////////////////////////
#define FT_MSG_VAL_BOUND_BIT (0)		/* UBI in nonleaf written to disk */
#define FT_MSG_VAL_TRUNCATE_BIT (1)		/* Page Cache in truncated */
#define FT_MSG_VAL_LEAF_BIT (2)			/* The page is referenced by a leaf */
#define FT_MSG_VAL_NONLEAF_BIT (3)		/* The page is referenced by a nonleaf */

#define FT_MSG_VAL_NB_WRITE (4)			/* The page is inserted by northbound for write */
#define FT_MSG_VAL_APPLIED_BIT (5)		/* The page is applied to basement node for query */
#define FT_MSG_VAL_FLUSH_TOLEAF_BIT (6)		/* The page is flushed to leaf */
#define FT_MSG_VAL_FLUSH_TONONLEAF_BIT (7)	/* The page is flushed to nonleaf */

#define FT_MSG_VAL_NB_READ (8)			/* For read path */
#define FT_MSG_VAL_CLR_BY_FT (9)		/* For priv flag cleared by FT */
#define FT_MSG_VAL_CLR_BY_VFS (10)		/* For priv flag cleared by VFS */
#define FT_MSG_VAL_FOR_CLONE (11)		/* For priv flag for clone */

#define FT_MSG_VAL_BN_PAGE_PUT (12)		/* For put bn page in ft-indirect.cc*/
#define FT_MSG_VAL_BN_PAGE_FREE (13)		/* For free bn page in ft-indirect.cc */
#define FT_MSG_VAL_NL_PAGE_PUT (14)		/* For put nl page in ft-indirect.cc */
#define FT_MSG_VAL_NL_PAGE_FREE (15)		/* For free nl page in ft-indirect.cc */

#define FT_MSG_VAL_GC_1 (16)			/* For gc1 in ule.cc */
#define FT_MSG_VAL_GC_2 (17)			/* For gc2 in ule.cc */
#define FT_MSG_VAL_GC_3 (18)			/* For gc3 in ule.cc */
#define FT_MSG_VAL_GC_4 (19)			/* For gc4 in ule.cc */

#endif /* FTFS_MSG_VAL_H */
