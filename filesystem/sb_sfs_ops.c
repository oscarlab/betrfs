/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/kernel.h>
#include <linux/mutex.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/file.h>
#include <linux/types.h>
#include <linux/namei.h>
#include <linux/path.h>
#include <linux/fs.h>
#include <linux/dcache.h>
#include <linux/fsnotify.h>
#include <linux/security.h>
#include <linux/uio.h>
#include <linux/mm.h>
#include <linux/falloc.h>
#include <linux/buffer_head.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/printk.h>
#include <linux/bitmap.h>

/*
 * This file is part of the southbound code.
 * It implements direct IO support on the Simple File Layer (aka SFS).
 */
#include "../simplefs/super.h"
#include "sb_files.h"
#include "ftfs_indirect.h"

#include "../simplefs/sfs_dio.c"

/* rw=1 for write; rw=0 for read */
#define SFS_OP_WR 1
#define SFS_OP_RD 0

int sb_sfs_dio_fsync(int fd)
{
	struct fd f;
	struct file *file;
	int ret = -EINVAL;

	f = sb_fdget(fd);
	file = f.file;
	if (file) {
		ret = sfs_dio_fsync(file, 0, ULONG_MAX);
	}
	sb_fdput(f);
	return ret;
}

/*
 * Remove all pages in the page cache for this file
 * needed to pass unit tests when dio interfaces are used
 */
int sb_sfs_truncate_page_cache(int fd)
{
	struct fd f;
	struct file *file;
	struct inode *inode;
	unsigned long nr_pages = 0;

	f = sb_fdget(fd);
	file = f.file;
	if (file) {
		inode = file_inode(file);
		if (!inode->i_mapping) {
			nr_pages = 0;
		} else {
			nr_pages = inode->i_mapping->nrpages;
			truncate_inode_pages(inode->i_mapping, 0);
		}
	}
	sb_fdput(f);
	return nr_pages;
}

bool sb_sfs_is_which_file(int fd, const char *name)
{
	struct fd f;
	struct file *file;
	struct dentry *dentry;
	bool ret = false;

	f = sb_fdget(fd);
	file = f.file;
	if (!file) {
		goto out;
	}
	dentry = file->f_path.dentry;
	if (!memcmp(dentry->d_name.name, name, strlen(name)))
		ret =true;
out:
	sb_fdput(f);
	return ret;
}

bool sb_is_data_db(int fd)
{
	return sb_sfs_is_which_file(fd, "ftfs_data");
}

bool sb_is_meta_db(int fd)
{
	return sb_sfs_is_which_file(fd, "ftfs_meta");
}

ssize_t sb_sfs_dio_read_write(int fd, uint8_t *raw_block,
			      int64_t size, int64_t offset, int rw,
			      void (*free_cb) (void*))
{
	struct fd f;
	struct file *file;
	ssize_t ret = 0;
	struct address_space *mapping;
	unsigned long nrpages = 0;

	if (offset < 0)
		return -EINVAL;

	f = sb_fdget(fd);
	file = f.file;
	if (file) {
		mapping = file_inode(file)->i_mapping;
		if (mapping)
			nrpages = mapping->nrpages;
		/*
		 * If we are using DIO interface
		 * do not expect there is any page
		 * cached pages in the radix tree
		 */
		BUG_ON(nrpages != 0);
		ret = sfs_directIO_read_write(file,
			raw_block, size, offset,
			rw, free_cb);
	}
	sb_fdput(f);
	return ret;
}

ssize_t sb_sfs_dio_read(int fd, uint8_t *raw_block, int64_t size,
			int64_t offset, void (*free_cb) (void*))
{
	return sb_sfs_dio_read_write(fd, raw_block, size, offset, SFS_OP_RD, free_cb);
}

ssize_t sb_sfs_dio_write(int fd, uint8_t *raw_block, int64_t size,
			 int64_t offset, void (*free_cb) (void*))
{
	return sb_sfs_dio_read_write(fd, raw_block, size, offset, SFS_OP_WR, free_cb);
}

/*
 * During writeback, in some cases the ft code will clone/copy a node (for reasons here),
 * which is not handled by the page count tracking.
 * Track reference counts properly when a node is cloned,
 * and add additional debugging support, which can be toggled at
 * compile time with the -DDEBUG_PAGE_SHARING flag.
 */
static inline bool is_cloned_node_data_page(struct page *page)
{
	return PagePrivate2(page);
}

static void process_cloned_node_data_page(struct page *page)
{
	ftfs_dec_page_private_count(page);
	ClearPagePrivate2(page);
}

/////////////////////////////////////////////////////////////////////////
//////////////////////// For write with page sharing ////////////////////
/////////////////////////////////////////////////////////////////////////
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
static void ftnode_page_end_io(struct bio *bio, int err)
{
	const int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
	struct bio_vec *bvec = bio->bi_io_vec + bio->bi_vcnt - 1;

	do {
		struct page *page = bvec->bv_page;
		if (--bvec >= bio->bi_io_vec)
			prefetchw(&bvec->bv_page->flags);
		if (bio_data_dir(bio) == READ) {
			if (uptodate) {
				SetPageUptodate(page);
			} else {
				printk("%s: pfn=%lx\n", __func__, page_to_pfn(page));
				BUG();
				ClearPageUptodate(page);
				SetPageError(page);
			}
			ftfs_unlock_page(page);
		} else { /* bio_data_dir(bio) == WRITE */
			if (!uptodate) {
				SetPageError(page);
			}
			if (PageReserved(page)) {
				/* Encounter data page of a cloned node */
				if (is_cloned_node_data_page(page)) {
					process_cloned_node_data_page(page);
				}
				ftfs_unlock_page(page);
			} else {
				BUG();
			}
		}
	} while (bvec >= bio->bi_io_vec);
	bio_put(bio);
}

static struct bio *ftnode_page_bio_submit(int rw, struct bio *bio)
{
	bio->bi_end_io = ftnode_page_end_io;
	submit_bio(rw, bio);
	return NULL;
}
#else /* LINUX_VERSION_CODE */
/*
 * It is the callback function registered for BIO
 * Block layer will call this when IO is done.
 */
static void sb_page_endio(struct page *page, bool is_write, int err)
{
	if (!is_write) {
		if (!err) {
			SetPageUptodate(page);
		} else {
			ClearPageUptodate(page);
			SetPageError(page);
		}
		ftfs_unlock_page(page);
	} else {
		BUG_ON(err);
		if (PageReserved(page)) {
			/* Encounter data page of a cloned node */
			if (is_cloned_node_data_page(page)) {
				process_cloned_node_data_page(page);
			}
			ftfs_unlock_page(page);
		} else {
			printk("Encounter not reserved page\n");
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
	}
}

static void ftnode_page_end_io(struct bio *bio)
{
	struct bio_vec *bv;
	int i;

	bio_for_each_segment_all(bv, bio, i) {
		struct page *page = bv->bv_page;
		sb_page_endio(page, bio_op(bio),
			   blk_status_to_errno(bio->bi_status));
	}
	bio_put(bio);
}

static struct bio *ftnode_page_bio_submit(int rw, struct bio *bio)
{
	bio->bi_end_io = ftnode_page_end_io;
	bio_set_op_attrs(bio, rw, 0);
	submit_bio(bio);
	return NULL;
}
#endif /* LINUX_VERSION_CODE */

struct page *ftfs_prepare_write_page(unsigned long pfn,
				     bool is_index,
				     bool is_cloned)
{
	struct page *page;

	if (!pfn_valid(pfn)) {
		printk("invalid pfn=%lu\n", pfn);
		BUG();
	}

	page = pfn_to_page(pfn);

	/* Sanity Check: Page is shared with upper layer radix tree */
	if (PagePrivate(page)) {
		if (is_index) {
			printk("Encounter private index page\n");
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
		if (page->mapping == NULL || ftfs_read_page_count(page) == 0) {
			printk("Encounter private page with null mapping\n");
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
	}
	/*
	 * After a node is read, we can return it to the ft
	 * layer before the reads all complete. In the worst case
	 * a read may still be pending when we try to write the
	 * node back to disk. We use prepare_write to catch this
	 * case and wait on the read to finish.
	 *
	 * For cloned node, we just confirm the data page has been locked.
	 * `private2` flag is not on and turn it on here so that
	 * the IO callback function can handle it differently
	 */
	if (is_cloned && !is_index) {
		//printk("is_cloned=%d, is_index=%d\n", is_cloned, is_index);
		if (!PageReserved(page)) {
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
		BUG_ON(PagePrivate2(page));
		SetPagePrivate2(page);
	} else if (PageReserved(page)) {
		wait_on_page_bit(page, PG_reserved);
	}
	return page;
}

struct pfn_array {
	unsigned long *pfns;
	int nr_pages;
	bool is_index;
	bool is_leaf;
	bool is_cloned;
};

static void ftfs_write_page_array(struct inode *inode, struct pfn_array *arr,
				  struct sfs_page_data *spd, loff_t *pos)
{
	unsigned long *pfn_arr = arr->pfns;
	unsigned int nr_pages = arr->nr_pages;
	struct page *page;
	int p, ret;
	bool is_index = arr->is_index;
	bool is_cloned = arr->is_cloned;
	bool skip_lock = is_cloned && (!is_index);

	if (pfn_arr == NULL || nr_pages == 0)
		return;
	if (inode->i_mapping->nrpages > 0) {
		printk("Page cache is not empty\n");
		BUG();
	}

	for (p = 0; p < nr_pages; p++) {
		page = ftfs_prepare_write_page(pfn_arr[p], is_index, is_cloned);
		ret = sfs_read_write_one_page(inode, page, *pos, spd, WRITE, skip_lock);
		*pos += 4096;
	}
}

ssize_t ftfs_write_ft_node(struct bp_serialize_extra *extra,
			   loff_t pos, bool is_leaf, bool is_cloned)
{
	struct fd f;
	ssize_t ret = -EBADF;
	struct file *file;
	struct dentry *dentry;
	struct inode *inode;
	struct pfn_array arr;
	int i;

	struct sfs_page_data spd = {
		.bio = NULL,
		.last_block_in_bio = 0,
		.end_cb = ftnode_page_end_io
	};

	if (pos < 0)
		return -EINVAL;

	f = sb_fdget(extra->fd);
	file = f.file;
	if (!file)
		return ret;

	dentry = file->f_path.dentry;
	inode = dentry->d_inode;

	arr.pfns = extra->begin_pfns;
	arr.nr_pages = extra->begin_pfn_cnt;
	arr.is_leaf = is_leaf;
	arr.is_cloned = is_cloned;

	arr.is_index = true; /* node header */
	ftfs_write_page_array(inode, &arr, &spd, &pos);

	if (!extra->bp_pfns_arr && !extra->bp_index_pfns_arr)
		goto done;
	for (i = 0; i < extra->num_bp; i++) {
		if (extra->bp_index_pfns_arr) {
			arr.pfns = extra->bp_index_pfns_arr[i];
			arr.nr_pages = extra->bp_index_cnt_arr[i];
			arr.is_index = true; /* kv part of partitions */
			ftfs_write_page_array(inode, &arr, &spd, &pos);
		}
		if (extra->bp_pfns_arr) {
			arr.pfns = extra->bp_pfns_arr[i];
			arr.nr_pages = extra->bp_cnt_arr[i];
			arr.is_index = false; /* pfn of partitions */
			ftfs_write_page_array(inode, &arr, &spd, &pos);
		}
	}
done:
	if (spd.bio != NULL) {
		ftnode_page_bio_submit(WRITE, spd.bio);
	}
	sb_fdput(f);
	return ret;
}

ssize_t ftfs_write_ft_leaf(struct bp_serialize_extra *extra, loff_t pos, bool is_cloned)
{
	return ftfs_write_ft_node(extra, pos, true, is_cloned);
}

ssize_t ftfs_write_ft_nonleaf(struct bp_serialize_extra *extra, loff_t pos, bool is_cloned)
{
	return ftfs_write_ft_node(extra, pos, false, is_cloned);
}

/////////////////////////////////////////////////////////////////////////
//////////////////////// For read with page sharing /////////////////////
/////////////////////////////////////////////////////////////////////////
size_t sb_async_read_pages(int fd, unsigned long *pfn_node,
			     int32_t nr_pages, loff_t pos)
{
	struct fd f;
	int ret;

	f = sb_fdget(fd);
	if (!f.file)
		return -EINVAL;
	ret = sfs_async_read_write_pages(f.file,
					 pfn_node,
					 nr_pages,
					 pos,
					 READ,
					 false /* is_blocking */
                                         );
	sb_fdput(f);
	return ret;
}

/* Wait for read page by waiting on the lock flag */
void sb_wait_read_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	wait_on_page_bit(page, PG_reserved);
}

/////////////////////////////////////////////////////////////////////////
//////////////////////////// Unitlity Functions /////////////////////////
/////////////////////////////////////////////////////////////////////////
/*
 * Allocate page for basement node prefetching.
 * can only happen for leaf node
 */
unsigned long sb_alloc_leaf_page(void)
{
#ifdef FT_INDIRECT
	unsigned long pfn = ftfs_alloc_page();
	return pfn;
#else
	struct page *page;
	page = alloc_page(GFP_KERNEL);
	return page_to_pfn(page);
#endif
}

/*
 * For memleak debug to distinguish
 * leaf and nonleaf allocation
 */
unsigned long sb_alloc_nonleaf_page(void)
{
	return sb_alloc_leaf_page();
}

/*
 * Free the page in the ft-index code for a leaf node
 * if its data are prefetched.
 */
void sb_free_leaf_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
#ifdef FT_INDIRECT
	BUG_ON(!page->private);
	kfree((struct ftfs_page_private *)page->private);
#endif
	__free_pages(page, 0);
}

/* Lock the page first before issue prefetching IO to block layer */
void sb_lock_read_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	lock_page(page);
}

bool sb_virt_addr_valid(void *kaddr)
{
	return  virt_addr_valid(kaddr);
}

unsigned long ftfs_get_kernfs_pfn(unsigned long start)
{
	return get_kernfs_pfn(start);
}

bool ftfs_check_page_state(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	return PageReserved(page);
}
