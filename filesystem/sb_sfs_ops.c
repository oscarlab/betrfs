#include <linux/types.h>
#include <linux/printk.h>
#include <linux/fs.h>
#include <linux/file.h>

/* This file is part of the southbound code.
 *
 * It implements direct IO support on the Simple File Layer (aka SFS).
 */

#include "../simplefs/sfs_dio.c"
#include "sb_files.h"

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
	struct dentry *dentry;
	unsigned long nr_pages = 0;

	f = sb_fdget(fd);
	file = f.file;
	if (file) {
		dentry = file->f_path.dentry;
		inode = dentry->d_inode;
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

bool sb_sfs_is_data_db(int fd)
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
	if (!memcmp(dentry->d_name.name, "ftfs_data", 9))
		ret =true;
out:
	sb_fdput(f);
	return ret;
}

ssize_t sb_sfs_dio_read_write(int fd, uint8_t *raw_block,
			      int64_t size, int64_t offset, int rw,
			      void (*free_cb) (void*))
{
	struct fd f;
	struct file *file;
	ssize_t ret = 0;
	struct inode *inode;
	struct dentry *dentry;
	struct address_space *mapping;
	unsigned long nrpages = 0;

	if (offset < 0)
		return -EINVAL;

	f = sb_fdget(fd);
	file = f.file;
	if (file) {
		dentry = file->f_path.dentry;
		inode = dentry->d_inode;
		mapping = inode->i_mapping;
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
			int64_t offset,
			void (*free_cb) (void*))
{
	return sb_sfs_dio_read_write(fd, raw_block, size, offset, SFS_OP_RD, free_cb);
}

ssize_t sb_sfs_dio_write(int fd, uint8_t *raw_block, int64_t size,
			int64_t offset,
			void (*free_cb) (void*))
{
	return sb_sfs_dio_read_write(fd, raw_block, size, offset, SFS_OP_WR, free_cb);
}

/* Use for basement node prefetching in ft-index code */
size_t sb_read_pages(int fd, unsigned long *pfn_arr, int32_t nr_pages, loff_t pos)
{
	struct fd f;
	ssize_t ret = -EBADF;

	if (pos < 0 || nr_pages <= 0)
		return -EINVAL;

	f = sb_fdget(fd);
	if (!f.file)
		return -EINVAL;
	ret = sfs_read_pages(f.file, pfn_arr, nr_pages, pos);
	sb_fdput(f);
	return ret;
}
/* Wait for read page by waiting on the lock flag */
void sb_wait_read_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	wait_on_page_locked(page);
}

/* Map pfn to address so that the data can be copied */
char *sb_map_page(unsigned long pfn)
{
	struct page *page;
	char *buf;
	BUG_ON(!pfn_valid(pfn));
	page = pfn_to_page(pfn);
	sb_wait_read_page(pfn);
	buf = (char*)kmap(page);
	return buf;
}

/* Ummap the pfn if it has been mapped */
void sb_unmap_page(unsigned long pfn)
{
	kunmap(pfn_to_page(pfn));
}

/*
 * Allocate page for basement node prefetching.
 * can only happen for leaf node
 */
unsigned long sb_alloc_leaf_page(void)
{
	struct page *page;
	page = alloc_page(GFP_KERNEL);
	return page_to_pfn(page);
}

/*
 * Free the page in the ft-index code for a leaf node
 * if its data are prefetched.
 */
void sb_free_leaf_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	__free_pages(page, 0);
}

/*
 * Use for async write to distinguish checkpoint threads from others.
 * Only implement async write for checkpoint threads
 */
char *sb_get_proc_name(void)
{
	return current->comm;
}

/* Lock the page first before issue prefetching IO to block layer */
void sb_lock_read_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	lock_page(page);
}
