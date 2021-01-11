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
#include <linux/pagemap.h>
#include <linux/pagevec.h>
#include <linux/vmalloc.h>

#include "super.h"

int get_kernfs_pages(unsigned long start, int nr_pages, int write,
		     struct page **pages)
{
	int i;
	if (is_vmalloc_addr((void *)start)) {
		for (i = 0; i < nr_pages; i++) {
			struct page *page;
			write = *(int *)start;
			page = vmalloc_to_page((void *)start);
			BUG_ON(page == NULL);
			pages[i] = page;
			start += PAGE_SIZE;
		}
	} else {
		int ret;
		WARN_ON(!PAGE_ALIGNED(start));
		for (i = 0; i < nr_pages; i++) {
			const struct kvec kiov = {
				.iov_base = (void *)start,
				.iov_len = PAGE_SIZE
			};
			ret = get_kernel_pages(&kiov, 1, write, pages + i);
			if (ret < 0)
				return i;
			start += PAGE_SIZE;
		}

	}
	return i;
}

unsigned long get_kernfs_pfn(unsigned long start)
{
	struct page *page;
	int write = 0;
	int ret;

	ret = get_kernfs_pages(start, 1, write, &page);
	return page_to_pfn(page);
}

void sfs_wait_read_write_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	wait_on_page_locked(page);
}

typedef void (*ftfs_free_cb)(void *);

struct sfs_dio_elem {
	uint8_t *raw_block;
	int64_t off;
	unsigned long *pfn_arr;
	int nr_pages;
	ftfs_free_cb free_cb;
	struct list_head dio_list;
};

/*
 * Use for asynchronous write with DIO interface. We add the information
 * related to the write request, e.g. offset, size, data buf to a linked
 * list so that we check whether all the data has been written to disk
 * later. It happens when a dirty node is evicted from cachetable,
 * but later needs to be read to memory again. There should have at most
 * one match on the dio write bookkeeping list. If an outstanding dio
 * write's range is overlapped with a previous write's range, the code
 * would wait for the previous write to become durable and remove its
 * item on the bookkeeping list.
 */
static int sfs_insert_dio_data(struct file *file, uint8_t *raw_block,
			       int nr_pages, unsigned long *pfn_arr,
			       int64_t off, ftfs_free_cb ftfs_free)
{
	struct inode *inode;
	struct sfs_dio_elem *dio_elem;
	struct sfs_dio_data *dio_data;
	struct sfs_inode *sfs_inode;

	inode = file->f_path.dentry->d_inode;
	sfs_inode = SFS_INODE(inode);

	dio_elem = kmalloc(sizeof(*dio_elem), GFP_KERNEL);
	BUG_ON(!dio_elem);
	INIT_LIST_HEAD(&dio_elem->dio_list);
	dio_elem->raw_block = raw_block;
	dio_elem->nr_pages = nr_pages;
	dio_elem->off = off;
	dio_elem->pfn_arr = pfn_arr;
	dio_elem->free_cb = ftfs_free;

	BUG_ON(!sfs_inode->dio);
	dio_data = sfs_inode->dio;

	mutex_lock(&dio_data->lock);
	//printk("%s: inode->i_ino=%lu get the lock, pid=%d\n", __func__, inode->i_ino, current->pid);
	list_add(&dio_elem->dio_list, &dio_data->data);
	dio_data->nr_items += 1;
	//printk("%s: inode->i_ino=%lu release the lock, pid=%d\n", __func__, inode->i_ino, current->pid);
	mutex_unlock(&dio_data->lock);
	return 0;
}

struct sfs_dio_request {
	struct file *file;
	int64_t size;
	int64_t offset;
	int rw;
	/*
	 * if true, data is pfn_arr;
	 * otherwise, data is raw buf.
	 */
	bool is_pfn;
	void *data;
};

static inline struct sfs_dio_data *sfs_get_dio(struct file *f)
{
	struct inode *inode;
	struct sfs_inode *sfs_inode;

	inode = f->f_path.dentry->d_inode;
	sfs_inode = SFS_INODE(inode);
	BUG_ON(!sfs_inode->dio);

	return sfs_inode->dio;
}

/*
 * We want to read from disk, but the previous async write may haven't completed
 * yet. Therefore, check the bookkeeping list of dio async writes and copy the
 * data we want to read. If no relevant data is found on the list, we fall back
 * to issue read request to disk. pfn_arr is the array of pages to hold the read data
 */
bool sfs_search_dio_data(struct sfs_dio_request *req)
{
	int nr_pages = req->size >> PAGE_SHIFT;
	unsigned long *pfn_arr = NULL;
	int64_t offset = req->offset;
	int64_t size = req->size;
	struct sfs_dio_elem *dio;
	struct sfs_dio_data *dio_data;
	struct list_head *pos, *p;
	int64_t item_size;
	int found_cnt = 0;
	int pfn_off, i;

	if (req->is_pfn) {
		pfn_arr = (unsigned long*) req->data;
	}

	dio_data = sfs_get_dio(req->file);
	mutex_lock(&dio_data->lock);
	list_for_each_safe(pos, p, &dio_data->data) {
		dio = list_entry(pos, struct sfs_dio_elem, dio_list);
		item_size = dio->nr_pages << PAGE_SHIFT;
		if (dio->off >= offset + size || dio->off + item_size <= offset)
			continue;
		found_cnt += 1;
		if (req->rw == WRITE) {
			/* FIXME: this waits on all pages in the dio
			 * if any pages are overlapping. This could
			 * potentially be optimized by only waiting
			 * on overlapping pages.
			 */
			for (i = 0; i < dio->nr_pages; i++) {
				wait_on_page_locked(pfn_to_page(dio->pfn_arr[i]));
			}
			kfree(dio->pfn_arr);
			dio->free_cb(dio->raw_block);
			list_del(pos);
			kfree(dio);
			continue;
		}
		/*
		 * If I have a hit here: the range for the query is fully
		 * contained and only one matched item. At this point,
		 * as an optimization, you can service the read once the disk
		 * write finishes. Note that a write would have continued,
		 * and we only attempt this if the range is fully contained
		 * by a prior write. The assertions below check that we
		 * can never have a pending read that partially overlaps
		 * our desired range.
		 */
		if (dio->off + item_size < offset + size || found_cnt > 1) {
			printk("(1) Invaid offset and size\n");
			BUG();
		}
		pfn_off = (offset - dio->off) >> PAGE_SHIFT;
		/* For write, we do not need this check */
		if (item_size < size || pfn_off < 0) {
			printk("(2) Invaid offset and size\n");
			BUG();
		}
		for (i = 0; i < nr_pages; i++) {
			char *src = NULL;
			unsigned long pfn_s = dio->pfn_arr[i+pfn_off];
			if (!pfn_valid(pfn_s)) {
				printk("Encounter invalid pfn_s=%lu\n", pfn_s);
				BUG();
			}
			wait_on_page_locked(pfn_to_page(pfn_s));
			src = kmap(pfn_to_page(pfn_s));
			if (req->is_pfn) {
				unsigned long pfn_d= pfn_arr[i];
				void *dest = kmap(pfn_to_page(pfn_d));
				memcpy(dest, src, PAGE_SIZE);
				kunmap(pfn_to_page(pfn_d));
			} else {
				memcpy((uint8_t*)req->data + (i << PAGE_SHIFT), src, PAGE_SIZE);
			}
			kunmap(pfn_to_page(pfn_s));
		}
	}
	mutex_unlock(&dio_data->lock);
	return found_cnt != 0;
}

static struct bio *
sfs_bio_alloc(struct block_device *bdev, sector_t first_sector)
{
	struct bio *bio;

	bio = bio_alloc(GFP_NOIO, BIO_MAX_PAGES);
	if (!bio) {
		printk("Alloc bio failed%d\n", __LINE__);
		BUG();
	} else {
		bio->bi_bdev = bdev;
		bio->bi_sector = first_sector;
	}
	return bio;
}

/*
 * define memory free function passed from ftfs.ko
 */

struct sfs_page_data {
	struct bio *bio;
	sector_t last_block_in_bio;
	atomic_t bio_count;
	void *raw_block;
	ftfs_free_cb free_cb;
};

static void sfs_page_end_io(struct bio *bio, int err)
{
	/*
	 * YZJ: Check the return value of IO completion 
	 */
	const int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
	struct bio_vec *bvec = bio->bi_io_vec + bio->bi_vcnt - 1;

	do {
		struct page *page = bvec->bv_page;
		if (--bvec >= bio->bi_io_vec)
			prefetchw(&bvec->bv_page->flags);
		if (bio_data_dir(bio) == READ) {
			if (!uptodate) {
				printk(KERN_ALERT "Read is not ok\n");
				BUG();
			}
			SetPageUptodate(page);
			unlock_page(page);
		} else { /* bio_data_dir(bio) == WRITE */
			if (!uptodate) {
				printk(KERN_ALERT "Write is not ok\n");
				BUG();
			}
			unlock_page(page);
		}
	} while (bvec >= bio->bi_io_vec);

	bio_put(bio);
}

static struct bio *sfs_page_bio_submit(struct sfs_page_data *spd,
			int rw, struct bio *bio)
{
	bio->bi_end_io = sfs_page_end_io;
	submit_bio(rw, bio);
	atomic_inc(&spd->bio_count);
	return NULL;
}

int sfs_read_write_one_page(struct inode *inode, struct page *page,
		off_t off, void *data, int rw, bool is_locked)
{
	struct sfs_page_data *spd = data;
	struct bio *bio = spd->bio;
	const unsigned blkbits = inode->i_blkbits;
	const unsigned blocks_per_page = PAGE_SIZE >> blkbits;
	sector_t block_in_file;
	sector_t block_nr;
	struct block_device *bdev = NULL;
	int length;
	int ret = 0;

	BUG_ON(blocks_per_page != 1);
	block_in_file = (sector_t) off >> blkbits;

	{
		/*
		 * YZJ: block allocation
		 */
		struct sfs_inode *sfs_inode;
		sfs_inode = SFS_INODE(inode);
		BUG_ON(block_in_file >= sfs_inode->block_count);
		block_nr = sfs_inode->start_block_number + block_in_file;
		bdev = inode->i_sb->s_bdev;
	}
  
	if (!is_locked)
		lock_page(page);

	/*
	 * YZJ: Submit a new bio if the block address is not contiguous
	 */
	if (bio && spd->last_block_in_bio != block_nr - 1) {
		bio = sfs_page_bio_submit(spd, rw, bio);
	}
alloc_new:
	if (bio == NULL) {
		/*
		 * YZJ: Linux always considers sectors to be 512 bytes
		 * long independently of the devices real block size.
		 */
		bio = sfs_bio_alloc(bdev, block_nr << (blkbits - 9));
		bio->bi_private = spd;
	}

	length = 1 << blkbits;
	if (bio_add_page(bio, page, length, 0) < length) {
		bio = sfs_page_bio_submit(spd, rw, bio);
		goto alloc_new;
	}

	spd->last_block_in_bio = block_nr;
	spd->bio = bio;
	return ret;
}

static void sfs_fill_spd(void *raw_block, int rw,
		ftfs_free_cb ftfs_free, struct sfs_page_data *spd)
{
	BUG_ON(spd == NULL);
	spd->bio = NULL,
	spd->last_block_in_bio = 0,
	spd->raw_block = raw_block,
	spd->free_cb = (rw == WRITE) ? ftfs_free : NULL,
	atomic_set(&spd->bio_count, 0);
}

ssize_t sfs_read_write_pages(struct file *file, unsigned long *pfn_arr,
		int nr_pages, loff_t pos, void *raw_block, int rw,
		struct sfs_page_data *spd)
{
	ssize_t ret = -EBADF;
	struct dentry *dentry;
	struct inode *inode;
	struct sfs_inode *sfs_inode;
	int p;
	//struct blk_plug plug;

	dentry = file->f_path.dentry;
	inode = dentry->d_inode;
	sfs_inode = SFS_INODE(inode);
	/*
	 * YZJ: allow an I/O submitter to send down multiple pieces of
	 * I/O before handing it to the device.
	 */
	//blk_start_plug(&plug);

	for (p = 0; p < nr_pages; p++) {
		struct page *page = pfn_to_page(pfn_arr[p]);
		sfs_read_write_one_page(inode, page, pos, spd, rw, false);
		pos += PAGE_SIZE;
	} 
	if (spd->bio != NULL) {
		sfs_page_bio_submit(spd, rw, spd->bio);
	}
	//blk_finish_plug(&plug);
	return ret;
}

ssize_t sfs_directIO_read_write(struct file *file, uint8_t *raw_block,
			int64_t size, int64_t offset, int rw,
			ftfs_free_cb ftfs_free)
{
	int i;
	int nr_pages;
	unsigned long *pfn_arr;
	unsigned long addr;
	struct sfs_page_data spd;
	bool cached = false;

	BUG_ON((size & (PAGE_SIZE - 1)) != 0);
	nr_pages= size >> PAGE_SHIFT;
	{
		struct sfs_dio_request req = {
			.file = file,
			.size = size,
			.offset = offset,
			.rw = rw,
			.data = raw_block,
			.is_pfn = false
		};
		cached = sfs_search_dio_data(&req);
		/* If it is read just return it is cached */
		if (cached && rw == READ) {
			//printk("%s: read from dio cache\n", __func__);
			goto out;
		}
	}

	pfn_arr = kmalloc(sizeof(unsigned long) * nr_pages, GFP_KERNEL);
	BUG_ON(!pfn_arr);

	for (i = 0; i < nr_pages; i++) {
		addr = (unsigned long) raw_block + i * PAGE_SIZE;
		pfn_arr[i] = get_kernfs_pfn(addr);
	}

	sfs_fill_spd(raw_block, rw, ftfs_free, &spd);
	sfs_read_write_pages(file, pfn_arr, nr_pages, offset,
			     raw_block, rw, &spd);
	if (rw == WRITE && ftfs_free) {
		sfs_insert_dio_data(file, raw_block, nr_pages,
				    pfn_arr, offset, ftfs_free);
	} else {
		for (i = 0; i < nr_pages; i++) {
			sfs_wait_read_write_page(pfn_arr[i]);
		}
		kfree(pfn_arr);
	}
out:
	return (ssize_t) size;
}

/*
 * This does an asynchronous, multi-page read of an array of pages in SFS.
 * This assumes the file is continguous on disk, but not necessarily in memory.
 */
ssize_t sfs_read_pages(struct file *file, unsigned long *pfn_arr,
		       int nr_pages, loff_t pos)
{
	ssize_t ret = nr_pages << PAGE_SHIFT;
	struct dentry *dentry;
	struct inode *inode;
	struct sfs_inode *sfs_inode;
	struct sfs_page_data spd;
	struct blk_plug plug;
	bool cached = false;
	int p;

	struct sfs_dio_request req = {
		.file = file,
		.size = ret,
		.offset = pos,
		.is_pfn = true,
		.data = pfn_arr,
		.rw = READ
	};

	cached = sfs_search_dio_data(&req);
	/* If it is read just return it is cached */
	if (cached) {
		goto out;
	}

	dentry = file->f_path.dentry;
	inode = dentry->d_inode;
	sfs_inode = SFS_INODE(inode);
	sfs_fill_spd(NULL, 0, NULL, &spd);

	blk_start_plug(&plug);
	for (p = 0; p < nr_pages; p++) {
		struct page *page = pfn_to_page(pfn_arr[p]);
		sfs_read_write_one_page(inode, page, pos, &spd, 0, true);
		pos += PAGE_SIZE;
	} 
	if (spd.bio != NULL) {
		sfs_page_bio_submit(&spd, 0, spd.bio);
	}
	blk_finish_plug(&plug);
out:
	return ret;
}

/*
 * This is a helper function for fsync, and not
 * expected to be used anywhere else.
 */
static int sfs_wait_dio_data(struct file* file, struct inode *inode,
			     loff_t start, loff_t end)
{
	struct sfs_dio_elem *dio;
	struct sfs_dio_data *dio_data;
	struct list_head *pos, *p;	
	unsigned long *pfn_arr;
	int nr_pages;
	int i = 0;
	struct sfs_inode *sfs_inode = SFS_INODE(inode);

	BUG_ON(!sfs_inode->dio);
	dio_data = sfs_inode->dio;

	mutex_lock(&dio_data->lock);

	//printk("(1) %s: inode->i_ino=%lu get the lock, pid=%d\n", __func__, inode->i_ino, current->pid);

	list_for_each_safe(pos, p, &dio_data->data) { 
		dio = list_entry(pos, struct sfs_dio_elem, dio_list);
		nr_pages = dio->nr_pages;
		pfn_arr = dio->pfn_arr;
		for (i = 0; i < nr_pages; i++) {
			wait_on_page_locked(pfn_to_page(pfn_arr[i]));
		}
		kfree(pfn_arr);
		dio->free_cb(dio->raw_block);
		list_del(pos);
		kfree(dio);
	}

	//printk("(2) %s: inode->i_ino=%lu after loop, pid=%d\n", __func__, inode->i_ino, current->pid);

	blkdev_issue_flush(inode->i_sb->s_bdev, GFP_KERNEL, NULL);

	//printk("(3) %s: inode->i_ino=%lu release the lock, pid=%d\n", __func__, inode->i_ino, current->pid);

	mutex_unlock(&dio_data->lock);
	return 0;
}

int sfs_dio_fsync(struct file *file, loff_t start, loff_t end)
{
	int ret;
	struct dentry *dentry = file->f_path.dentry;
	struct inode *inode = dentry->d_inode;

	ret = sfs_wait_dio_data(file, inode, start, end);
	return ret;
}
