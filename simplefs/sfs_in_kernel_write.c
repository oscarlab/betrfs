#include <linux/export.h>
#include <linux/compiler.h>
#include <linux/fs.h>
#include <linux/uaccess.h>
#include <linux/aio.h>
#include <linux/capability.h>
#include <linux/kernel_stat.h>
#include <linux/gfp.h>
#include <linux/mm.h>
#include <linux/swap.h>
#include <linux/mman.h>
#include <linux/pagemap.h>
#include <linux/file.h>
#include <linux/uio.h>
#include <linux/hash.h>
#include <linux/writeback.h>
#include <linux/backing-dev.h>
#include <linux/pagevec.h>
#include <linux/blkdev.h>
#include <linux/security.h>
#include <linux/cpuset.h>
#include <linux/hardirq.h>
#include <linux/memcontrol.h>
#include <linux/cleancache.h>
#include <linux/cpu.h>
#include <linux/percpu.h>
#include <linux/buffer_head.h>
#include <linux/task_io_accounting_ops.h>
#include <linux/bit_spinlock.h>
#include <trace/events/block.h>

static int __sfs_block_commit_write(struct inode *inode, struct page *page,
		unsigned from, unsigned to)
{
	unsigned block_start, block_end;
	int partial = 0;
	unsigned blocksize;
	struct buffer_head *bh, *head;

	bh = head = page_buffers(page);
	blocksize = bh->b_size;
	block_start = 0;

	do {
		block_end = block_start + blocksize;
		if (block_end <= from || block_start >= to) {
			if (!buffer_uptodate(bh))
				partial = 1;
		} else {
			set_buffer_uptodate(bh);
			mark_buffer_dirty(bh);
		}
		clear_buffer_new(bh);

		block_start = block_end;
		bh = bh->b_this_page;
	} while (bh != head);

	/*
	 * If this is a partial write which happened to make all buffers
	 * uptodate then we can optimize away a bogus readpage() for
	 * the next read(). Here we 'discover' whether the page went
	 * uptodate as a result of this (potentially partial) write.
	 */
	if (!partial)
		SetPageUptodate(page);
	return 0;
}

int sfs_generic_write_end(struct file *file, struct address_space *mapping,
			loff_t pos, unsigned len, unsigned copied,
			struct page *page, void *fsdata)
{
	struct inode *inode = mapping->host;
	unsigned start;

	start = pos & (PAGE_CACHE_SIZE - 1);

	if (unlikely(copied < len)) {
		/*
		 * The buffers that were written will now be uptodate, so we
		 * don't have to worry about a readpage reading them and
		 * overwriting a partial write. However if we have encountered
		 * a short write and only partially written into a buffer, it
		 * will not be marked uptodate, so a readpage might come in and
		 * destroy our partial write.
		 *
		 * Do the simplest thing, and just treat any short write to a
		 * non uptodate page as a zero-length write, and force the
		 * caller to redo the whole thing.
		 */
		if (!PageUptodate(page))
			copied = 0;

		page_zero_new_buffers(page, start+copied, start+len);
	}
	flush_dcache_page(page);

	__sfs_block_commit_write(inode, page, start, start+copied);
	unlock_page(page);
	page_cache_release(page);

	return copied;
}

static struct buffer_head *create_page_buffers(struct page *page,
			struct inode *inode, unsigned int b_state)
{
	BUG_ON(!PageLocked(page));

	if (!page_has_buffers(page))
		create_empty_buffers(page, 1 << inode->i_blkbits, b_state);
	return page_buffers(page);
}

/*
 * YZJ: Propagate Uptodate flag from page to buffer head.
 * For write, Uptodate flag means the data is ready to be written out
 */
static int __sfs_block_write_begin(struct page *page, loff_t pos, unsigned len,
		get_block_t *get_block)
{
	unsigned from = pos & (PAGE_CACHE_SIZE - 1);
	unsigned to = from + len;
	struct inode *inode = page->mapping->host;
	unsigned block_start, block_end;
	sector_t block;
	int err = 0;
	unsigned blocksize, bbits;
	struct buffer_head *bh, *head, *wait[2], **wait_bh=wait;

	BUG_ON(!PageLocked(page));
	BUG_ON(from > PAGE_CACHE_SIZE);
	BUG_ON(to > PAGE_CACHE_SIZE);
	BUG_ON(from > to);

	head = create_page_buffers(page, inode, 0);
	blocksize = head->b_size;
	bbits = ilog2(blocksize);

	block = (sector_t)page->index << (PAGE_CACHE_SHIFT - bbits);

	for(bh = head, block_start = 0; bh != head || !block_start;
	    block++, block_start=block_end, bh = bh->b_this_page) {
		block_end = block_start + blocksize;
		/* YZJ: for bh that does not overlap with the write */
		if (block_end <= from || block_start >= to) {
			if (PageUptodate(page)) {
				if (!buffer_uptodate(bh))
					set_buffer_uptodate(bh);
			}
			continue;
		}
		if (buffer_new(bh))
			clear_buffer_new(bh);
		if (!buffer_mapped(bh)) {
			WARN_ON(bh->b_size != blocksize);
			err = get_block(inode, block, bh, 1);
			if (err)
				break;
			if (buffer_new(bh)) {
				if (PageUptodate(page)) {
					clear_buffer_new(bh);
					set_buffer_uptodate(bh);
					mark_buffer_dirty(bh);
					continue;
				}
				/* YZJ: We pre-allocated the blocks, so read it in before write */
				if (block_end > to || block_start < from) {
					ll_rw_block(READ, 1, &bh);
					*wait_bh++=bh;
				}	
				continue;
			}
		}
		if (PageUptodate(page)) {
			if (!buffer_uptodate(bh))
				set_buffer_uptodate(bh);
			continue; 
		}

		/* YZJ: This bh should be uptodate now */
		BUG_ON(!buffer_uptodate(bh));
	}
	/*
	 * If we issued read requests - let them complete.
	 */
	while(wait_bh > wait) {
		wait_on_buffer(*--wait_bh);
		if (!buffer_uptodate(*wait_bh))
			err = -EIO;
	}
	if (unlikely(err))
		page_zero_new_buffers(page, from, to);
	return err;
}

/*
 * sfs_block_write_begin takes care of the basic task of block allocation and
 * bringing partial write blocks uptodate first.
 *
 * The filesystem needs to handle block truncation upon failure.
 */
int sfs_block_write_begin(struct address_space *mapping, loff_t pos, unsigned len,
		unsigned flags, struct page **pagep, get_block_t *get_block)
{
	pgoff_t index = pos >> PAGE_CACHE_SHIFT;
	struct page *page;
	int status;

	page = grab_cache_page_write_begin(mapping, index, flags);
	if (!page)
		return -ENOMEM;

	status = __sfs_block_write_begin(page, pos, len, get_block);
	if (unlikely(status)) {
		unlock_page(page);
		page_cache_release(page);
		page = NULL;
	}

	*pagep = page;
	return status;
}

static ssize_t sfs_perform_write(struct file *file,
				struct iov_iter *i, loff_t pos)
{
	struct address_space *mapping = file->f_mapping;
	const struct address_space_operations *a_ops = mapping->a_ops;
	long status = 0;
	ssize_t written = 0;
	unsigned int flags = 0;

	/*
	 * Copies from kernel address space cannot fail (NFSD is a big user).
	 */
	if (segment_eq(get_fs(), KERNEL_DS))
		flags |= AOP_FLAG_UNINTERRUPTIBLE;

	do {
		struct page *page;
		unsigned long offset;	/* Offset into pagecache page */
		unsigned long bytes;	/* Bytes to write to page */
		size_t copied;		/* Bytes copied from user */
		void *fsdata;

		offset = (pos & (PAGE_CACHE_SIZE - 1));
		bytes = min_t(unsigned long, PAGE_CACHE_SIZE - offset,
						iov_iter_count(i));
again:
		status = a_ops->write_begin(file, mapping, pos, bytes, flags,
						&page, &fsdata);
		if (unlikely(status))
			break;

		/* YZJ: Should not encounter mmap-ed file here */
		BUG_ON(mapping_writably_mapped(mapping));

		pagefault_disable();
		copied = iov_iter_copy_from_user_atomic(page, i, offset, bytes);
		pagefault_enable();
		flush_dcache_page(page);

		mark_page_accessed(page);
		status = a_ops->write_end(file, mapping, pos, bytes, copied,
						page, fsdata);
		if (unlikely(status < 0))
			break;
		copied = status;

		cond_resched();

		iov_iter_advance(i, copied);
		if (unlikely(copied == 0)) {
			/*
			 * If we were unable to copy any data at all, we must
			 * fall back to a single segment length write.
			 *
			 * If we didn't fallback here, we could livelock
			 * because not all segments in the iov can be copied at
			 * once without a pagefault.
			 */
			bytes = min_t(unsigned long, PAGE_CACHE_SIZE - offset,
						iov_iter_single_seg_count(i));
			goto again;
		}
		pos += copied;
		written += copied;
		/*
		 * SFS's dirty throttling is active only for log flush.
		 * The page data is written with special API and they are not
		 * part of dirty throttling bookkeeping. As a result, SFS can
		 * get a unfair share of dirty page limit and cause very long delay.
		 *
		 * The drawback of disabling dirty throttling here is that we
		 * lose background writeback for the log file. I am not
		 * sure how important it is. YZJ -- 10/05/2019
		 */
#if 0
		balance_dirty_pages_ratelimited(mapping);
#endif
		if (fatal_signal_pending(current)) {
			status = -EINTR;
			break;
		}
	} while (iov_iter_count(i));
	return written ? written : status;
}

ssize_t __sfs_in_kernel_write(struct kiocb *iocb, const struct iovec *iov,
				 unsigned long nr_segs, loff_t *ppos)
{
	struct file *file = iocb->ki_filp;
	struct address_space * mapping = file->f_mapping;
	struct iov_iter i;
	size_t count;
	struct inode 	*inode = mapping->host;
	loff_t		pos;
	ssize_t		written;
	ssize_t		err;
	ssize_t		ret;

	err = generic_segment_checks(iov, &nr_segs, &count, VERIFY_READ);
	if (err)
		return err;

	pos = *ppos;

	/* We can write back this queue in page reclaim */
	current->backing_dev_info = mapping->backing_dev_info;
	written = 0;

	err = generic_write_checks(file, &pos, &count, S_ISBLK(inode->i_mode));
	if (err)
		goto out;

	if (count == 0)
		goto out;
	
	iov_iter_init(&i, iov, nr_segs, count, written);
	ret = sfs_perform_write(file, &i, pos);

	if (likely(ret >= 0)) {
		written += ret;
		*ppos = pos + ret;
  	}
	err = written ? written : ret;
out:
	current->backing_dev_info = NULL;
	return err;
}

ssize_t sfs_in_kernel_write(struct kiocb *iocb, const struct iovec *iov,
		unsigned long nr_segs, loff_t pos)
{
	struct file *file = iocb->ki_filp;
	struct inode 	*inode = file->f_mapping->host;
	ssize_t		ret;

	BUG_ON(iocb->ki_pos != pos);

	mutex_lock(&inode->i_mutex);
	ret = __sfs_in_kernel_write(iocb, iov, nr_segs, &iocb->ki_pos);
	mutex_unlock(&inode->i_mutex);

	if (ret > 0 || ret == -EIOCBQUEUED) {
		ssize_t err;

		/* YZJ: perform syncing after a write if file / inode is sync */
		err = generic_write_sync(file, pos, ret);
		if (err < 0 && ret > 0)
			ret = err;
	}
	return ret;
}
