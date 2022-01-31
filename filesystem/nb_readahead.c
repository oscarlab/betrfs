/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file handles the read path for page sharing.
 * It implement file_operations->aio_read (ftfs_file_aio_read)
 * and file_operations->mmap (ftfs_file_mmap).
 */
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
#include <linux/memcontrol.h>
#include <linux/cleancache.h>
#include <linux/pagemap.h>
#include <linux/bitmap.h>
#include <linux/kallsyms.h>

#include "ftfs_northbound.h"
#include "ftfs_indirect.h"

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
typedef unsigned long (*max_sane_readahead_t)(unsigned long);
typedef unsigned long (*get_init_ra_size_t)(unsigned long, unsigned long);
typedef int (*file_read_actor_t)(read_descriptor_t *, struct page *, unsigned long, unsigned long);

DECLARE_SYMBOL_FTFS(max_sane_readahead);
DECLARE_SYMBOL_FTFS(get_init_ra_size);
DECLARE_SYMBOL_FTFS(file_read_actor);

int resolve_nb_readahead_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(max_sane_readahead);
	LOOKUP_SYMBOL_FTFS(get_init_ra_size);
	LOOKUP_SYMBOL_FTFS(file_read_actor);
	return 0;
} 
#else /* LINUX_VERSION_CODE */
/*
 * Set the initial window size, round to next power of 2 and square
 * for small size, x 4 for medium, and x 2 for large
 * for 128k (32 page) max ra
 * 1-8 page = 32k initial, > 8 page = 128k initial
 *
 * YZJ: this function is not in /proc/kallsyms. It is copied
 * from readahead.c. This function is to calucate initial size
 * of readahead window.
 */
static unsigned long get_init_ra_size(unsigned long size, unsigned long max)
{
	unsigned long newsize = roundup_pow_of_two(size);

	if (newsize <= max / 32)
		newsize = newsize * 4;
	else if (newsize <= max / 4)
		newsize = newsize * 2;
	else
		newsize = max;

	return newsize;
}
#endif

/* Copied from readahead.c. It is not in /proc/kallsyms */
static unsigned long get_next_ra_size(struct file_ra_state *ra,
					unsigned long max)
{
	unsigned long cur = ra->size;
	unsigned long newsize;

	if (cur < max / 16)
		newsize = 4 * cur;
	else
		newsize = 2 * cur;

	return min(newsize, max);
}

/*  Copied from readahead.c. It is not in /proc/kallsyms */
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
static pgoff_t count_history_pages(struct address_space *mapping,
				   struct file_ra_state *ra,
				   pgoff_t offset, unsigned long max)
#else /* LINUX_VERSION_CODE */
static pgoff_t count_history_pages(struct address_space *mapping,
				   pgoff_t offset, unsigned long max)
#endif /* LINUX_VERSION_CODE */
{
	pgoff_t head;

	rcu_read_lock();
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	head = radix_tree_prev_hole(&mapping->page_tree, offset - 1, max);
#else /* LINUX_VERSION_CODE */
	head = page_cache_prev_hole(mapping, offset - 1, max);
#endif /* LINUX_VERSION_CODE */
	rcu_read_unlock();

	return offset - 1 - head;
}

/*  Copied from readahead.c. It is not in /proc/kallsyms */
static int try_context_readahead(struct address_space *mapping,
				 struct file_ra_state *ra,
				 pgoff_t offset,
				 unsigned long req_size,
				 unsigned long max)
{
	pgoff_t size;

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	size = count_history_pages(mapping, ra, offset, max);
#else /* LINUX_VERSION_CODE */
	size = count_history_pages(mapping, offset, max);
#endif /* LINUX_VERSION_CODE */

	/*
	 * no history pages:
	 * it could be a random read
	 */
	if (!size)
		return 0;

	/*
	 * starts from beginning of file:
	 * it is a strong indication of long-run stream (or whole-file-read)
	 */
	if (size >= offset)
		size *= 2;

	ra->start = offset;
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	ra->size = ftfs_get_init_ra_size(size + req_size, max);
#else /* LINUX_VERSION_CODE */
	ra->size = get_init_ra_size(size + req_size, max);
#endif /* LINUX_VERSION_CODE */
	ra->async_size = ra->size;

	return 1;
}

extern int ftfs_bstore_scan_msg_vals(DB *data_db, DBT *meta_dbt, DB_TXN *txn,
			pgoff_t *page_offsets, unsigned long *pfn_arr,
			unsigned long nr_to_read, struct address_space *mapping,
			unsigned long last_offset);
/*
 * called by __ftfs_do_page_cache_readahead to issue a range query to the ft code.
 * page_offsets is an array of missing positions of the pages requested by the
 * readahead code. pfn_arr is the output which has the PFNs of the missing pages
 * from the ft layer. nr_to_read is the number of pages needed.
 *
 * It mainly calls ftfs_bstore_scan_msg_vals to generate a range query.
 */
static int ftfs_read_pages(struct address_space *mapping, unsigned long *page_offsets,
			   unsigned long *pfn_arr, unsigned long nr_to_read,
			   unsigned long last_offset)
{
	struct ftfs_sb_info *sbi = mapping->host->i_sb->s_fs_info;
	struct ftfs_inode *ftfs_inode = FTFS_I(mapping->host);
	DBT *meta_dbt;
	DB_TXN *txn;
	int ret = 0;

	meta_dbt = nb_get_read_lock(ftfs_inode);
	TXN_GOTO_LABEL(retry);
	nb_bstore_txn_begin(sbi->db_env, NULL, &txn, TXN_READONLY);

	ret = ftfs_bstore_scan_msg_vals(sbi->data_db, meta_dbt, txn,
					page_offsets, pfn_arr, nr_to_read,
					mapping, ftfs_inode->last_offset);
	if (ret) {
		DBOP_JUMP_ON_CONFLICT(ret, retry);
		nb_bstore_txn_abort(txn);
	} else {
		ret = nb_bstore_txn_commit(txn, DB_TXN_NOSYNC);
		COMMIT_JUMP_ON_CONFLICT(ret, retry);
	}

	ftfs_inode->last_nr_pages = nr_to_read;
	ftfs_inode->last_offset = last_offset;

	nb_put_read_lock(ftfs_inode);
	return ret;
}

/**
 * copied from _do_page_cache_readahead with changes below:
 * 1) no page allocation for readahead
 * 2) issue read to ft code when there is cache miss instead of
 * issuing one read for all the missed pages.
 *
 * FIXME: we may issue too many reads to ft code here.
 */
static int __ftfs_do_page_cache_readahead(
		struct address_space *mapping,
		struct file *filp,
		pgoff_t offset,
		unsigned long nr_to_read,
		unsigned long lookahead_size)
{
	struct inode *inode = mapping->host;
	struct page *page;
	/* The last page we want to read */
	unsigned long end_index;
	loff_t isize = i_size_read(inode);
	unsigned long *read_page_offsets = NULL;
	unsigned long *pfn_arr = NULL;
	/*
	 * The offset of the last page in a
	 * range query used to detect seq read
	 */
	unsigned long last_offset;
	int page_idx;
	int nr_missed = 0;
	int ret = 0;

	if (isize == 0)
		goto out;
	end_index = ((isize - 1) >> PAGE_SHIFT);
	/* FIXME: can we get rid of memory allocation during the query path */
	read_page_offsets = kmalloc(2 * nr_to_read * sizeof(unsigned long), GFP_KERNEL);
	if (read_page_offsets == NULL)
		return -EINVAL;

	pfn_arr = read_page_offsets + nr_to_read;
	for (page_idx = 0; page_idx < nr_to_read; page_idx++) {
		read_page_offsets[page_idx] = FTFS_READAHEAD_INDEX_SKIP;
		pfn_arr[page_idx] = FTFS_READAHEAD_INDEX_SKIP;
	}

	for (page_idx = 0; page_idx < nr_to_read; page_idx++) {
		pgoff_t page_offset = offset + page_idx;
		if (page_offset > end_index)
			break;
		rcu_read_lock();
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
		page = radix_tree_lookup(&mapping->page_tree, page_offset);
#else /* LINUX_VERSION_CODE */
		page = radix_tree_lookup(&mapping->i_pages, page_offset);
#endif /* LINUX_VERSION_CODE */
		rcu_read_unlock();
		if (page && !radix_tree_exceptional_entry(page)) {
			continue;
		}
		/* Yes, this page is requested */
		read_page_offsets[page_idx] = page_offset;
		last_offset = page_offset;
		nr_missed++;
	}
	if (nr_missed) {
		/* Go to ft-index to do the read */
		//printk("%s: nr_to_read=%ld\n", __func__, nr_to_read);
		ret = ftfs_read_pages(mapping, read_page_offsets, pfn_arr, nr_to_read, last_offset);
	}

	for (page_idx = 0; nr_missed > 0 && page_idx < nr_to_read; page_idx++) {
		/* We are encounter a hole of the file at this offset.
		 * We need to insert a page with 0 in this case because
		 * someone may issue writes via mmap().
		 */
		//printk("%s: page_idx=%d, offset=%lu, pfn=%lu\n", __func__, page_idx, read_page_offsets[page_idx], pfn_arr[page_idx]);
		if (read_page_offsets[page_idx] != FTFS_READAHEAD_INDEX_SKIP &&
			pfn_arr[page_idx] == FTFS_READAHEAD_INDEX_SKIP) {
			gfp_t gfp_mask = mapping_gfp_mask(mapping);
			struct page *zero_page = __ftfs_page_cache_alloc(gfp_mask);
			char *buf = kmap_atomic(zero_page);
			memset(buf, 0, PAGE_SIZE);
			kunmap_atomic(buf);
			add_to_page_cache(zero_page, mapping, read_page_offsets[page_idx], GFP_KERNEL);
			unlock_page(zero_page);
			pfn_arr[page_idx] = page_to_pfn(zero_page);
			SetPageUptodate(zero_page);
		}
		/* Skip page already in not requested by readahead */
		if (read_page_offsets[page_idx] == FTFS_READAHEAD_INDEX_SKIP) {
			continue;
		}
		if (page_idx == nr_to_read - lookahead_size) {
			//printk("%s: line=%d\n", __func__, __LINE__);
			//printk("%s: index=%lu, pfn=%lu\n", __func__, page_idx, pfn_arr[page_idx]);
			page = pfn_to_page(pfn_arr[page_idx]);
			SetPageReadahead(page);
		}
	}
	kfree(read_page_offsets);
out:
	return ret;
}

/**
 * The 6 functions below are copied from readahead.c with
 * minor changes --- adding ftfs_ to the function names
 */

/*
 * Submit IO for the read-ahead request in file_ra_state.
 */
static unsigned long ftfs_ra_submit(struct file_ra_state *ra,
			struct address_space *mapping, struct file *filp)
{
	int actual;
	actual = __ftfs_do_page_cache_readahead(mapping, filp,
					ra->start, ra->size, ra->async_size);
	return actual;
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
/*
 * A minimal readahead algorithm for trivial sequential/random reads.
 *
 * YZJ: copied from readahead.c from ondemand_readahead() 
 */
static unsigned long
ftfs_ondemand_readahead(struct address_space *mapping,
		   struct file_ra_state *ra, struct file *filp,
		   bool hit_readahead_marker, pgoff_t offset,
		   unsigned long req_size)
{
	unsigned long max = ftfs_max_sane_readahead(ra->ra_pages);

	/*
	 * start of file
	 */
	if (!offset)
		goto initial_readahead;

	/*
	 * It's the expected callback offset, assume sequential access.
	 * Ramp up sizes, and push forward the readahead window.
	 */
	if ((offset == (ra->start + ra->size - ra->async_size) ||
	     offset == (ra->start + ra->size))) {
		ra->start += ra->size;
		ra->size = get_next_ra_size(ra, max);
		ra->async_size = ra->size;
		goto readit;
	}

	/*
	 * Hit a marked page without valid readahead state.
	 * E.g. interleaved reads.
	 * Query the pagecache for async_size, which normally equals to
	 * readahead size. Ramp it up and use it as the new readahead size.
	 */
	if (hit_readahead_marker) {
		pgoff_t start;

		rcu_read_lock();
		start = radix_tree_next_hole(&mapping->page_tree, offset+1,max);
		rcu_read_unlock();

		if (!start || start - offset > max)
			return 0;

		ra->start = start;
		ra->size = start - offset;	/* old async_size */
		ra->size += req_size;
		ra->size = get_next_ra_size(ra, max);
		ra->async_size = ra->size;
		goto readit;
	}

	/*
	 * oversize read
	 */
	if (req_size > max)
		goto initial_readahead;

	/*
	 * sequential cache miss
	 */
	if (offset - (ra->prev_pos >> PAGE_SHIFT) <= 1UL)
		goto initial_readahead;

	/*
	 * Query the page cache and look for the traces(cached history pages)
	 * that a sequential stream would leave behind.
	 */
	if (try_context_readahead(mapping, ra, offset, req_size, max))
		goto readit;

	/*
	 * standalone, small random read
	 * Read as is, and do not pollute the readahead state.
	 */
	return __ftfs_do_page_cache_readahead(mapping, filp, offset, req_size, 0);

initial_readahead:
	ra->start = offset;
	ra->size = ftfs_get_init_ra_size(req_size, max);
	ra->async_size = ra->size > req_size ? ra->size - req_size : ra->size;

readit:
	/*
	 * Will this read hit the readahead marker made by itself?
	 * If so, trigger the readahead marker hit now, and merge
	 * the resulted next readahead window into the current one.
	 */
	if (offset == ra->start && ra->size == ra->async_size) {
		ra->async_size = get_next_ra_size(ra, max);
		ra->size += ra->async_size;
	}

	return ftfs_ra_submit(ra, mapping, filp);
}
#else
/*
 * A minimal readahead algorithm for trivial sequential/random reads.
 * Mostly copied from ondemand_readahead. Only replace the ftfs_* functions
 *
 * YZJ: copied from readahead.c from ondemand_readahead() 
 */
static unsigned long
ftfs_ondemand_readahead(struct address_space *mapping,
		   struct file_ra_state *ra, struct file *filp,
		   bool hit_readahead_marker, pgoff_t offset,
		   unsigned long req_size)
{
	struct backing_dev_info *bdi = inode_to_bdi(mapping->host);
	unsigned long max_pages = ra->ra_pages;
	unsigned long add_pages;
	pgoff_t prev_offset;

	/*
	 * If the request exceeds the readahead window, allow the read to
	 * be up to the optimal hardware IO size
	 */
	if (req_size > max_pages && bdi->io_pages > max_pages)
		max_pages = min(req_size, bdi->io_pages);

	/*
	 * start of file
	 */
	if (!offset)
		goto initial_readahead;

	/*
	 * It's the expected callback offset, assume sequential access.
	 * Ramp up sizes, and push forward the readahead window.
	 */
	if ((offset == (ra->start + ra->size - ra->async_size) ||
	     offset == (ra->start + ra->size))) {
		ra->start += ra->size;
		ra->size = get_next_ra_size(ra, max_pages);
		ra->async_size = ra->size;
		goto readit;
	}

	/*
	 * Hit a marked page without valid readahead state.
	 * E.g. interleaved reads.
	 * Query the pagecache for async_size, which normally equals to
	 * readahead size. Ramp it up and use it as the new readahead size.
	 */
	if (hit_readahead_marker) {
		pgoff_t start;

		rcu_read_lock();
		start = page_cache_next_hole(mapping, offset + 1, max_pages);
		rcu_read_unlock();

		if (!start || start - offset > max_pages)
			return 0;

		ra->start = start;
		ra->size = start - offset;	/* old async_size */
		ra->size += req_size;
		ra->size = get_next_ra_size(ra, max_pages);
		ra->async_size = ra->size;
		goto readit;
	}

	/*
	 * oversize read
	 */
	if (req_size > max_pages)
		goto initial_readahead;

	/*
	 * sequential cache miss
	 * trivial case: (offset - prev_offset) == 1
	 * unaligned reads: (offset - prev_offset) == 0
	 */
	prev_offset = (unsigned long long)ra->prev_pos >> PAGE_SHIFT;
	if (offset - prev_offset <= 1UL)
		goto initial_readahead;

	/*
	 * Query the page cache and look for the traces(cached history pages)
	 * that a sequential stream would leave behind.
	 */
	if (try_context_readahead(mapping, ra, offset, req_size, max_pages))
		goto readit;

	/*
	 * standalone, small random read
	 * Read as is, and do not pollute the readahead state.
	 */
	return __ftfs_do_page_cache_readahead(mapping, filp, offset, req_size, 0);

initial_readahead:
	ra->start = offset;
	ra->size = get_init_ra_size(req_size, max_pages);
	ra->async_size = ra->size > req_size ? ra->size - req_size : ra->size;

readit:
	/*
	 * Will this read hit the readahead marker made by itself?
	 * If so, trigger the readahead marker hit now, and merge
	 * the resulted next readahead window into the current one.
	 * Take care of maximum IO pages as above.
	 */
	if (offset == ra->start && ra->size == ra->async_size) {
		add_pages = get_next_ra_size(ra, max_pages);
		if (ra->size + add_pages <= max_pages) {
			ra->async_size = add_pages;
			ra->size += add_pages;
		} else {
			ra->size = max_pages;
			ra->async_size = max_pages >> 1;
		}
	}

	return ftfs_ra_submit(ra, mapping, filp);
}
#endif

static void ftfs_page_cache_sync_readahead(struct address_space *mapping,
			       struct file_ra_state *ra, struct file *filp,
			       pgoff_t offset, unsigned long req_size)
{
	BUG_ON(filp && (filp->f_mode & FMODE_RANDOM));
	ftfs_ondemand_readahead(mapping, ra, filp, false, offset, req_size);
}

/*
 * YZJ: async readahead is important for seq read, I believe
 */
static void ftfs_page_cache_async_readahead(struct address_space *mapping,
			struct file_ra_state *ra, struct file *filp,
			struct page *page, pgoff_t offset,
			unsigned long req_size)
{
	/* no read-ahead */
	if (!ra->ra_pages)
		return;

	/*
	 * Same bit is used for PG_readahead and PG_reclaim.
	 */
	if (PageWriteback(page))
		return;

	ClearPageReadahead(page);

	/*
	 * Defer asynchronous read-ahead on IO congestion.
	 */
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	if (bdi_read_congested(mapping->backing_dev_info))
#else /* LINUX_VERSION_CODE */
	if (inode_read_congested(mapping->host))
#endif /* LINUX_VERSION_CODE */
		return;

	/* do read-ahead */
	ftfs_ondemand_readahead(mapping, ra, filp, true, offset, req_size);
}
/* end of copy */


/**
 * Largely copied from do_generic_file_read in filemap.c. Some changes:
 * 1) when the page is not up to date, we call ftfs_lock_page for waiting
 * 2) when sync_readahead return NULL page, we just BUG
 */
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
static void ftfs_generic_file_read(struct file *filp, loff_t *ppos,
		read_descriptor_t *desc, read_actor_t actor)
{
	struct address_space *mapping = filp->f_mapping;
	struct inode *inode = mapping->host;
	struct file_ra_state *ra = &filp->f_ra;
	pgoff_t index;
	pgoff_t last_index;
	pgoff_t prev_index;
	unsigned long offset;      /* offset into pagecache page */
	unsigned int prev_offset;

	index = *ppos >> PAGE_SHIFT;
	prev_index = ra->prev_pos >> PAGE_SHIFT;
	prev_offset = ra->prev_pos & (PAGE_SIZE-1);
	last_index = (*ppos + desc->count + PAGE_SIZE-1) >> PAGE_SHIFT;
	offset = *ppos & ~PAGE_MASK;

	for (;;) {
		struct page *page;
		pgoff_t end_index;
		loff_t isize;
		unsigned long nr, ret;

		cond_resched();
		page = find_get_page(mapping, index);
		if (!page) {
			ftfs_page_cache_sync_readahead(mapping,
					ra, filp,
					index, last_index - index);
			page = find_get_page(mapping, index);
			if (unlikely(page == NULL)) {
				if (i_size_read(inode) == 0)
					goto out;
				isize = i_size_read(inode);
				end_index = (isize - 1) >> PAGE_SHIFT;
				if (index > end_index)
					goto out;
				printk("%s: index=%lu\n", __func__, index);
				goto no_cached_page;
			}
		}

		if (PageReadahead(page)) {
			ftfs_page_cache_async_readahead(mapping,
					ra, filp, page,
					index, last_index - index);
		}
		/*
		 * Let wait if page is not uptodate or
		 * ftfs_lock_page is not released yet
		 */
		if (!PageUptodate(page) || PageReserved(page)) {
			goto page_not_up_to_date;
		}
page_ok:
		/*
		 * i_size must be checked after we know the page is Uptodate.
		 *
		 * Checking i_size after the check allows us to calculate
		 * the correct value for "nr", which means the zero-filled
		 * part of the page is not copied back to userspace (unless
		 * another truncate extends the file - this is desired though).
		 */
		isize = i_size_read(inode);
		end_index = (isize - 1) >> PAGE_SHIFT;
		if (unlikely(!isize || index > end_index)) {
			put_page(page);
			goto out;
		}

		/* nr is the maximum number of bytes to copy from this page */
		nr = PAGE_SIZE;
		if (index == end_index) {
			nr = ((isize - 1) & ~PAGE_MASK) + 1;
			if (nr <= offset) {
				put_page(page);
				goto out;
			}
		}
		nr = nr - offset;

		/* If users can be writing to this page using arbitrary
		 * virtual addresses, take care about potential aliasing
		 * before reading the page on the kernel side.
		 */
		if (mapping_writably_mapped(mapping))
			flush_dcache_page(page);
		/*
		 * When a sequential read accesses a page several times,
		 * only mark it as accessed the first time.
		 */
		if (prev_index != index || offset != prev_offset)
			mark_page_accessed(page);
		prev_index = index;
		/*
		 * Ok, we have the page, and it's up-to-date, so
		 * now we can copy it to user space...
		 *
		 * The actor routine returns how many bytes were actually used..
		 * NOTE! This may not be the same as how much of a user buffer
		 * we filled up (we may be padding etc), so we can only update
		 * "pos" here (the actor routine has to update the user buffer
		 * pointers and the remaining count).
		 */
		ret = actor(desc, page, offset, nr);
		offset += ret;
		index += offset >> PAGE_SHIFT;
		offset &= ~PAGE_MASK;
		prev_offset = offset;

		put_page(page);
		if (ret == nr && desc->count)
			continue;
		goto out;
page_not_up_to_date:
		if (!PagePrivate(page)) {
			SetPageUptodate(page);
			goto page_ok;
		}
		/*
		 * YZJ: if we get the lock, it is supposed to be up
		 * to date, otherwise some error happened 
		 */
		ftfs_lock_page_killable(page);
		if (!page->mapping) {
			ftfs_unlock_page(page);
			put_page(page);
			continue;
		}

		if (PageUptodate(page)) {
			ftfs_unlock_page(page);
			goto page_ok;
		}
		if (page) {
			printk("%s: page is not uptodate\n", __func__);
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}

no_cached_page:
		/*
		 * Ok. it isn't cached, so we need to create a new
		 * page ..
		 */
		printk("%s: The page is not in page cache, we do not expected this\n", __func__);
		{
			struct ftfs_inode *ftfs_inode = FTFS_I(mapping->host);
			DBT *meta_dbt;
			meta_dbt = nb_get_read_lock(ftfs_inode);
			toku_dump_hex("meta_dbt:", meta_dbt->data, meta_dbt->size);
			nb_put_read_lock(ftfs_inode);
			BUG();
		}
	}
out:
	ra->prev_pos = prev_index;
	ra->prev_pos <<= PAGE_SHIFT;
	ra->prev_pos |= prev_offset;

	*ppos = ((loff_t)index << PAGE_SHIFT) + offset;
	file_accessed(filp);
}

/**
 * Copied from readahead.c by cutting the case of O_DIRECT
 * and calling our own ftfs_generic_file_read.
 */
ssize_t
ftfs_file_aio_read(struct kiocb *iocb, const struct iovec *iov,
		unsigned long nr_segs, loff_t pos)
{
	struct file *filp = iocb->ki_filp;
	ssize_t retval;
	unsigned long seg = 0;
	size_t count;
	loff_t *ppos = &iocb->ki_pos;

	count = 0;
	retval = generic_segment_checks(iov, &nr_segs, &count, VERIFY_WRITE);
	if (retval)
		return retval;

	/* BUG for O_DIRECT */
	if (filp->f_flags & O_DIRECT) {
		BUG();
	}

	count = retval;
	for (seg = 0; seg < nr_segs; seg++) {
		read_descriptor_t desc;
		loff_t offset = 0;
		/*
		 * If we did a short DIO read we need to skip the section of the
		 * iov that we've already read data into.
		 */
		if (count) {
			if (count > iov[seg].iov_len) {
				count -= iov[seg].iov_len;
				continue;
			}
			offset = count;
			count = 0;
		}

		desc.written = 0;
		desc.arg.buf = iov[seg].iov_base + offset;
		desc.count = iov[seg].iov_len - offset;
		if (desc.count == 0)
			continue;
		desc.error = 0;
		ftfs_generic_file_read(filp, ppos, &desc, ftfs_file_read_actor);
		retval += desc.written;
		if (desc.error) {
			retval = retval ?: desc.error;
			break;
		}
		if (desc.count > 0)
			break;
	}
	return retval;
}

/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/*
 * Asynchronous readahead happens when we find the page and PG_readahead,
 * so we want to possibly extend the readahead further..
 *
 * Copied from in filemap.c do_async_mmap_readahead without any change.
 */
static void do_async_mmap_readahead(struct vm_area_struct *vma,
				    struct file_ra_state *ra,
				    struct file *file,
				    struct page *page,
				    pgoff_t offset)
{
	struct address_space *mapping = file->f_mapping;

	/* If we don't want any read-ahead, don't bother */
	if (vma->vm_flags & VM_RAND_READ)
		return;
	if (ra->mmap_miss > 0)
		ra->mmap_miss--;
	if (PageReadahead(page))
		ftfs_page_cache_async_readahead(mapping, ra, file,
					   page, offset, ra->ra_pages);
}

/* When this returns, vmf->page is expected to be locked */
int ftfs_filemap_fault(struct vm_area_struct *vma, struct vm_fault *vmf)
{
	struct file *file = vma->vm_file;
	struct address_space *mapping = file->f_mapping;
	struct file_ra_state *ra = &file->f_ra;
	struct inode *inode = mapping->host;
	pgoff_t offset = vmf->pgoff;
	struct page *page;
	pgoff_t size;
	int ret = 0;

	size = (i_size_read(inode) + PAGE_SIZE - 1) >> PAGE_SHIFT;
	if (offset >= size)
		return VM_FAULT_SIGBUS;

	page = find_get_page(mapping, offset);
	/* FAULT_FLAG_TRIED: second try */
	if (likely(page) && !(vmf->flags & FAULT_FLAG_TRIED)) {
		do_async_mmap_readahead(vma, ra, file, page, offset);
	} else if (!page) {
		ftfs_page_cache_sync_readahead(mapping, ra, file,
					offset, ra->ra_pages);
		ret = VM_FAULT_MAJOR;
		page = find_get_page(mapping, offset);
		if (!page) {
			printk("Cannot find page after readahead\n");
			return VM_FAULT_SIGBUS;
			//BUG();
		}
	}

	if (!PageUptodate(page)) {
		sb_wait_read_page(page_to_pfn(page));
		BUG_ON(!page->mapping);
		BUG_ON(!PageUptodate(page));
	}

	lock_page(page);
	if (unlikely(page->mapping != mapping)) {
		printk("Invalid page->mapping\n");
		BUG();
	}

	VM_BUG_ON(page->index != offset);
	if (!PageUptodate(page)) {
		printk("%s: page need to be uptodate\n", __func__);
		BUG();
	}

	if (PagePrivate(page)) {
		struct page *cow_page;
		gfp_t gfp_mask = mapping_gfp_mask(mapping);
		BUG_ON(page_mapped(page));
		if (mapping_cap_account_dirty(mapping))
			gfp_mask |= __GFP_WRITE;
		/* NOTE: cow_page is locked in ftfs_cow_page */
		cow_page = ftfs_cow_page(page, mapping,
				page->index, gfp_mask, 0);
		SetPageUptodate(cow_page);
		vmf->page = cow_page;
		unlock_page(page);
		put_page(page);
	} else {
		vmf->page = page;
	}
	return ret | VM_FAULT_LOCKED;
}
#else
/*
 * Largely copied from generic_file_read in filemap.c. Some changes:
 * 1) when the page is not up to date, we call ftfs_lock_page for waiting
 * 2) when sync_readahead return NULL page, we just BUG
 */
static ssize_t ftfs_generic_file_buffered_read(struct kiocb *iocb,
		struct iov_iter *iter, ssize_t written)
{
	//read_descriptor_t *desc, read_actor_t actor)

	struct file *filp = iocb->ki_filp;
	struct address_space *mapping = filp->f_mapping;
	struct inode *inode = mapping->host;
	struct file_ra_state *ra = &filp->f_ra;
	loff_t *ppos = &iocb->ki_pos;

	pgoff_t index;
	pgoff_t last_index;
	pgoff_t prev_index;
	unsigned long offset;      /* offset into pagecache page */
	unsigned int prev_offset;
	int error = 0;

	if (unlikely(*ppos >= inode->i_sb->s_maxbytes))
		return 0;
	iov_iter_truncate(iter, inode->i_sb->s_maxbytes);

	index = *ppos >> PAGE_SHIFT;
	prev_index = ra->prev_pos >> PAGE_SHIFT;
	prev_offset = ra->prev_pos & (PAGE_SIZE-1);
	last_index = (*ppos + iter->count + PAGE_SIZE-1) >> PAGE_SHIFT;
	offset = *ppos & ~PAGE_MASK;

	for (;;) {
		struct page *page;
		pgoff_t end_index;
		loff_t isize;
		unsigned long nr, ret;

		cond_resched();
		page = find_get_page(mapping, index);
		if (!page) {
			ftfs_page_cache_sync_readahead(mapping,
					ra, filp,
					index, last_index - index);
			page = find_get_page(mapping, index);
			if (unlikely(page == NULL)) {
				if (i_size_read(inode) == 0)
					goto out;
				isize = i_size_read(inode);
				end_index = (isize - 1) >> PAGE_SHIFT;
				if (index > end_index)
					goto out;
				printk("%s: index=%lu\n", __func__, index);
				goto no_cached_page;
			}
		}

		if (PageReadahead(page)) {
			ftfs_page_cache_async_readahead(mapping,
					ra, filp, page,
					index, last_index - index);
		}
		/*
		 * Let wait if page is not uptodate or
		 * ftfs_lock_page is not released yet
		 */
		if (!PageUptodate(page) || PageReserved(page)) {
			goto page_not_up_to_date;
		}
page_ok:
		/*
		 * i_size must be checked after we know the page is Uptodate.
		 *
		 * Checking i_size after the check allows us to calculate
		 * the correct value for "nr", which means the zero-filled
		 * part of the page is not copied back to userspace (unless
		 * another truncate extends the file - this is desired though).
		 */
		isize = i_size_read(inode);
		end_index = (isize - 1) >> PAGE_SHIFT;
		if (unlikely(!isize || index > end_index)) {
			put_page(page);
			goto out;
		}

		/* nr is the maximum number of bytes to copy from this page */
		nr = PAGE_SIZE;
		if (index == end_index) {
			nr = ((isize - 1) & ~PAGE_MASK) + 1;
			if (nr <= offset) {
				put_page(page);
				goto out;
			}
		}
		nr = nr - offset;

		/* If users can be writing to this page using arbitrary
		 * virtual addresses, take care about potential aliasing
		 * before reading the page on the kernel side.
		 */
		if (mapping_writably_mapped(mapping))
			flush_dcache_page(page);
		/*
		 * When a sequential read accesses a page several times,
		 * only mark it as accessed the first time.
		 */
		if (prev_index != index || offset != prev_offset)
			mark_page_accessed(page);
		prev_index = index;
		/*
		 * Ok, we have the page, and it's up-to-date, so
		 * now we can copy it to user space...
		 *
		 * The actor routine returns how many bytes were actually used..
		 * NOTE! This may not be the same as how much of a user buffer
		 * we filled up (we may be padding etc), so we can only update
		 * "pos" here (the actor routine has to update the user buffer
		 * pointers and the remaining count).
		 */
		ret = copy_page_to_iter( page, offset, nr, iter);
		offset += ret;
		index += offset >> PAGE_SHIFT;
		offset &= ~PAGE_MASK;
		prev_offset = offset;

		put_page(page);
		written += ret;
		if (!iov_iter_count(iter))
			goto out;
		if (ret < nr) {
			error = -EFAULT;
			goto out;
		}
		continue;

page_not_up_to_date:
		if (!PagePrivate(page)) {
			SetPageUptodate(page);
			goto page_ok;
		}
		/*
		 * YZJ: if we get the lock, it is supposed to be up
		 * to date, otherwise some error happened 
		 */
		ftfs_lock_page_killable(page);
		if (!page->mapping) {
			ftfs_unlock_page(page);
			put_page(page);
			continue;
		}

		if (PageUptodate(page)) {
			ftfs_unlock_page(page);
			goto page_ok;
		}
		if (page) {
			printk("%s: page is not uptodate\n", __func__);
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}

no_cached_page:
		/*
		 * Ok. it isn't cached, so we need to create a new
		 * page ..
		 */
		printk("%s: The page is not in page cache, we do not expected this\n", __func__);
		{
			struct ftfs_inode *ftfs_inode = FTFS_I(mapping->host);
			DBT *meta_dbt;
			meta_dbt = nb_get_read_lock(ftfs_inode);
			toku_dump_hex("meta_dbt:", meta_dbt->data, meta_dbt->size);
			nb_put_read_lock(ftfs_inode);
			BUG();
		}
	}
out:
	ra->prev_pos = prev_index;
	ra->prev_pos <<= PAGE_SHIFT;
	ra->prev_pos |= prev_offset;

	*ppos = ((loff_t)index << PAGE_SHIFT) + offset;
	file_accessed(filp);
	return written ? written : error;
}

/**
 * Copied from readahead.c (generic_file_read_iter) by cutting
 * the case of O_DIRECT and calling our own ftfs_generic_file_read.
 */
ssize_t
ftfs_file_read_iter(struct kiocb *iocb, struct iov_iter *iter)
{
	size_t count = iov_iter_count(iter);
	ssize_t retval = 0;

	if (!count)
		goto out; /* skip atime */

	retval = ftfs_generic_file_buffered_read(iocb, iter, retval);
out:
	return retval;
}

/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/*
 * Asynchronous readahead happens when we find the page and PG_readahead,
 * so we want to possibly extend the readahead further..
 *
 * Copied from filemap.c  do_async_mmap_readahead() without change
 */
static void do_async_mmap_readahead(struct vm_area_struct *vma,
				    struct file_ra_state *ra,
				    struct file *file,
				    struct page *page,
				    pgoff_t offset)
{
	struct address_space *mapping = file->f_mapping;

	/* If we don't want any read-ahead, don't bother */
	if (vma->vm_flags & VM_RAND_READ)
		return;
	if (ra->mmap_miss > 0)
		ra->mmap_miss--;
	if (PageReadahead(page))
		ftfs_page_cache_async_readahead(mapping, ra, file,
					   page, offset, ra->ra_pages);
}

/* When this returns, vmf->page is expected to be locked
 * Followed filemap_fault in filemap.c.
 * Call ftfs_* variant and handle copy-on-write is the page
 * is shared.
 */
vm_fault_t ftfs_filemap_fault(struct vm_fault *vmf)
{
	struct vm_area_struct *vma = vmf->vma;
	struct file *file = vma->vm_file;
	struct address_space *mapping = file->f_mapping;
	struct file_ra_state *ra = &file->f_ra;
	struct inode *inode = mapping->host;
	pgoff_t offset = vmf->pgoff;
	struct page *page;
	pgoff_t size;
	vm_fault_t ret = 0;

	size = (i_size_read(inode) + PAGE_SIZE - 1) >> PAGE_SHIFT;
	if (offset >= size)
		return VM_FAULT_SIGBUS;

	page = find_get_page(mapping, offset);
	/* FAULT_FLAG_TRIED: second try */
	if (likely(page) && !(vmf->flags & FAULT_FLAG_TRIED)) {
		do_async_mmap_readahead(vma, ra, file, page, offset);
	} else if (!page) {
		ftfs_page_cache_sync_readahead(mapping, ra, file,
					offset, ra->ra_pages);
		ret = VM_FAULT_MAJOR;
		page = find_get_page(mapping, offset);
		if (!page) {
			printk("Cannot find page after readahead\n");
			return VM_FAULT_SIGBUS;
			//BUG();
		}
	}

	if (!PageUptodate(page)) {
		sb_wait_read_page(page_to_pfn(page));
		BUG_ON(!page->mapping);
		//printk("PageUptodate(page)=%d\n", PageUptodate(page));
		//BUG_ON(!PageUptodate(page));
		if (!PageUptodate(page)) {
			SetPageUptodate(page);
		}
	}

	lock_page(page);
	if (unlikely(page->mapping != mapping)) {
		printk("Invalid page->mapping\n");
		BUG();
	}

	VM_BUG_ON(page->index != offset);
	if (!PageUptodate(page)) {
		printk("%s: page need to be uptodate\n", __func__);
		BUG();
	}

	if (PagePrivate(page)) {
		struct page *cow_page;
		gfp_t gfp_mask = mapping_gfp_mask(mapping);
		BUG_ON(page_mapped(page));
		if (mapping_cap_account_dirty(mapping))
			gfp_mask |= __GFP_WRITE;
		/* NOTE: cow_page is locked in ftfs_cow_page */
		cow_page = ftfs_cow_page(page, mapping,
				page->index, gfp_mask, 0);
		SetPageUptodate(cow_page);
		vmf->page = cow_page;
		unlock_page(page);
		put_page(page);
	} else {
		vmf->page = page;
	}
	return ret | VM_FAULT_LOCKED;
}
#endif
