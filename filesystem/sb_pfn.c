/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* The file implements functions to manipulate pages,
 * including allocating and free pages for ft layer and page
 * cache, page ref counting, changing the page->private data.
 * It also implements ftfs_lock_page() and related functions
 * for synchronization.
 */
#include <linux/slab.h>
#include <linux/gfp.h>
#include <linux/mm.h>
#include <linux/highmem.h>
#include <linux/pagemap.h>
#include <linux/sched.h>
#include <linux/wait.h>
#include <linux/mmzone.h>
#include <linux/hash.h>
#include <linux/swap.h>
#include <linux/bitmap.h>
#include <linux/kallsyms.h>

#include "ftfs.h"
#include "ftfs_indirect.h"

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
#include <linux/wait.h>
#include <linux/sched/signal.h>
#endif /* LINUX_VERSION_CODE */

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
typedef int (*sleep_on_page_t)(void *word);
typedef int (*sleep_on_page_killable_t)(void *word);
typedef wait_queue_head_t* (*page_waitqueue_t)(struct page *page);
DECLARE_SYMBOL_FTFS(sleep_on_page);
DECLARE_SYMBOL_FTFS(sleep_on_page_killable);
DECLARE_SYMBOL_FTFS(page_waitqueue);
#else
typedef int (*wait_on_page_bit_common_t)(wait_queue_head_t *q,
					struct page *page,
					int bit_nr, int state,
					bool lock);
typedef void (*wake_up_page_bit_t)(struct page *page, int bit_nr);
typedef wait_queue_head_t* page_wait_table_t;
DECLARE_SYMBOL_FTFS(wait_on_page_bit_common);
DECLARE_SYMBOL_FTFS(wake_up_page_bit);
DECLARE_SYMBOL_FTFS(page_wait_table);
#endif

int resolve_sb_pfn_symbols(void)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	LOOKUP_SYMBOL_FTFS(sleep_on_page);
	LOOKUP_SYMBOL_FTFS(sleep_on_page_killable);
	LOOKUP_SYMBOL_FTFS(page_waitqueue);
#else
	LOOKUP_SYMBOL_FTFS(wait_on_page_bit_common);
	LOOKUP_SYMBOL_FTFS(wake_up_page_bit);
	LOOKUP_SYMBOL_FTFS(page_wait_table);
#endif
	return 0;
}

#ifdef TOKU_MEMLEAK_DETECT
static atomic_t free_page_counter = ATOMIC_INIT(0);
static atomic_t add_lru_page_counter = ATOMIC_INIT(0);

void ftfs_print_free_page_counter(void)
{
	printk("%s: free_page_counter=%d\n", __func__, atomic_read(&free_page_counter));
	printk("%s: add_lru_page_counter=%d\n", __func__, atomic_read(&add_lru_page_counter));
}
#endif

/////////////////////////////////////////////////
/////////////// Utility Functions ///////////////
/////////////////////////////////////////////////
static int sb_get_pid(void)
{
	return current->pid;
}

static char *sb_get_proc_name(void)
{
	return (char*)current->comm;
}

void ftfs_print_page(unsigned long pfn) 
{
	struct page *page = pfn_to_page(pfn);
	struct ftfs_page_private *priv = FTFS_PAGE_PRIV(page);

	printk("=========Page Info=====\n");
	printk("Process is %s\n", sb_get_proc_name());
	printk("PID is %d\n", sb_get_pid());
	printk("pfn=%lx\n", pfn);
	printk("page->_count=%d\n", ftfs_read_page_count(page));
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
	printk("Reserved Flag=%d\n", PageReserved(page));
	printk("Waiter Flag=%d\n", PageWaiters(page));
#endif /* LINUX_VERSION_CODE */
	printk("page->_mapcount=%d\n", atomic_read(&page->_mapcount));
	printk("Private Flag=%d\n", PagePrivate(page));
	printk("Private2 Flag=%d\n", PagePrivate2(page));
	printk("Lock Flag=%d\n", PageLocked(page));
	printk("Writeback Flag=%d\n", PageWriteback(page));
	printk("Uptodate Flag=%d\n", PageUptodate(page));
	printk("LRU Flag=%d\n", PageLRU(page));
	printk("index is %lx\n", page->index);
	printk("mapping=%p\n", page->mapping);
	if (priv == NULL)
		return;
	printk("private hex is %lx\n", priv->private);
	printk("private count is %d\n", atomic_read(&priv->_count));
}

void ftfs_set_page_up_to_date(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	SetPageUptodate(page);
}

/*
 * The page has been locked, we can modified the shared flag and refcount
 * the input page is removed from page cache, and that the returned (copied)
 * page is added to the page cache in its place.
 */
struct page* ftfs_cow_page(struct page* page,
		struct address_space *mapping, pgoff_t index,
		gfp_t gfp_mask, gfp_t gfp_notmask)
{
	char *buf_src;
	char *buf_dest;
	struct page *cow_page;
	int status;

	/*
	 * Again, what state this page can be in?
	 * It is a shared page. It can be undergoing IO by ft code.
	 * We need to wait its completion for data copy if it is read.
	 * If the IO is write, it seems ok. But let wait anyway.
	 */
	BUG_ON(PageWriteback(page));
	if (PageReserved(page))
		wait_on_page_bit(page, PG_reserved);

	/*
	 * allocate a new page. do the copy.
	 * delete page from page cache and clear its flag
	 * The lock is not release yet.
	 */
	cow_page = __ftfs_page_cache_alloc(gfp_mask & ~gfp_notmask);
	BUG_ON(!cow_page);
	preempt_disable();
	buf_src = kmap_atomic(page);
	buf_dest = kmap_atomic(cow_page);
	memcpy(buf_dest, buf_src, PAGE_SIZE);
	kunmap_atomic(buf_src);
	kunmap_atomic(buf_dest);
	preempt_enable();
	ClearPagePrivate(page);
	delete_from_page_cache(page);
	/*
	 * handled the old page, let up add the new page
	 * to page cache. add_to_page_cache is to add newly
	 * allocated pages. Since the page is new, so we can
	 * just run __set_page_locked() against it.
	 */
	status = add_to_page_cache(cow_page, mapping, index,
			GFP_KERNEL & ~gfp_notmask);
	if (unlikely(status)) {
		printk("Add cow page failed\n");
		put_page(cow_page);
		BUG_ON(status == -EEXIST);
		return NULL;
	}
	/* Do some post-checking here:
	 * cow_page should be locked and has refcount == 2
	 */
	BUG_ON(ftfs_read_page_count(cow_page) != 2);
	BUG_ON(!PageLocked(cow_page));
	if (PageUptodate(page))
		SetPageUptodate(cow_page);
	return cow_page;
}

/////////////////////////////////////////////////
/////////////// Page Allocation /////////////////
/////////////////////////////////////////////////
void ftfs_reset_page_private(unsigned long pfn);
unsigned long ftfs_get_page_private(unsigned long pfn);

struct page* __ftfs_page_cache_alloc(gfp_t gfp)
{
	struct ftfs_page_private *priv;
	struct page *page;
	page = __page_cache_alloc(gfp);
	BUG_ON(page == NULL);
	BUG_ON(ftfs_read_page_count(page) != 1);

	priv = kzalloc(sizeof(*priv), GFP_KERNEL);
	BUG_ON(priv == NULL);
	priv->private = 0;
	page->private = (unsigned long)priv;
	debug_ftfs_alloc_page(page, PAGE_MAGIC);
	return page;
}

unsigned long ftfs_alloc_page(void)
{
	struct page *page;
	page = __ftfs_page_cache_alloc(FTFS_READ_PAGE_GFP);
	BUG_ON(page == NULL);
	return page_to_pfn(page);
}

void ftfs_unlock_page(struct page *page);
void ftfs_lock_page(struct page *page);

/* Map pfn to address so that the data can be copied.
 * Caller must NOT sleep or call a potentially blocking
 * function between calling this.
 */
char *sb_map_page_atomic(unsigned long pfn, bool skip_wait)
{
	struct page *page;
	char *buf;

	page = pfn_to_page(pfn);
	if (!skip_wait) {
		sb_wait_read_page(pfn);
	}
	preempt_disable();
	buf = (char*)kmap_atomic(page);
	return buf;
}

/* Ummap the pfn if it has been mapped.
 * Caller must NOT sleep or call a potentially blocking
 * function between calling this.
 */
void sb_unmap_page_atomic(void *addr)
{
	kunmap_atomic(addr);
	preempt_enable();
}

/* Called in ft-ops.cc to break page sharing between a nonleaf
 * and leaf node. Copy the src and the leaf will reference the
 * the copied page. This function disable preemption when
 * copying the page.
 */
void sb_copy_page_atomic(unsigned long src, unsigned long dest,
			 unsigned int size)
{
	struct page *src_page;
	struct page *dest_page;
	char *src_buf;
	char *dest_buf;

	src_page = pfn_to_page(src);
	dest_page = pfn_to_page(dest);
	sb_wait_read_page(src);
	preempt_disable();
	src_buf = (char*)kmap_atomic(src_page);
	dest_buf = (char*)kmap_atomic(dest_page);
	memcpy(dest_buf, src_buf, size);

	kunmap_atomic(src_buf);
	kunmap_atomic(dest_buf);
	preempt_enable();
}

/* Increase internal refcount */
void ftfs_inc_page_private_count(struct page *page)
{
	struct ftfs_page_private *priv = FTFS_PAGE_PRIV(page);
	if (!priv) {
		ftfs_print_page(page_to_pfn(page));
		BUG();
	}
	atomic_inc(&priv->_count);
}

/**
 * decrease internal refcount and called to dereference
 * the page of a cloned ftnode. Due to garbage collection
 * it may be not the last one to dereference the page internally
 */
void ftfs_dec_page_private_count(struct page *page)
{
	struct ftfs_page_private *priv;
	int internal_cnt = 0;

	priv = FTFS_PAGE_PRIV(page);
	if (!priv) {
		ftfs_print_page(page_to_pfn(page));
		BUG();
	}
	internal_cnt = atomic_read(&priv->_count);
	if (internal_cnt > 1) {
		atomic_dec(&priv->_count);
	} else {
		ftfs_print_page(page_to_pfn(page));
		BUG();
	}
}

static void  __ftfs_free_page(struct page *page)
{
	struct ftfs_page_private *priv;
	debug_ftfs_free_page(page);
	priv = FTFS_PAGE_PRIV(page);
	kfree(priv);
	page->private = 0;
	put_page(page);
}

void ftfs_set_page_private(unsigned long pfn, unsigned long bit);

/**
 * We need to modifiy the Private flag, therefore need to
 * lock the page first.
 * page->_count is increased by 1 whenever the page is shared
 * with ft code. page->private->_count is the count for ft code.
 * When it reaches 0, it means this page can be managed by VFS
 * alone. Therefore add this page to LRU list.
 * If ft code owns the page exclusively, then just free the page's
 * private data and add the page to LRU list
 */
void ftfs_internal_free_page(struct page *page, bool is_leaf)
{
	struct ftfs_page_private *priv;
	int internal_count;
	int count;
	bool is_shared;
	bool is_read = false;

	get_page(page);
	/*
	 * Lock ordering:
	 * Lock the page first by calling lock_page();
	 * then call ftfs_lock_page() to wait for IO completion
	 */
	lock_page(page);
	/* Let us wait for the IO on this page.
	 * After this lock, no IO is for this page.
	 */
	ftfs_lock_page(page);
	VM_BUG_ON(!PageReserved(page));

	if (page->private == 0) {
		ftfs_print_page(page_to_pfn(page));
		BUG();
	}
	priv = FTFS_PAGE_PRIV(page);
	internal_count = atomic_read(&priv->_count);	
	/* we just called get_page */
	count = ftfs_read_page_count(page);
	is_shared = PagePrivate(page);

	/* Page cache does not own it any more */
	if (count == 1) {
		/* cannot be shared */
		if (is_shared || page->mapping) {
			printk("Encounter shared page, count=%d\n", count);
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}

		if (internal_count == 1) {
#ifdef TOKU_MEMLEAK_DETECT
			atomic_inc(&free_page_counter);
#endif
			__ftfs_free_page(page);
		} else if (internal_count > 1) {
			ftfs_dec_page_private_count(page);
		} else {
			printk("%s: internal_count=%d\n", __func__, internal_count);
			printk("%s: count=%d\n", __func__, count);
			printk("%s: page->mapping=%p\n", __func__, page->mapping);
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
	} else if (count > 1 && !is_shared) {
		BUG_ON(page->mapping);
#ifdef FTFS_DEBUG
		printk("count=%d, internal_count=%d\n", count, internal_count);
#endif
		if (internal_count == 1) {
			put_page(page);
			atomic_dec(&priv->_count);
#ifdef TOKU_MEMLEAK_DETECT
			atomic_inc(&add_lru_page_counter);
#endif
			debug_ftfs_free_page(page);
			kfree(priv);
			page->private = 0;
			lru_cache_add_file(page);
			goto out;
		} else if (internal_count > 1) {
			ftfs_dec_page_private_count(page);
		} else {
			printk("%s: internal_count=%d\n", __func__, internal_count);
			ftfs_print_page(page_to_pfn(page));
			BUG();
		}
	} else if (count > 1) {
		BUG_ON(!page->mapping);
		is_read = ftfs_is_read_page(priv->private);
		if (internal_count == 1) {
			ftfs_set_page_private(page_to_pfn(page), FT_MSG_VAL_CLR_BY_FT);
			ClearPagePrivate(page);
			put_page(page);
			atomic_dec(&priv->_count);
			if (atomic_read(&priv->_count) != 0) {
				printk("%s: wrong internal count\n", __func__);
				ftfs_print_page(page_to_pfn(page));
				BUG();
			}
#ifdef TOKU_MEMLEAK_DETECT
			atomic_inc(&add_lru_page_counter);
#endif
			debug_ftfs_free_page(page);
			kfree(priv);
			page->private = 0;
			lru_cache_add_file(page);
			goto out;
		} else if (internal_count > 1) {
			ftfs_dec_page_private_count(page);
		} else {
			printk("%s: internal_count=%d\n", __func__, internal_count);
			BUG();
		}
	} else {
		printk("%s count=%d\n", __func__, count);
		BUG();
	}
out:
	ftfs_unlock_page(page);
	unlock_page(page);
	put_page(page);
}

void ftfs_free_page_list(unsigned long *pfn_arr,
			 int nr_pages, bool is_leaf)
{
	struct page *page;
	int i = 0;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		ftfs_internal_free_page(page, is_leaf);
	}
}

//////////////////////////////////////////////////////////
//////////////////  Manipulate page private //////////////
///////// Mainly for write path from VFS to toku /////////
//////////////////////////////////////////////////////////
void ftfs_set_page_private(unsigned long pfn, unsigned long bit)
{
	struct page *page;
	struct ftfs_page_private *priv;

	page = pfn_to_page(pfn);
	if (!pfn_valid(pfn)) {
		printk("Found invalid pfn=%lu\n", pfn);
		BUG();	
	}
	priv = FTFS_PAGE_PRIV(page);
	if (priv == NULL) {
		ftfs_print_page(page_to_pfn(page));
		BUG();
	}
	bitmap_set(&priv->private, bit, 1);
}

void ftfs_reset_page_private(unsigned long pfn)
{
	struct page *page;
	struct ftfs_page_private *priv;

	if (!pfn_valid(pfn)) {
		printk("Found invalid pfn=%lu\n", pfn);
		BUG();	
	}
	page = pfn_to_page(pfn);
	priv = FTFS_PAGE_PRIV(page);
	priv->private = 0;
}

void ftfs_clear_page_private(unsigned long pfn, unsigned long bit)
{
	struct page *page;
	struct ftfs_page_private *priv;

	if (!pfn_valid(pfn)) {
		printk("Found invalid pfn=%lu\n", pfn);
		BUG();	
	}
	page = pfn_to_page(pfn);
	priv = FTFS_PAGE_PRIV(page);
	bitmap_clear(&priv->private, bit, 1);
}

void ftfs_clear_page_list_private(unsigned long* pfn_arr,
				  int nr_pages, int bit)
{
	int i = 0;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		ftfs_clear_page_private(pfn_arr[i], bit);
	}
}

int ftfs_page_refcount(unsigned long head_pfn)
{
	struct page *head_page;

	if (!pfn_valid(head_pfn)) {
		printk("Found invalid pfn=%lu\n", head_pfn);
		BUG();	
	}
	head_page = pfn_to_page(head_pfn);
	return ftfs_read_page_count(head_page);
}

void ftfs_set_page_list_private(unsigned long* pfn_arr,
				int nr_pages, int bit)
{
	int i = 0;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		ftfs_set_page_private(pfn_arr[i], bit);
	}
}

/*
 * When a node is cloned for writing back to disk
 * call ftfs_lock_page() for all the pages first
 * so that garbage collection knows that this page
 * is under IO and wait its completion.
 */
void ftfs_lock_page_list_for_clone(unsigned long* pfn_arr, int nr_pages)
{
	int i = 0;
	struct page *page;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		ftfs_lock_page(page);
	}
}

void ftfs_unlock_page_list_for_clone(unsigned long* pfn_arr, int nr_pages)
{
	int i = 0;
	struct page *page;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		ftfs_unlock_page(page);
	}
}

unsigned long ftfs_get_page_private(unsigned long pfn)
{
	struct page *page;
	struct ftfs_page_private *priv;

	if (!pfn_valid(pfn)) {
		printk("Found invalid pfn=%lu\n", pfn);
		BUG();	
	}
	page = pfn_to_page(pfn);
	priv = FTFS_PAGE_PRIV(page);
	BUG_ON(!priv);

	return priv->private;
}

/* General functio to increase refcount a page arr */
static void ftfs_get_page_list(unsigned long *pfn_arr,
			       int nr_pages)
{
	struct page *page;
	int i = 0;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		ftfs_inc_page_private_count(page);
	}
}

/* General function to decrease refcount of page array */
static void ftfs_put_page_list(unsigned long *pfn_arr,
			       int nr_pages, bool is_leaf)
{
	struct page *page;
	int i = 0;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		ftfs_dec_page_private_count(page);
	}
}

/* for nonleaf node */
void ftfs_fifo_get_page_list(unsigned long* pfn_arr, int nr_pages)
{
	ftfs_get_page_list(pfn_arr, nr_pages);
}

void ftfs_fifo_put_page_list(unsigned long* pfn_arr, int nr_pages)
{
	ftfs_put_page_list(pfn_arr, nr_pages, true);
}

void ftfs_check_page_list(unsigned long *pfn_arr, int nr_pages,
	int expected_count, int expected_internal_count)
{
	struct ftfs_page_private *priv;
	struct page *page;
	int i = 0;

	BUG_ON(pfn_arr == NULL);
	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		BUG_ON(ftfs_read_page_count(page) != expected_count);
		priv = FTFS_PAGE_PRIV(page);
		BUG_ON(atomic_read(&priv->_count) != expected_internal_count);
	}
}

/* for leaf node */
void ftfs_bn_get_page_list(unsigned long *pfn_arr, int nr_pages)
{
	ftfs_get_page_list(pfn_arr, nr_pages);
}
/*
 * Used when we are sure it is not the last one to reference the page.
 * Otherwise use ftfs_free_page_list
 */
void ftfs_bn_put_page_list(unsigned long *pfn_arr, int nr_pages)
{
	ftfs_put_page_list(pfn_arr, nr_pages, true);
}

bool sb_test_page_list_bit(unsigned long *pfn_arr, int nr_pages, int bit)
{
	struct ftfs_page_private *priv;
	struct page *page;
	bool is_bit_set;
	bool res;
	int i = 0;

	page = pfn_to_page(pfn_arr[0]);
	priv = FTFS_PAGE_PRIV(page);
	if (priv == NULL) {
		ftfs_print_page(pfn_arr[0]);
		BUG();
	}
	res = (priv->private & (1 << bit));

	/* If the first page is bound page, the others need be the same */
	for (i = 1; i < nr_pages; i++) {
		page = pfn_to_page(pfn_arr[i]);
		priv = FTFS_PAGE_PRIV(page);
		is_bit_set = (priv->private & (1 << bit));
		BUG_ON(is_bit_set != res);
	}
	return res;
}

bool sb_is_page_list_bound(unsigned long *pfn_arr, int nr_pages)
{
	return sb_test_page_list_bit(pfn_arr, nr_pages, FT_MSG_VAL_BOUND_BIT);
}

/* This is a file-backed page */
bool sb_is_wb_page(unsigned long *pfn_arr, int nr_pages)
{
	struct page *page;
	int i;
	bool is_wb = false;

	for (i = 0; i < nr_pages; i++) {
		page = pfn_to_page(pfn_arr[i]);
		if (i == 0) {
			is_wb = PageWriteback(page);
		} else {
			BUG_ON(is_wb != PageWriteback(page));
		}
	}
	return is_wb;
}

/**
 * Called when a k-v pair from northbound is inserted.
 * The pages have to be writeback and locked.
 * ft data structure --- basement node buffer or fifo
 */
void sb_wb_page_list_unlock(unsigned long *pfn_arr,
			int nr_pages, bool is_leaf)
{
	struct page *page;
	int i = 0;

	BUG_ON(pfn_arr == NULL || nr_pages <= 0);

	for (i = 0; i < nr_pages; i++) {
		BUG_ON(!pfn_valid(pfn_arr[i]));
		page = pfn_to_page(pfn_arr[i]);
		BUG_ON(!PageWriteback(page));
		BUG_ON(!PageLocked(page));
		/* Page has to be shared */
		BUG_ON(!PagePrivate(page));
		end_page_writeback(page);
		ftfs_inc_page_private_count(page);
	}

	for (i = 0; i < nr_pages; i++) {
		page = pfn_to_page(pfn_arr[i]);
		unlock_page(page);
	}
}

void ftfs_wait_reserved_page(unsigned long pfn)
{
	struct page *page = pfn_to_page(pfn);
	if (PageReserved(page)) {
		wait_on_page_bit(page, PG_reserved);
	}
}

///////////////////////////////////////////////////////////
/////////////       Wait Lock Stuff    ////////////////////
///////////////////////////////////////////////////////////
/**
 * Implement Page Reserved lock
 */
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
void __ftfs_lock_page(struct page *page)
{
	DEFINE_WAIT_BIT(wait, &page->flags, PG_reserved);
	__wait_on_bit_lock(ftfs_page_waitqueue(page), &wait,
			   ftfs_sleep_on_page, TASK_UNINTERRUPTIBLE);
}

int __ftfs_lock_page_killable(struct page *page)
{
	DEFINE_WAIT_BIT(wait, &page->flags, PG_reserved);
	return __wait_on_bit_lock(ftfs_page_waitqueue(page), &wait,
				  ftfs_sleep_on_page_killable, TASK_KILLABLE);
}

void ftfs_unlock_page(struct page *page)
{
	VM_BUG_ON(!PageReserved(page));
	clear_bit_unlock(PG_reserved, &page->flags);
	smp_mb__after_clear_bit();
	__wake_up_bit(ftfs_page_waitqueue(page), &page->flags, PG_reserved);
}
#else /* LINUX_VERSION_CODE */
/*
 *Copied from filemap.c --- because page_waitqueue is not exported in 4.19 kernel.
 * The workaround is to export the variable page_wait_table
 */
#define PAGE_WAIT_TABLE_BITS 8
#define PAGE_WAIT_TABLE_SIZE (1 << PAGE_WAIT_TABLE_BITS)
static wait_queue_head_t *ftfs_page_waitqueue(struct page *page)
{
	return &ftfs_page_wait_table[hash_ptr(page, PAGE_WAIT_TABLE_BITS)];
}

void __ftfs_lock_page(struct page *page)
{
	wait_queue_head_t *q = ftfs_page_waitqueue(page);
	ftfs_wait_on_page_bit_common(q, page, PG_reserved, TASK_UNINTERRUPTIBLE, true);
}

int __ftfs_lock_page_killable(struct page *page)
{
	wait_queue_head_t *q = ftfs_page_waitqueue(page);
	return ftfs_wait_on_page_bit_common(q, page, PG_reserved, TASK_KILLABLE, true);
}

static inline bool ftfs_clear_bit_unlock_is_negative_byte(long nr, volatile void *mem)
{
	clear_bit_unlock(nr, mem);
	return test_bit(PG_waiters, mem);
}

void ftfs_unlock_page(struct page *page)
{
	bool tmp;
	BUILD_BUG_ON(PG_waiters != 7);
	VM_BUG_ON_PAGE(!PageReserved(page), page);
	tmp = ftfs_clear_bit_unlock_is_negative_byte(PG_reserved, &page->flags);
	if (tmp)
		ftfs_wake_up_page_bit(page, PG_reserved);
}
#endif /* LINUX_VERSION_CODE */

/**
 * Mostly copied from filemap.c, but added ftfs_ to origial function names
 * and used PG_reserve flag instead of PG_locked.
 */
int ftfs_trylock_page(struct page *page)
{
	return (likely(!test_and_set_bit_lock(PG_reserved, &page->flags)));
}

void ftfs_lock_page(struct page *page)
{
	might_sleep();
	if (!ftfs_trylock_page(page)) {
		__ftfs_lock_page(page);
	}
}

int ftfs_lock_page_killable(struct page *page)
{
	might_sleep();
	if (!ftfs_trylock_page(page))
		return __ftfs_lock_page_killable(page);
	return 0;
}
