#include <linux/module.h>
#include <linux/string.h>
#include <linux/slab.h>
#include <linux/mm.h>
#include <linux/kernel.h>
#include <linux/syscalls.h>
#include <asm/mman.h>
#include <linux/block_dev.h>

struct page* bd_get_page(struct block_device *bdev, pgoff_t index)
{
    struct inode *bd_inode = bdev->bd_inode;
    struct address_space *mapping = bd_inode->i_mapping;
    struct page *page;
    int error;

    BUG_ON(!mapping);

    page = find_or_create_page(mapping, index, mapping_gfp_mask(mapping)); 
    if( !PageUptodate(page) )
    {
        error = mapping->a_ops->readpage(NULL, page);
        BUG_ON(error);
        lock_page(page);
    }

    return page;
}

void bd_put_page(struct page *page)
{
    if(PageLocked(page))
        unlock_page(page);
    page_cache_release(page);
}

int bd_read_block_from_disk(struct block_device *bdev, pgoff_t index, void *dst)
{
    struct page *page;
    void *src; 

    if (!dst)
	return -EINVAL;
 
    page = bd_get_page(bdev, index);
    src = kmap(page);
    if (!src)
	return -EINVAL;
    
    memcpy(dst, src, PAGE_SIZE);
    kunmap(page);
    bd_put_page(page);

    return 0;
}
EXPORT_SYMBOL(bd_read_block_from_disk);

int bd_write_block_to_disk(struct block_device *bdev, pgoff_t index, void *src)
{
    struct page *page = NULL;
    loff_t pos;
    int error;
    void *fs_data = NULL;
    void *dst;
    struct address_space *mapping = bdev->bd_inode->i_mapping;

    if (!src)
	return -EINVAL;
 
    pos = index << PAGE_CACHE_SHIFT; 
    error = mapping->a_ops->write_begin(NULL, mapping, pos,
            PAGE_CACHE_SIZE, AOP_FLAG_UNINTERRUPTIBLE, &page, &fs_data);
    BUG_ON(error);
    dst = kmap(page);
    if (!dst)
	return -EINVAL;
    
    memcpy(dst, src, PAGE_SIZE);
    kunmap(page);
    error = mapping->a_ops->write_end(NULL, mapping, pos, PAGE_CACHE_SIZE,
                         PAGE_CACHE_SIZE, page, fs_data);
    BUG_ON(error != PAGE_CACHE_SIZE);

    return 0;
}
EXPORT_SYMBOL(bd_write_block_to_disk);

MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Quotient Filter");
