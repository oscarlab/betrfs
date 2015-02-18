/*
 * Test Suite of FTFS bdev functionality
 */

#include <linux/module.h>
#include <linux/proc_fs.h>
#include <linux/random.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <asm/uaccess.h>
#include <linux/string.h>
#include <linux/syscalls.h>
#include <linux/fcntl.h>
#include <linux/mm.h>
#include <asm/mman.h>
#include <linux/writeback.h>
#include <linux/highmem.h>
#include <linux/pagemap.h>
#include <linux/block_dev.h>

#define BDEV_NAME "/dev/loop0"
#define BDEV_MODE (FMODE_READ|FMODE_WRITE)

extern int bd_write_block_to_disk(struct block_device *bdev, pgoff_t index, void *src);
extern int bd_read_block_from_disk(struct block_device *bdev, pgoff_t index, void *dst);

int test1(char *buf, char **start, off_t offset, int count, int *eof,
        void *data)
{
    struct block_device *bdev;

    bdev = blkdev_get_by_path(BDEV_NAME, BDEV_MODE,  NULL);
    if(IS_ERR(bdev))
    {
        printk("Cannot open block device for CF\n");
        return -EFAULT;
    }
    
    blkdev_put(bdev, BDEV_MODE); 

    printk(KERN_INFO "BDEV test 1 succeeded\n");
    return 0;
}

int test2(char *buf, char **start, off_t offset, int count, int *eof,
        void *data)
{
    struct block_device *bdev;
    int ret = 0;
    void *bytes;

    bdev = blkdev_get_by_path(BDEV_NAME, BDEV_MODE,  NULL);
    if(IS_ERR(bdev))
    {
        printk("Cannot open block device for CF\n");
        return -EFAULT;
    }

    bytes = kmalloc(PAGE_SIZE, GFP_KERNEL);
    if (!bytes)
	return -ENOMEM;

    memset(bytes, 'J', PAGE_SIZE);
    
    if ((ret = bd_write_block_to_disk(bdev, 0, bytes)))
	goto out;

    blkdev_put(bdev, BDEV_MODE); 

out:
    kfree(bytes);
    printk(KERN_INFO "BDEV test 2 succeeded\n");
    return ret;
}

int test3(char *buf, char **start, off_t offset, int count, int *eof,
        void *data)
{
    struct block_device *bdev;
    int ret = 0;
    void *bytes;

    bdev = blkdev_get_by_path(BDEV_NAME, BDEV_MODE,  NULL);
    if(IS_ERR(bdev))
    {
	    printk("Cannot open block device for CF\n");
	    return -EFAULT;
    }

    bytes = kmalloc(PAGE_SIZE, GFP_KERNEL);
    if (!bytes)
	    return -ENOMEM;

    if ((ret = bd_read_block_from_disk(bdev, 0, bytes)))
	goto out;

    printk(KERN_INFO "First byte is %c\n", ((char*)bytes)[0]);
    
    blkdev_put(bdev, BDEV_MODE); 

out:
    kfree(bytes);
    printk(KERN_INFO "BDEV test 3 succeeded\n");
    return ret;
}

static struct proc_dir_entry *dir;

static int cf_test_module_init(void)
{
    struct proc_dir_entry *entry;

    dir = proc_mkdir("ftst_bdev", NULL);

    entry = create_proc_entry("1", 0666, dir);
    entry->read_proc = test1;
    entry = create_proc_entry("2", 0666, dir);
    entry->read_proc = test2;
    entry = create_proc_entry("3", 0666, dir);
    entry->read_proc = test3;

    return 0;
}

static void cf_test_module_exit(void)
{
    remove_proc_entry("1", dir);
    remove_proc_entry("2", dir);
    remove_proc_entry("3", dir);
    remove_proc_entry("cascade_filter", NULL);
}

module_init(cf_test_module_init);
module_exit(cf_test_module_exit);

MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Cascade Filter Test Suite");
