/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/module.h>    // included for all kernel modules
#include <linux/kernel.h>    // included for KERN_INFO
#include <linux/init.h>      // included for __init and __exit macros
#include <linux/err.h>
#include <linux/gfp.h>
#include <linux/string.h>
#include <linux/moduleparam.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include <linux/fs.h>
#include <linux/fs_struct.h>
#include <asm/uaccess.h>
#include <linux/kallsyms.h>
#include <linux/dcache.h>
#include "ftfs_southbound.h"
#include "sb_misc.h"
#include "sb_malloc.h"
#include "sb_files.h"
#include "sb_dir.h"
#include "sb_error.h"
#include "ftfs.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("BetrFS.org");
MODULE_DESCRIPTION("B^e-tree file system");

static char *sb_dev = NULL;
module_param(sb_dev, charp, 0);
MODULE_PARM_DESC(sb_dev, "the southbound file system's block device");

static char *sb_fstype = NULL;
module_param(sb_fstype, charp, 0);
MODULE_PARM_DESC(sb_fstype, "the southbound file system type (ex: ext4)");

/* Do not remove --This filename is not the testname passed to toku_run_test,
 * it is a test file name used by one of portability tests which used
 * getenv(toku_test_filename) before getenv is removed.*/
char * ftfs_test_filename = NULL;
module_param(ftfs_test_filename, charp, 0);
MODULE_PARM_DESC(ftfs_test_filename, "this is the filename for portability test");

int toku_ncpus = (1<<30);
module_param(toku_ncpus, int, 0);
MODULE_PARM_DESC(toku_ncpus, "Limit on the number of CPUs used by the FT code");

static struct proc_dir_entry *toku_proc_entry;
static int last_result;

void ftfs_error (const char * function, const char * fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	printk(KERN_CRIT "ftfs error: %s: ", function);
	vprintk(fmt, args);
	printk(KERN_CRIT "\n");
	va_end(args);
}

//samething as ftfs_error...when ftfs fs calls needs to dump info out
void ftfs_log(const char * function, const char * fmt, ...)
{
#ifdef FTFS_DEBUG
	va_list args;
	va_start(args, fmt);
	printk(KERN_ALERT "ftfs log: %s: ", function);
	vprintk(fmt, args);
	printk(KERN_ALERT "\n");
	va_end(args);
#endif
}


/**
 * A fairly useless function. We just read the result of the last test
 * run (pass or fail).
 */
ssize_t toku_read_proc(struct file *filp, char __user *buf, size_t count,
		       loff_t *off)
{
	int res;
	char tmp[20];

	if (*off)
	  return 0;

	res = sprintf(tmp, "[%d]\n", last_result);
	if (count < res)
		res = count;

	if (copy_to_user(buf, tmp, res))
		ftfs_error(__func__, "could not copy all bytes to user");

	*off += res;

	return res;
}

ssize_t toku_write_proc(struct file *file, const char __user *buffer,
		    size_t count, loff_t *offp)
{

	int i, ret;
	char *buf;

	buf = kmalloc(count+1, GFP_KERNEL);

	if (!buf) {
		ftfs_error(__func__, "toku_write_proc: out of memory");
		return -ENOMEM;
	}

	ret = copy_from_user(buf, buffer, count);
	if (ret) {
		ftfs_error(__func__, "toku_write_proc: bad buffer");
		return ret;
	}

	for (i = 0; i < count; i++) {
		if (buf[i] == '\n') {
			buf[i] = '\0';
			break;
		}
	}

	buf[count] = '\0';

	//last_result = run_test(buf);
	ret = thread_run_test(buf);

	kfree(buf);

	// If the test failed, return an error code
	if (ret < 0) return ret;

	return count;
}

const struct file_operations toku_proc_fops = {
	.read = toku_read_proc,
	.write = toku_write_proc,
};

static int toku_test_init(void)
{
	last_result = 0;
	/* create proc file */
	toku_proc_entry = proc_create(TOKU_PROC_NAME, 0666, NULL,
				      &toku_proc_fops);

	if (toku_proc_entry == NULL) {
		remove_proc_entry(TOKU_PROC_NAME, NULL);
		ftfs_error(__func__, "Failed to initialize toku procfile: %s",
				TOKU_PROC_NAME);
		return -ENOMEM;
	}

	ftfs_log(__func__, "toku procfs entry created");

	return 0;
}

/*
 * If an init function is provided, an exit function must also be provided
 * to allow module unload.
 */
static void toku_test_exit(void)
{
	remove_proc_entry(TOKU_PROC_NAME, NULL);
	ftfs_log(__func__, "toku procfs entry removed");
}

static void __exit ftfs_module_exit(void)
{
	put_ftfs_southbound();

	if (ftfs_private_umount())
		ftfs_error(__func__, "unable to umount ftfs southbound");
	destroy_ftfs_vmalloc_cache();
	toku_test_exit();
}

static int resolve_ftfs_symbols(void)
{
	if (resolve_ftfs_southbound_symbols() ||
	    resolve_ftfs_files_symbols() ||
	    resolve_ftfs_dir_symbols() ||
	    resolve_ftfs_malloc_symbols() ||
	    resolve_toku_misc_symbols()) {
		return -EINVAL;
	}
	return 0;
}

static int __init ftfs_module_init(void)
{
	int ret;
	void *data = NULL;

	if (!sb_dev) {
		ftfs_error(__func__, "no mount device for ftfs_southbound!");
		return -EINVAL;
	}

	if (!sb_fstype) {
		ftfs_error(__func__, "no fstype for ftfs_southbound!");
		return -EINVAL;
	}

	ret = resolve_ftfs_symbols();
	if (ret) {
		ftfs_error(__func__, "could not resolve all symbols!");
		return ret;
	}

	ftfs_log(__func__, "Successfully loaded all ftfs_files.c symbols");


	/*
	 * Now we create a disconnected mount for our southbound file
	 * system. It will not be inserted into any mount trees, but
	 * we pin a global struct vfsmount that we use for all path
	 * resolution.
	 */

	ret = ftfs_private_mount(sb_dev, sb_fstype, data);
	if (ret) {
		ftfs_error(__func__, "can't mount southbound");
		return ret;
	}

	BUG_ON(ftfs_fs);
	BUG_ON(ftfs_files);

	/*
	 * The southbound "file system context" needs to be created to
	 * force all fractal tree worker threads to "see" our file
	 * system as if they were running in user space.
	 */

	ret = init_ftfs_southbound();
	if (ret) {
		ftfs_error(__func__, "can't init southbound_fs");
		return ret;
	}

	ret = init_ftfs_vmalloc_cache();
	if (ret) {
		ftfs_error(__func__, "can't init vmalloc caches");
		return ret;
	}

	return toku_test_init();
}

module_init(ftfs_module_init);
module_exit(ftfs_module_exit);
