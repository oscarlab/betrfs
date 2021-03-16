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
#include "sb_error.h"
#include "sb_files.h"
#include "sb_dir.h"
#include "ftfs.h"
#include "ftfs_northbound.h"
#include "nb_proc_toku_engine_status.h"
#include "nb_proc_toku_checkpoint.h"
#include "nb_proc_toku_flusher.h"
#include "nb_proc_toku_memleak_detect.h"
#include "nb_proc_toku_dump_node.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Stony Brook University");
MODULE_DESCRIPTION("Fractal Tree File System");

static char *sb_dev = NULL;
module_param(sb_dev, charp, 0);
MODULE_PARM_DESC(sb_dev, "the southbound file system's block device");

static char *sb_fstype = NULL;
module_param(sb_fstype, charp, 0);
MODULE_PARM_DESC(sb_fstype, "the southbound file system type (ex: ext4)");

int toku_ncpus = (1<<30);
module_param(toku_ncpus, int, 0);
MODULE_PARM_DESC(toku_ncpus, "Limit on the number of CPUs used by the FT code");
long txn_count = 0;
long seq_count = 0;
long non_seq_count = 0;
extern int toku_ydb_init(void);
extern void toku_ydb_destroy(void);
extern void printf_count_blindwrite(void);
inline void ftfs_error (const char * function, const char * fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	printk(KERN_CRIT "ftfs error: %s: ", function);
	vprintk(fmt, args);
	printk(KERN_CRIT "\n");
	va_end(args);
}

//samething as ftfs_error...when ftfs fs calls needs to dump info out
inline void ftfs_log(const char * function, const char * fmt, ...)
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
static void __exit ftfs_module_exit(void)
{
	destroy_ft_index();
	destroy_ftfs_vmalloc_cache();
	exit_ftfs_fs();

	put_ftfs_southbound();

	TOKU_MEMLEAK_EXIT;
	toku_engine_status_exit();
	toku_checkpoint_exit();
	toku_flusher_exit();
	toku_dump_node_exit();
	printf_count_blindwrite();
	if (ftfs_private_umount())
		ftfs_error(__func__, "unable to umount ftfs southbound");
	ftfs_error(__func__, "seq count = %ld, non seq count = %ld, txn count = %ld\n", seq_count, non_seq_count, txn_count);
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

	printk(KERN_ERR "ftfs_module_init is called!\n");

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

	ret = init_ftfs_fs();
	if (ret) {
		ftfs_error(__func__, "can't init ftfs_fs");
		return ret;
	}

	ret = toku_engine_status_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku engine proc");
		return ret;
	}

	ret = toku_checkpoint_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku checkpoint proc");
		return ret;
	}

	ret = toku_flusher_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku flusher proc");
		return ret;
	}

	ret = toku_dump_node_init();
	if (ret) {
		ftfs_error(__func__, "can't init toku dump node proc");
		return ret;
	}

	ret = init_ftfs_vmalloc_cache();
	if (ret) {
		ftfs_error(__func__, "can't init vmalloc caches");
		return ret;
	}

	return init_ft_index();
}

module_init(ftfs_module_init);
module_exit(ftfs_module_exit);
