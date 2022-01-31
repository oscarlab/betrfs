/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file implements the hooks needed to make a kernel module.
 */

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
#include "ftfs.h"
#include "ftfs_northbound.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("BetrFS.org");
MODULE_DESCRIPTION("B^e-tree file system");

int toku_ncpus = (1<<30);
module_param(toku_ncpus, int, 0);
MODULE_PARM_DESC(toku_ncpus, "Limit on the number of CPUs used by the FT code");
extern void toku_ydb_destroy(void);
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
	exit_ftfs_fs();

	if (sb_private_umount())
		ftfs_error(__func__, "unable to umount ftfs southbound");
}

static int resolve_ftfs_symbols(void)
{
	if (resolve_ftfs_southbound_symbols() ||
	    resolve_sb_files_symbols() ||
	    resolve_sb_malloc_symbols() ||
	    resolve_toku_misc_symbols()) {
		return -EINVAL;
	}
	return 0;
}

static int __init ftfs_module_init(void)
{
	int ret;

	// Try manually init-ing this field
	ftfs_vfs = NULL;

	ret = resolve_ftfs_symbols();
	if (ret) {
		ftfs_error(__func__, "could not resolve all symbols!");
		return ret;
	}

	ftfs_log(__func__, "Successfully loaded all ftfs_files.c symbols");

	ret = init_ftfs_fs();
	if (ret) {
		ftfs_error(__func__, "failed to initialize ftfs_fs");
		return ret;
	}

	return 0;
}

module_init(ftfs_module_init);
module_exit(ftfs_module_exit);
