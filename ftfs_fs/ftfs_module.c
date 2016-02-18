#include <linux/module.h>
#include "ftfs_fs.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Stony Brook University");
MODULE_DESCRIPTION("Fractal Tree File System");

static int __init beg_ftfs_fs(void)
{
	return init_ftfs_fs();
}

static void __exit end_ftfs_fs(void)
{
	exit_ftfs_fs();
}


module_init(beg_ftfs_fs);
module_exit(end_ftfs_fs);
