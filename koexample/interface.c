// interface.c, required interface for every kernel module
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include "cppmod.h"

static int __init cppmod_init(void)
{
	printk(KERN_INFO "cpp module installed\n");
	start_driver();
	return 0;
}

static void __exit cppmod_exit(void)
{
	stop_driver();
	printk(KERN_INFO "cpp module removed\n");
}

module_init(cppmod_init);
module_exit(cppmod_exit);

MODULE_LICENSE("GPL");
