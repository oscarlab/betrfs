/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifdef TOKU_MEMLEAK_DETECT
#include <linux/types.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include <linux/uaccess.h>
#include "nb_proc_toku_memleak_detect.h"
#include "ftfs_southbound.h"

extern void init_mem_trace(void);
extern void free_mem_trace(void);

static struct proc_dir_entry * toku_memleak_entry;
/**
 * A fairly useless function. We don't display anything to user.
 */
static ssize_t nb_proc_toku_memleak_proc_read(struct file *filp, char __user *buf,
					      size_t count, loff_t *off)
{
	return 0;
}

/*
 * this function basically does nothing. can whoever wrote this please
 * fix that or clean it up?
 */
static ssize_t nb_proc_toku_memleak_proc_write(struct file *file,
					       const char __user *buffer,
					       size_t count, loff_t *offp)
{
	int i, ret;
	char *buf;

	printk(KERN_ALERT "start memleak detection\n");
	buf = kmalloc(count+1, GFP_KERNEL);
	if (!buf) {
		printk(KERN_ALERT "toku_write_proc: out of memory\n");
		return -ENOMEM;
	}

	ret = copy_from_user(buf, buffer, count);
	if (ret) {
		printk(KERN_ALERT "toku_write_proc: bad buffer\n");
		goto out;
	}

	for (i = 0; i < count; i++) {
		if (buf[i] == '\n') {
			buf[i] = '\0';
			break;
		}
	}

	buf[count] = '\0';

	if (strcmp(buf, "dump") == 0){
		printk(KERN_ALERT "Printing memleak log\n");
		printk(KERN_ALERT "actually not... maintainer, please fix\n");
	} else {
		printk(KERN_ALERT "Not a valid option for memleak proc. "
			"Use: dump, you used, %s\n", buf);
		ret = -EPERM;
	}

out:
	kfree(buf);
	return ret;
}

const struct file_operations toku_memleak_proc_fops = {
	.read = nb_proc_toku_memleak_proc_read,
	.write = nb_proc_toku_memleak_proc_write,
};

int nb_proc_toku_memleak_detect_init(void)
{
	/* create proc file */
	toku_memleak_entry = proc_create(TOKU_MEMLEAK_PROC_NAME, 0666, NULL,
				      &toku_memleak_proc_fops);
	if (toku_memleak_entry == NULL) {
		remove_proc_entry(TOKU_MEMLEAK_PROC_NAME, NULL);
		printk(KERN_ALERT "Failed to initialize toku procfile: %s\n", TOKU_MEMLEAK_PROC_NAME);
		return -ENOMEM;
	}
	printk(KERN_ALERT "toku memleak procfs entry created\n");

	/* init mem trace hash struct */
	init_mem_trace();
	printk(KERN_ALERT "toku mem trace hash struct created\n");
	return 0;
}

/*
 * If an init function is provided, an exit function must also be provided
 * to allow module unload.
 */
void nb_proc_toku_memleak_detect_exit(void)
{
	/* create proc file */
	free_mem_trace();
	remove_proc_entry(TOKU_MEMLEAK_PROC_NAME, NULL);
	printk(KERN_ALERT "toku memleak procfs entry removed\n");
}
#endif
