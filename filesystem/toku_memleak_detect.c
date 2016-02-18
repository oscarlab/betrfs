#ifdef TOKU_MEMLEAK_DETECT
#include <linux/types.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include "ftfs_fs.h"
#include "toku_memleak_detect.h"

static struct proc_dir_entry * toku_memleak_entry;
/**
 * A fairly useless function. We don't display anything to user.
 */
static ssize_t toku_read_memleak_proc(struct file *filp, char __user *buf,
				      size_t count, loff_t *off)
{
	return 0;
}

/*
 * this function basically does nothing. can whoever wrote this please
 * fix that or clean it up?
 */
static ssize_t toku_write_memleak_proc(struct file *file,
				       const char __user *buffer,
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
		ftfs_log(__func__, "Printing memleak log");
		ftfs_log(__func__, "actually not... maintainer, please fix.");
	} else {
		ftfs_error(__func__, "Not a valid option for memleak proc. "
			   "Use: dump, you used, %s", buf);
		ret = -EPERM;
	}

out:
	kfree(buf);
	return out;
}

const struct file_operations toku_memleak_proc_fops = {
	.read = toku_read_memleak_proc,
	.write = toku_write_memleak_proc,
};

int toku_memleak_detect_init(void)
{
	/* create proc file */
	struct file *fd;

	toku_memleak_entry = proc_create(TOKU_MEMLEAK_PROC_NAME, 0666, NULL,
				      &toku_memleak_proc_fops);

	if (toku_memleak_entry == NULL) {
		remove_proc_entry(TOKU_MEMLEAK_PROC_NAME, NULL);
		ftfs_error(__func__, "Failed to initialize toku procfile: %s",
				TOKU_MEMLEAK_PROC_NAME);
		return -ENOMEM;
	}

	ftfs_log(__func__, "toku memleak procfs entry created");

	/* init mem trace hash struct */
	init_mem_trace();
	ftfs_log(__func__, "toku mem trace hash struct created");

	return 0;
}

/*
 * If an init function is provided, an exit function must also be provided
 * to allow module unload.
 */
void toku_memleak_detect_exit(void)
{
	/* create proc file */
	remove_proc_entry(TOKU_MEMLEAK_PROC_NAME, NULL);
	ftfs_log(__func__, "toku memleak procfs entry removed");
}
#endif
