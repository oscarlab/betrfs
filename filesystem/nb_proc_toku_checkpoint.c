/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the northbound code.
 *
 * It implements a /proc file (/proc/toku_checkpoint) that,
 * when written to, forces a checkpoint of the Be-tree.
 */

#include <linux/types.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include "ftfs_northbound.h"
#include "nb_proc_toku_checkpoint.h"

static struct proc_dir_entry * checkpoint_entry;

static ssize_t nb_proc_toku_read_checkpoint_proc(struct file *file, char __user *buf,
						 size_t count, loff_t *off)
{
	return 0;
}

static ssize_t nb_proc_toku_write_checkpoint_proc(struct file *file,
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

	if (0 == strcmp(buf, "checkpoint"))
		bstore_checkpoint();

	ret = count;

out:
	kfree(buf);
	return ret;
}

const struct file_operations toku_proc_checkpoint_fops = {
	.read = nb_proc_toku_read_checkpoint_proc,
	.write = nb_proc_toku_write_checkpoint_proc,
};

int nb_proc_toku_checkpoint_init(void) {
	checkpoint_entry = proc_create(TOKU_CHECKPOINT_PROC, 0666, NULL,
					&toku_proc_checkpoint_fops);

	if (checkpoint_entry == NULL) {
		remove_proc_entry(TOKU_CHECKPOINT_PROC, NULL);
		ftfs_error(__func__, "Failed to initialize toku procfile: %s",
				TOKU_CHECKPOINT_PROC);
		return -ENOMEM;
	}

	ftfs_log(__func__, "toku procfs entry created");

	return 0;
}

void nb_proc_toku_checkpoint_exit(void) {
	remove_proc_entry(TOKU_CHECKPOINT_PROC, NULL);
	ftfs_log(__func__, "toku checkpoint procfs entry removed");
}
