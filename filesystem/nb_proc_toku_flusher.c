/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/types.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include "ftfs_northbound.h"
#include "nb_proc_toku_flusher.h"

static struct proc_dir_entry * flusher_entry;

static ssize_t nb_proc_toku_flusher_read(struct file *file, char __user *buf,
					 size_t count, loff_t *off)
{
	return 0;
}

static ssize_t nb_proc_toku_flusher_write(struct file *file,
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

	if (0 == strcmp(buf, "flush")) {
		ret = bstore_hot_flush_all();
		if (ret)
			goto out;
	}

	ret = count;

out:
	kfree(buf);
	return ret;
}

const struct file_operations toku_proc_flusher_fops = {
	.read = nb_proc_toku_flusher_read,
	.write = nb_proc_toku_flusher_write,
};

int nb_proc_toku_flusher_init(void) {
	flusher_entry = proc_create(TOKU_FLUSHER_PROC, 0666, NULL,
					&toku_proc_flusher_fops);

	if (flusher_entry == NULL) {
		remove_proc_entry(TOKU_FLUSHER_PROC, NULL);
		ftfs_error(__func__, "Failed to initialize toku procfile: %s",
				TOKU_FLUSHER_PROC);
		return -ENOMEM;
	}

	ftfs_log(__func__, "toku procfs entry created");

	return 0;
}

void nb_proc_toku_flusher_exit(void) {
	remove_proc_entry(TOKU_FLUSHER_PROC, NULL);
	ftfs_log(__func__, "toku flusher procfs entry removed");
}
