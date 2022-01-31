#include <linux/types.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include "ftfs_northbound.h"
#include "nb_proc_toku_dump_node.h"

static struct proc_dir_entry * dump_node_entry;

static ssize_t nb_proc_toku_read_dump_node(struct file *file, char __user *buf,
					   size_t count, loff_t *off)
{
	return 0;
}

static ssize_t nb_proc_toku_write_dump_node(struct file *file,
					    const char __user *buffer,
					    size_t count, loff_t *offp)
{
	int i, j, ret, r;
	char *buf;
	bool is_data;
	int64_t blocknum;

	buf = kmalloc(count+1, GFP_KERNEL);

	if (!buf) {
		ftfs_error(__func__, "toku_write_proc: out of memory");
		return -ENOMEM;
	}

	ftfs_error(__func__, "%d\n", __LINE__);
	ret = copy_from_user(buf, buffer, count);
	if (ret) {
		ftfs_error(__func__, "toku_write_proc: bad buffer");
		goto out;
	}

	for (i = 0; i < count; i++) {
		if (buf[i] ==  ',' || buf[i] == '\n') {
			buf[i] = '\0';
			break;
		}
	}
	//BUG_ON(i<count-1);

	ftfs_error(__func__, "i=%d,%s\n",i, buf);
	if (0 == strcmp(buf, "meta")) {
		is_data=false;
	} else {
		//BUG_ON(0 == strcmp(buf, "data"));
		is_data=true;

	}

	ftfs_error(__func__, "%d\n", __LINE__);
	for(j=i+1; j<count; j++){
		if(buf[j] == '\n') {
			buf[j] = '\0';
			break;
		}
	}
	buf[count] = '\0';
	blocknum = simple_strtoll(&buf[i+1], NULL, 10);
	ftfs_error(__func__, "blocknum=%ld", blocknum);
	ret = count;
  	r = nb_bstore_dump_node(is_data, blocknum);
	if(r) {
	 	ftfs_error(__func__, "failed");
	}

out:
	kfree(buf);
	return ret;
}

const struct file_operations toku_proc_dump_node_fops = {
	.read = nb_proc_toku_read_dump_node,
	.write = nb_proc_toku_write_dump_node,
};

int nb_proc_toku_dump_node_init(void) {
	dump_node_entry = proc_create(TOKU_DUMP_NODE_PROC, 0666, NULL,
					&toku_proc_dump_node_fops);

	if (dump_node_entry == NULL) {
		remove_proc_entry(TOKU_DUMP_NODE_PROC, NULL);
		ftfs_error(__func__, "Failed to initialize toku procfile: %s",
				TOKU_DUMP_NODE_PROC);
		return -ENOMEM;
	}

	ftfs_log(__func__, "toku procfs entry created");

	return 0;
}

void nb_proc_toku_dump_node_exit(void) {
	remove_proc_entry(TOKU_DUMP_NODE_PROC, NULL);
	ftfs_log(__func__, "toku procfs entry removed");
}
