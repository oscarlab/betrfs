#include <linux/types.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/proc_fs.h>
#include "ftfs_fs.h"
#include "ftfs_profile.h"

static struct proc_dir_entry * ftfs_profile_entry;

static void ftfs_print_unlink_profile(void)
{
	printk("===== unlink =====\n");	
	printk("time of if branch:%lld\n", unlink_stat.stage1_if.tv64);
	printk("time of else branch:%lld\n", unlink_stat.stage1_else.tv64);
	printk("time of stage2:%lld\n", unlink_stat.stage2.tv64);
	printk("time of ftfs_unlink:%lld\n", unlink_stat.stage2.tv64 + 
					     unlink_stat.stage1_else.tv64 +
					     unlink_stat.stage1_if.tv64);
	printk("time of ftfs_rmdir:%lld\n", unlink_stat.rmdir.tv64);
	printk("time of ftfs_destroy_inode:%lld\n", unlink_stat.destroy_inode.tv64);
	printk("time of ftfs_evict_inode:%lld\n", unlink_stat.evict_inode.tv64);
	printk("count of if branch:%u\n", unlink_stat.count_if);
	printk("count of else branch:%u\n", unlink_stat.count_else);
	printk("count of ftfs_rmdir:%u\n", unlink_stat.count_rmdir);
	printk("count of ftfs_destroy_inode:%u\n", unlink_stat.count_destroy);
	printk("count of ftfs_evict_inode:%u\n", unlink_stat.count_evict);
	printk("===== end ======\n");	
	memset(&unlink_stat, 0, sizeof(unlink_stat));
}

static void ftfs_print_unlink_clean_profile(void)
{
	memset(&unlink_stat, 0, sizeof(unlink_stat));
}

static ssize_t ftfs_read_profile_proc(struct file *file, char __user *buf,
				     size_t count, loff_t *off)
{
	return 0;
}

static ssize_t ftfs_write_profile_proc(struct file *file,
				      const char __user *buffer,
				      size_t count, loff_t *offp)
{
	int i, ret;
	char *buf;

	buf = kmalloc(count+1, GFP_KERNEL);

	if (!buf) {
		ftfs_error(__func__, "ftfs_write_profile_proc: out of memory");
		return -ENOMEM;
	}

	ret = copy_from_user(buf, buffer, count);
	if (ret) {
		ftfs_error(__func__, "ftfs_write_profile_proc: bad buffer");
		goto out;
	}

	for (i = 0; i < count; i++) {
		if (buf[i] == '\n') {
			buf[i] = '\0';
			break;
		}
	}

	buf[count] = '\0';

	if (0 == strcmp(buf, "unlink")) {
		ftfs_print_unlink_profile();
	}

	if (0 == strcmp(buf, "unlink_clean")) {
		ftfs_print_unlink_clean_profile();
	}




	ret = count;

out:
	kfree(buf);
	return ret;
}

const struct file_operations ftfs_proc_profile_fops = {
	.read = ftfs_read_profile_proc,
	.write = ftfs_write_profile_proc,
};

int ftfs_profile_init(void) {
	ftfs_profile_entry = proc_create(FTFS_PROFILE_PROC, 0666, NULL,
					&ftfs_proc_profile_fops);

	if (ftfs_profile_entry == NULL) {
		remove_proc_entry(FTFS_PROFILE_PROC, NULL);
		ftfs_error(__func__, "Failed to init profile proc file: %s",
				FTFS_PROFILE_PROC);
		return -ENOMEM;
	}

	ftfs_log(__func__, "ftfs profile procfs entry created");

	return 0;
}

void ftfs_profile_exit(void) {
	remove_proc_entry(FTFS_PROFILE_PROC, NULL);
	ftfs_log(__func__, "toku procfs entry removed");
}
