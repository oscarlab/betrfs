/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <linux/uaccess.h>

#define BUFSIZE  100

unsigned long just_inserted_pfn = 0;

static ssize_t pfn_write(struct file *file, const char __user *ubuf,
			 size_t count, loff_t *ppos) 
{
	return -EROFS;
}

static ssize_t pfn_read(struct file *file, char __user *ubuf,
			size_t count, loff_t *ppos) 
{
	char buf[BUFSIZE];
	int len=0;
	if(*ppos > 0 || count < BUFSIZE)
		return 0;
	len += scnprintf(buf, BUFSIZE, "pfn = %lx\n", just_inserted_pfn);

	if(copy_to_user(ubuf, buf, len))
		return -EFAULT;
	*ppos = len;
	return len;
}

static struct file_operations pfn_ops =
{
	.owner = THIS_MODULE,
	.read = pfn_read,
	.write = pfn_write,
};

static struct proc_dir_entry *pfn_ent;

int proc_pfn_init(void)
{
	pfn_ent = proc_create("ftfs_pfn", 0660, NULL, &pfn_ops);
	return 0;
}

void proc_pfn_deinit(void)
{
	proc_remove(pfn_ent);
}
