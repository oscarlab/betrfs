#include <linux/kernel.h>
#include <linux/types.h>
#include "ftfs_pthread.h"
#include "ftfs.h"

extern char *strerror(int e);
void set_errno(int ret)
{
	return;
}

int get_errno(void)
{
	return ENOSYS;
}

void ftfs_set_errno(int ret)
{
	set_errno(ret);
}

int ftfs_get_errno(void)
{
	return get_errno();
}


void perror(const char *s)
{
	int e = get_errno();

	printk(KERN_ALERT "%s\n", s);
	printk(KERN_ALERT ": %s\n", strerror(e));
}
