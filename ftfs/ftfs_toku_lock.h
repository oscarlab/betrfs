#ifndef _FTFS_ERROR_H
#define _FTFS_ERROR_H

#include <linux/list.h>
#include <linux/spinlock.h>
#include <linux/spinlock_types.h>

struct mutex lock_list;
DEFINE_MUTEX(lock_list);

struct toku_lockfile {
	char *fname;
	struct list_head list;
};

struct list_head toku_lockfile_list;
LIST_HEAD(toku_lockfile_list);

int ftfs_toku_lock_file(const char *fname, size_t len);
int ftfs_toku_unlock_file(const char *fname, size_t len);

#endif
