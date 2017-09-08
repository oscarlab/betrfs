/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef FTFS_PROFILE_H
#define FTFS_PROFILE_H

#include <linux/string.h>


#define FTFS_PROFILE_PROC "ftfs_profile"

int ftfs_profile_init(void);
void ftfs_profile_exit(void);

struct ftfs_unlink_stat {
	ktime_t stage1_if;
	ktime_t stage1_else;
	ktime_t stage2;
	ktime_t rmdir;
	ktime_t destroy_inode;
	ktime_t evict_inode;

	unsigned int count_if;
	unsigned int count_else;
	unsigned int count_rmdir;
	unsigned int count_destroy;
	unsigned int count_evict;
};

extern struct ftfs_unlink_stat unlink_stat;


#endif /* FTFS_PROFILE_H */
