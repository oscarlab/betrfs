/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef _FTFS_SOUTHBOUND_H
#define _FTFS_SOUTHBOUND_H

#include <linux/sched.h>

struct ftfs_southbound {
	void *blah;
};

extern size_t db_cachesize;

#define SOUTHBOUND_VARS
#define SOUTHBOUND_ATTACH
#define SOUTHBOUND_RESTORE

static inline int ftfs_super_statfs(struct dentry * d, struct kstatfs * buf)
{ return 0; }
static inline int attach_ftfs_southbound(struct task_struct *tsk) { return 0; }
static inline void save_task_southbound(struct task_struct *tsk,
					struct ftfs_southbound *save) { }
static inline void restore_task_southbound(struct task_struct *tsk,
			struct ftfs_southbound *saved) { }
static inline void detach_ftfs_southbound(struct task_struct *tsk) { }
static inline void destroy_ft_index(void) { }
static inline int init_ft_index(void) { db_cachesize = 1L << 30; return 0; }
static inline int in_southbound_context(struct task_struct *tsk) { return 1; }
static inline int ftfs_debug_break(void) { return 0; }
#endif /* _FTFS_SOUTHBOUND_H */
