/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef _FTFS_SOUTHBOUND_H
#define _FTFS_SOUTHBOUND_H

#include <linux/sched.h>

#include <linux/mount.h>
#include <linux/fs_struct.h>
#include <linux/fdtable.h>
#include <linux/nsproxy.h>
#include <linux/cred.h>

#define FTFS_MS_FLAGS (MS_KERNMOUNT) //| MS_PRIVATE

#define SOUTHBOUND_VARS				\
	struct ftfs_southbound southbound;	\
	struct task_struct *sb_tsk;		\
	const struct cred *curr_cred;

#define SOUTHBOUND_ATTACH				\
	sb_tsk = current;				\
	save_task_southbound(sb_tsk, &southbound);	\
	attach_ftfs_southbound(sb_tsk);			\
	sb_override_creds(&curr_cred);

#define SOUTHBOUND_RESTORE				\
	detach_ftfs_southbound(sb_tsk);			\
	restore_task_southbound(sb_tsk, &southbound);	\
	revert_creds(curr_cred);


extern struct vfsmount *ftfs_vfs;
extern struct fs_struct *ftfs_fs;
extern struct files_struct *ftfs_files;
extern struct cred *ftfs_cred;

struct ftfs_southbound {
	struct fs_struct *fs;
	struct files_struct *files;
	struct cred *cred;
};

struct mount {
	struct list_head mnt_hash;
	struct mount *mnt_parent;
	struct dentry *mnt_mountpoint;
	struct vfsmount mnt;
	struct rcu_head mnt_rcu;
#ifdef CONFIG_SMP
	struct mnt_pcp __percpu *mnt_pcp;
#else
	int mnt_count;
	int mnt_writers;
#endif
	struct list_head mnt_mounts;    /*  list of children, anchored here */
	struct list_head mnt_child;     /*  and going through their mnt_child */
	struct list_head mnt_instance;  /*  mount instance on sb->s_mounts */
	const char *mnt_devname;        /*  Name of device e.g. /dev/dsk/hda1 */
	struct list_head mnt_list;
	struct list_head mnt_expire;    /*  link in fs-specific expiry list */
	struct list_head mnt_share;     /*  circular list of shared mounts */
	struct list_head mnt_slave_list;/*  list of slave mounts */
	struct list_head mnt_slave;     /*  slave list entry */
	struct mount *mnt_master;       /*  slave is on master->mnt_slave_list */
	struct mnt_namespace *mnt_ns;   /*  containing namespace */
	struct mountpoint *mnt_mp;      /*  where is it mounted */
#ifdef CONFIG_FSNOTIFY
	struct hlist_head mnt_fsnotify_marks;
	__u32 mnt_fsnotify_mask;
#endif
	int mnt_id;                     /*  mount identifier */
	int mnt_group_id;               /*  peer group identifier */
	int mnt_expiry_mark;            /*  true if marked for expiry */
	int mnt_pinned;
	struct path mnt_ex_mountpoint;
};


int resolve_ftfs_southbound_symbols(void);

void sb_override_creds(const struct cred **saved);
int attach_ftfs_southbound(struct task_struct *tsk);
void detach_ftfs_southbound(struct task_struct *tsk);
int init_ftfs_southbound(void);
void put_ftfs_southbound(void);

int sb_private_mount(const char *dev_name, const char *fstype, void *data);
int sb_private_umount(void);

int in_southbound_context(struct task_struct *tsk);

void save_task_southbound(struct task_struct *tsk,
			struct ftfs_southbound *save);
void restore_task_southbound(struct task_struct *tsk,
			struct ftfs_southbound *saved);
#endif /* _FTFS_SOUTHBOUND_H */
