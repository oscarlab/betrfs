/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound.
 *
 * It largely implements "glue" code for mounting the
 * southbound file system and doing directory operations
 * on the southbound.
 *
 * Some of this, if not all, can probably be removed once we
 * get rid of other copied kernel code that can be eliminated
 * via SFS.
 */

#include <linux/kallsyms.h>
#include <linux/slab.h>
#include <asm/page_types.h>
#include <linux/fs_struct.h>
#include <linux/mount.h>
#include "ftfs_southbound.h"
#include "ftfs.h"
//#include "ftfs_stat.h"

static DEFINE_MUTEX(ftfs_southbound_lock);

typedef void (* exit_files_t)(struct task_struct *tsk);
typedef void (* exit_fs_t)(struct task_struct *tsk);
typedef struct files_struct *(* dup_fd_t)(struct files_struct *, int *);
typedef void (* set_fs_root_t)(struct fs_struct *, const struct path *);
typedef void (* set_fs_pwd_t)(struct fs_struct *, const struct path *);
typedef struct fs_struct *(* copy_fs_struct_t)(struct fs_struct *);
typedef struct fs_struct *(* free_fs_struct_t)(struct fs_struct *);
typedef void (* put_filesystem_t)(struct file_system_type *fs);
typedef void (* put_files_struct_t)(struct files_struct *files);
typedef void (* put_files_struct_t)(struct files_struct *files);

DECLARE_SYMBOL_FTFS(exit_files);
DECLARE_SYMBOL_FTFS(exit_fs);
DECLARE_SYMBOL_FTFS(dup_fd);
DECLARE_SYMBOL_FTFS(set_fs_root);
DECLARE_SYMBOL_FTFS(set_fs_pwd);
DECLARE_SYMBOL_FTFS(copy_fs_struct);
DECLARE_SYMBOL_FTFS(free_fs_struct);
DECLARE_SYMBOL_FTFS(put_files_struct);
DECLARE_SYMBOL_FTFS(put_filesystem);

/* debugging use only */
typedef unsigned int (* mnt_get_count_t)(struct mount *mnt);
DECLARE_SYMBOL_FTFS(mnt_get_count);

int resolve_ftfs_southbound_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(exit_files);
	LOOKUP_SYMBOL_FTFS(exit_fs);
	LOOKUP_SYMBOL_FTFS(dup_fd);
	LOOKUP_SYMBOL_FTFS(set_fs_root);
	LOOKUP_SYMBOL_FTFS(set_fs_pwd);
	LOOKUP_SYMBOL_FTFS(copy_fs_struct);
	LOOKUP_SYMBOL_FTFS(free_fs_struct);
	LOOKUP_SYMBOL_FTFS(put_files_struct);
	LOOKUP_SYMBOL_FTFS(put_filesystem);

/* debugging use only */
	LOOKUP_SYMBOL_FTFS(mnt_get_count);

	return 0;
}

/* copied from init_files */
static struct files_struct ftfs_files_init = {
	.count          = ATOMIC_INIT(1),
	.fdt            = &ftfs_files_init.fdtab,
	.fdtab          = {
		.max_fds        = NR_OPEN_DEFAULT,
		.fd             = &ftfs_files_init.fd_array[0],
		.close_on_exec  = ftfs_files_init.close_on_exec_init,
		.open_fds       = ftfs_files_init.open_fds_init,
	},
	.file_lock      = __SPIN_LOCK_UNLOCKED(ftfs_files_init.file_lock),
};
/* to get struct mount *: return container_of(mnt, struct mount, mnt); */

struct vfsmount *ftfs_vfs = NULL;
struct fs_struct *ftfs_fs = NULL;
struct files_struct *ftfs_files = NULL;
struct cred *ftfs_cred = NULL;

/* must hold ftfs_southbound_lock */
int __init_ftfs_southbound_files(void)
{
	int err;
	struct files_struct *files;

	BUG_ON(ftfs_files);

	files = ftfs_dup_fd(&ftfs_files_init, &err);
	if (!files) {
		ftfs_error(__func__, "init ftfs_files");
		ftfs_files = NULL;
		return err;
	}

	ftfs_files = files;

	return 0;
}

/* must hold ftfs_southbound_lock */
int __init_ftfs_southbound_fs(void)
{
	struct fs_struct *current_fs = current->fs;
	struct fs_struct *fs;
	struct path path;

	BUG_ON(ftfs_fs);
	BUG_ON(!ftfs_vfs);

	fs = ftfs_copy_fs_struct(current_fs);
	if (!fs) {
		ftfs_error(__func__, "init ftfs_fs");
		ftfs_fs = NULL;
		return -ENOMEM;
	}
	ftfs_log(__func__, "ftfs->fs->umask: %d", fs->umask);

	path.mnt = ftfs_vfs;
	path.dentry = ftfs_vfs->mnt_root;

	ftfs_set_fs_root(fs, &path);
	ftfs_set_fs_pwd(fs, &path);

	ftfs_fs = fs;

	return 0;
}

/* must hold ftfs_southbound_lock */
int __init_ftfs_southbound_cred(void)
{
	BUG_ON(ftfs_cred);
	ftfs_cred = prepare_kernel_cred(NULL);
	return ftfs_cred ? 0 : -ENOMEM;
}

/* takes and releases ftfs_southbound lock */
int init_ftfs_southbound(void)
{
	int ret;

	BUG_ON(!ftfs_vfs);
	BUG_ON(ftfs_fs);
	BUG_ON(ftfs_files);
	BUG_ON(ftfs_cred);

	mutex_lock(&ftfs_southbound_lock);

	ret = __init_ftfs_southbound_fs();
	if (ret) {
		ftfs_error(__func__, "can't init southbound_fs");
		return ret;
	}

	ret = __init_ftfs_southbound_files();
	if (ret) {
		ftfs_error(__func__, "initialize ftfs_files");
		ftfs_free_fs_struct(ftfs_fs);
		return ret;
	}

	ret = __init_ftfs_southbound_cred();
	if (ret) {
		ftfs_error(__func__, "initialize ftfs_cred");
		ftfs_free_fs_struct(ftfs_fs);
		return ret;
	}

	BUG_ON(!ftfs_fs);
	BUG_ON(!ftfs_files);
	BUG_ON(!ftfs_cred);

	mutex_unlock(&ftfs_southbound_lock);

	return 0;
}

/*
 * Whenever we do any sort of path lookup, we must make sure we are in
 * the namespace of our southbound fs. This is a simple check that our
 * task struct has the right context.
 */
int in_southbound_context(struct task_struct *tsk)
{
	int err;
	mutex_lock(&ftfs_southbound_lock);
	task_lock(tsk);
	err = path_equal(&tsk->fs->root, &ftfs_fs->root);
	task_unlock(tsk);
	mutex_unlock(&ftfs_southbound_lock);
	return err;
}

/*
 * attach to the ftfs fs_struct (path, root, pwd, etc)
 *
 * must hold ftfs_southbound lock
 * takes and relases lock of tsk
 */
static int __attach_ftfs_southbound(struct task_struct *tsk)
{
	struct fs_struct *fs_copy;

	fs_copy = ftfs_copy_fs_struct(ftfs_fs);
	if (!fs_copy) {
		ftfs_error(__func__, "ftfs_fs copy failed");
		return -ENOMEM;
	}

	task_lock(tsk);
	tsk->fs = fs_copy;
	tsk->files = (struct files_struct *)POISON_FREE; /* we should
				   * not be using the file table, so
				   * poison it for testing */
	task_unlock(tsk);

	return 0;
}

/*
 * Insert our global southbound file system state into the currently
 * running proccess' task_struct.
 */
int attach_ftfs_southbound(struct task_struct *tsk)
{
	int ret;

	mutex_lock(&ftfs_southbound_lock);
	if (!ftfs_fs) {
		mutex_unlock(&ftfs_southbound_lock);
		return -EINVAL;
	}
	ret = __attach_ftfs_southbound(tsk);
	mutex_unlock(&ftfs_southbound_lock);

	return ret;
}

#ifdef SOUTHBOUND_DEBUG
static void debug_sb_refcounts(struct task_struct *tsk)
{
	struct fs_struct *fs = tsk->fs;
	BUG_ON(!fs);

	task_lock(tsk);
	spin_lock(&fs->lock);
	BUG_ON(fs->users != 1);
	spin_unlock(&fs->lock);
	task_unlock(tsk);
}
#endif

/*
 * Detach the file system state from the currently running proccess'
 * task_struct. This can be our own, the default state inherited from
 * kthreadd, or anything.
 *
 * no locks necessary
 */
void detach_ftfs_southbound(struct task_struct *tsk)
{
#ifdef SOUTHBOUND_DEBUG
	debug_sb_refcounts(tsk);
#endif
	ftfs_exit_fs(tsk);
}

void save_task_southbound(struct task_struct *tsk,
			struct ftfs_southbound *save)
{
	struct fs_struct *fs;

	if (!save || IS_ERR(save))
		return;

	save->files = tsk->files;
	save->fs = fs = tsk->fs;

	task_lock(tsk);
	if (fs) {
		spin_lock(&fs->lock);
		tsk->fs = NULL;
		spin_unlock(&fs->lock);
	}
	tsk->files = NULL;
	task_unlock(tsk);
}

void sb_override_creds(const struct cred **saved)
{
	*saved = override_creds(ftfs_cred);
}

void restore_task_southbound(struct task_struct *tsk,
			struct ftfs_southbound *saved)
{
	struct fs_struct *fs;

	if (!saved || IS_ERR(saved))
		return;

	fs = saved->fs;

	task_lock(tsk);
	if (fs) {
		spin_lock(&fs->lock);
		tsk->fs = fs;
		spin_unlock(&fs->lock);
	} else {
		tsk->fs = NULL;
	}
	tsk->files = saved->files;
	task_unlock(tsk);
}

/* copy of static function defined in fs/mount.h */
static inline struct mount *real_mount(struct vfsmount *mnt)
{
	return container_of(mnt, struct mount, mnt);
}

/* mount our hidden southbound filesystem */
int sb_private_mount(const char *dev_name, const char *fs_type, void *data)
{
	int err;
	struct vfsmount *vfs_mount;
	struct file_system_type *type;

	BUG_ON(ftfs_vfs);

	if (!dev_name || !*dev_name) {
		err = -EINVAL;
		goto err_out;
	}

	if (!fs_type || !*fs_type) {
		err = -EINVAL;
		goto err_out;
	}

	type = get_fs_type(fs_type);
	// We didn't find the type we wanted
	if (!type) {
		printk(KERN_ERR "Invalid file system type [%s]\n", fs_type);
		return -EINVAL;
	}
	vfs_mount = vfs_kern_mount(type, FTFS_MS_FLAGS, dev_name, data);
	if (!IS_ERR(vfs_mount) && (type->fs_flags & FS_HAS_SUBTYPE) &&
            !vfs_mount->mnt_sb->s_subtype)
                //mnt = ftfs_fs_set_subtype(mnt, fstype);
		BUG();

	/* this causes problems during unmount. only for internal mounts */
	//real_mount(vfs_mount)->mnt_ns = ERR_PTR(-EINVAL);

	ftfs_put_filesystem(type);

	if (IS_ERR(vfs_mount)) {
		err = PTR_ERR(vfs_mount);
		goto err_out;
	}

	mutex_lock(&ftfs_southbound_lock);
	ftfs_vfs = mntget(vfs_mount);
	mutex_unlock(&ftfs_southbound_lock);

	pr_devel("%s mnt_ns=%p\n", __func__, real_mount(vfs_mount)->mnt_ns);

	return 0;

err_out:
	mutex_lock(&ftfs_southbound_lock);
	ftfs_vfs = NULL;
	mutex_unlock(&ftfs_southbound_lock);
	return err;
}

int __list_open_southbound_files(struct super_block *sb);

/* must hold ftfs_southbound lock */
int __ftfs_private_umount(void)
{
	if (may_umount_tree(ftfs_vfs)) {
		kern_unmount(ftfs_vfs);
		ftfs_vfs = NULL;
		return 0;
	} else {
		int cnt;
		BUG_ON(!ftfs_vfs);
		cnt = __list_open_southbound_files(ftfs_vfs->mnt_sb);
		ftfs_error(__func__, "%d files are open\n", cnt);
		return -EBUSY;
	}
}

/* takes and releases ftfs_southbound lock */
int sb_private_umount(void)
{
	int err;

	BUG_ON(!ftfs_vfs);

	mutex_lock(&ftfs_southbound_lock);
	err = __ftfs_private_umount();
	mutex_unlock(&ftfs_southbound_lock);

	return err;
}

/* takes and releases ftfs_southbound lock */
void put_ftfs_southbound(void)
{
	int kill;

	BUG_ON(!ftfs_vfs);
	BUG_ON(!ftfs_fs);

	mutex_lock(&ftfs_southbound_lock);

	mntput(ftfs_vfs);

	spin_lock(&ftfs_fs->lock);
	kill = !--ftfs_fs->users;
	spin_unlock(&ftfs_fs->lock);
	if (kill) {
		ftfs_free_fs_struct(ftfs_fs);
		ftfs_fs = NULL;
	}

	mutex_unlock(&ftfs_southbound_lock);
}

/* FROM fs/file_table.c.
 *
 * These will not be safe, but they are really just for debugging
 */
/*
 *
 * These macros iterate all files on all CPUs for a given superblock.
 * files_lglock must be held globally.
 */
#ifdef CONFIG_SMP

/*
 * These macros iterate all files on all CPUs for a given superblock.
 * files_lglock must be held globally.
 */
#define do_file_list_for_each_entry(__sb, __file)		\
{								\
	int i;							\
	for_each_possible_cpu(i) {				\
		struct list_head *list;				\
		list = per_cpu_ptr((__sb)->s_files, i);		\
		list_for_each_entry((__file), list, f_u.fu_list)

#define while_file_list_for_each_entry				\
	}							\
}

#else

#define do_file_list_for_each_entry(__sb, __file)		\
{								\
	struct list_head *list;					\
	list = &(sb)->s_files;					\
	list_for_each_entry((__file), list, f_u.fu_list)

#define while_file_list_for_each_entry				\
}
#endif // CONFIG_SMP

int __list_open_southbound_files(struct super_block *sb)
{
	int count = 0;
	struct file *f;

	do_file_list_for_each_entry(sb, f) {
		count++;
		ftfs_error(__func__, "%s is still open",
			 f->f_path.dentry->d_name.name);
	} while_file_list_for_each_entry;

	return count;
}

int list_open_southbound_files(void)
{
	int err;

	BUG_ON(!ftfs_vfs);

	mutex_lock(&ftfs_southbound_lock);
	err = __list_open_southbound_files(ftfs_vfs->mnt_sb);
	mutex_unlock(&ftfs_southbound_lock);

	return err;
}

unsigned int southbound_mnt_count(void)
{
	return ftfs_mnt_get_count(real_mount(ftfs_vfs));
}
