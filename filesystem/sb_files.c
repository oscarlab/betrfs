/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* sb_files.c
 *
 * This file is part of the "southbound" code.  It implements
 * support for file handles and related abstractions for the
 * ft code.
 *
 * It is also used internally by the southbound directory and stat
 * code.
 */

#include <linux/kernel.h>
#include <linux/kallsyms.h>
#include <linux/mutex.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/file.h>
#include <linux/types.h>
#include <linux/namei.h>
#include <linux/path.h>
#include <linux/fs.h>
#include <linux/dcache.h>
#include <linux/fsnotify.h>
#include <linux/security.h>
#include <linux/uio.h>
#include <linux/mm.h>
#include <linux/falloc.h>
#include <linux/version.h>

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
#include <linux/sched/xacct.h>
#endif /* LINUX_VERSION_CODE */

#include "sb_files.h"
#include "ftfs_southbound.h"
#include "ftfs.h"

typedef int (* do_truncate_t)(struct dentry *, loff_t, unsigned int, struct file *);
typedef int (* __close_fd_t)(struct files_struct *, unsigned);
typedef void (* __set_open_fd_t)(int, struct fdtable *);
typedef int (* __alloc_fd_t)(struct files_struct *, unsigned, unsigned, unsigned);
typedef int (* __fd_install_t)(struct files_struct *, unsigned int, struct file *);
typedef int (* rw_verify_area_t)(int, struct file *, const loff_t *, size_t);
typedef int (* kern_path_t)(const char *, unsigned, struct path *);
typedef struct dentry *(* kern_path_locked_t)(const char *, struct path *);
typedef int (* user_path_at_empty_t)(int, const char *, unsigned, struct path *, int *);
typedef long (*do_utimes_t) (int, const char*, struct timespec *, int);
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
typedef long (* sys_close_t)(unsigned int fd);
typedef int (* sys_symlink_t)(const char*, const char *);
typedef long (*sys_sync_t) (void);
typedef long (*sys_link_t) (const char*, const char*);
#else /* LINUX_VERSION_CODE */
typedef long (*ksys_sync_t) (void);
#endif /* LINUX_VERSION_CODE */

DECLARE_SYMBOL_FTFS(do_truncate);
DECLARE_SYMBOL_FTFS(__close_fd);
DECLARE_SYMBOL_FTFS(__alloc_fd);
DECLARE_SYMBOL_FTFS(__fd_install);
DECLARE_SYMBOL_FTFS(rw_verify_area);
DECLARE_SYMBOL_FTFS(kern_path);
DECLARE_SYMBOL_FTFS(kern_path_locked);
DECLARE_SYMBOL_FTFS(user_path_at_empty);
DECLARE_SYMBOL_FTFS(do_utimes);

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
DECLARE_SYMBOL_FTFS(sys_close);
DECLARE_SYMBOL_FTFS(sys_symlink);
DECLARE_SYMBOL_FTFS(sys_sync);
DECLARE_SYMBOL_FTFS(sys_link);

#if BITS_PER_LONG == 32
typedef long (* sys_ftruncate64_t)(unsigned int, loff_t);
DECLARE_SYMBOL_FTFS(sys_ftruncate64);
#else
typedef long (* sys_ftruncate_t)(unsigned int, loff_t);
DECLARE_SYMBOL_FTFS(sys_ftruncate);
#endif

#else /* LINUX_VERSION_CODE */
DECLARE_SYMBOL_FTFS(ksys_sync);
#endif /* LINUX_VERSION_CODE */

int resolve_sb_files_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(do_truncate);
	LOOKUP_SYMBOL_FTFS(__close_fd);
	LOOKUP_SYMBOL_FTFS(__alloc_fd);
	LOOKUP_SYMBOL_FTFS(__fd_install);
	LOOKUP_SYMBOL_FTFS(rw_verify_area);
	LOOKUP_SYMBOL_FTFS(kern_path);
	LOOKUP_SYMBOL_FTFS(kern_path_locked);
	LOOKUP_SYMBOL_FTFS(user_path_at_empty);
	LOOKUP_SYMBOL_FTFS(do_utimes);
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	LOOKUP_SYMBOL_FTFS(sys_symlink);
	LOOKUP_SYMBOL_FTFS(sys_close);
	LOOKUP_SYMBOL_FTFS(sys_link);
	LOOKUP_SYMBOL_FTFS(sys_sync);

#if BITS_PER_LONG == 32
	LOOKUP_SYMBOL_FTFS(sys_ftruncate64);
#else
	LOOKUP_SYMBOL_FTFS(sys_ftruncate);
#endif

#else /* LINUX_VERSION_CODE */
	LOOKUP_SYMBOL_FTFS(ksys_sync);
#endif /* LINUX_VERSION_CODE */
	return 0;
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
/*
 * this is a hack. we should instead change the kernel to have a
 * helper that takes a files_struct
 * same as fget_light in file.c
 */
struct file *sb_fget_light(unsigned int fd, int *fput_needed)
{
	struct file *file;

	struct files_struct *files = ftfs_files; /* only line changed */

	*fput_needed = 0;
	if (atomic_read(&files->count) == 1) {
		file = fcheck_files(files, fd);
		if (file && (file->f_mode & FMODE_PATH))
			file = NULL;
	} else {
		rcu_read_lock();
		file = fcheck_files(files, fd);
		if (file) {
			if (!(file->f_mode & FMODE_PATH) &&
			    atomic_long_inc_not_zero(&file->f_count))
				*fput_needed = 1;
			else
				/* Didn't get the reference, someone's freed */
				file = NULL;
		}
		rcu_read_unlock();
	}

	return file;
}
#else /* LINUX_VERSION_CODE */
/* copied from __fdget() in fs/file.c
 *
 * We can not use kernel's function directly because we have our own file lists.
 * Kernel: https://elixir.bootlin.com/linux/v4.19.99/source/fs/file.c#L733
 * Us: struct files_struct *files = ftfs_files;
 */
unsigned long __sb_fdget(unsigned int fd)
{
	struct files_struct *files = ftfs_files;
	struct file *file;

	if (atomic_read(&files->count) == 1) {
		file = __fcheck_files(files, fd);
		if (!file || unlikely(file->f_mode & FMODE_PATH))
			return 0;
		return (unsigned long)file;
	}
	rcu_read_lock();
loop:
	file = fcheck_files(files, fd);
	if (file) {
		if (file->f_mode & FMODE_PATH)
			file = NULL;
		else if (!get_file_rcu(file))
			goto loop;
	}
	rcu_read_unlock();
	if (!file)
		return 0;
	return FDPUT_FPUT | (unsigned long)file;
}

unsigned long __sb_fdget_pos(unsigned int fd)
{
	unsigned long v = __sb_fdget(fd);
	struct file *file = (struct file *)(v & ~3);

	if (file && (file->f_mode & FMODE_ATOMIC_POS)) {
		if (file_count(file) > 1) {
			v |= FDPUT_POS_UNLOCK;
			mutex_lock(&file->f_pos_lock);
		}
	}
	return v;
}
#endif /* LINUX_VERSION_CODE */

int sb_get_unused_fd_flags(unsigned flags)
{
	return ftfs___alloc_fd(ftfs_files, 0, rlimit(RLIMIT_NOFILE), flags);
}

/* see put_unused_fd */
void sb_put_unused_fd(unsigned int fd)
{
	struct files_struct *files = ftfs_files;
	struct fdtable *fdt;

	spin_lock(&files->file_lock);
	fdt = files_fdtable(files);
	__clear_bit(fd, fdt->open_fds);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,99)
	__clear_bit(fd / BITS_PER_LONG, fdt->full_fds_bits);
#endif /* LINUX_VERSION_CODE */
	if (fd < files->next_fd)
		files->next_fd = fd;
	spin_unlock(&files->file_lock);
}

void sb_fd_install(unsigned int fd, struct file *file)
{
	ftfs___fd_install(ftfs_files, fd, file);
}

/*
similar to do_sys_open but with southbound context switch
 */
int open(const char *pathname, int flags, umode_t mode)
{
	int fd;
	struct file *f;
	SOUTHBOUND_VARS;

	fd = sb_get_unused_fd_flags(flags);
	if(fd < 0)
		return fd;

	SOUTHBOUND_ATTACH;
	f = filp_open(pathname, flags, mode);
	SOUTHBOUND_RESTORE;

	if (IS_ERR(f)) {
		sb_put_unused_fd(fd);
#ifdef FTFS_DEBUG_IO
		ftfs_error(__func__, "filp_open (%s) failed:%d",pathname, PTR_ERR(f));
#endif
		return PTR_ERR(f);
	}

	fsnotify_open(f);
	sb_fd_install(fd, f);

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "open(%s): %d\n", pathname, fd);
#endif

	return fd;
}

int open64(const char *pathname, int flags, umode_t mode)
{
	return open(pathname, flags | O_LARGEFILE, mode);
}

int close(int fd)
{
	int ret;
	ret = ftfs___close_fd(ftfs_files, fd);

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "close: %d\n", fd);
#endif

	return ret;
}

/* exactly vfs_write, minus the userspace access check. maybe use vfs_write? */
static ssize_t ftfs_write(struct file *file, const char *buf, size_t count,
			  loff_t *pos)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	ssize_t ret;

	mm_segment_t saved = get_fs();
	set_fs(get_ds());

	if (!(file->f_mode & FMODE_WRITE)) {
		set_fs(saved);
		return -EBADF;
	}

	if (!file->f_op || (!file->f_op->write && !file->f_op->aio_write)) {
		set_fs(saved);
		return -EINVAL;
	}

	ret = ftfs_rw_verify_area(WRITE, file, pos, count);
	if (ret >= 0) {
		count = ret;
		file_start_write(file);
		if (file->f_op->write)
			ret = file->f_op->write(file, buf, count, pos);
		else
			ret = do_sync_write(file, buf, count, pos);
		if (ret > 0) {
			fsnotify_modify(file);
			add_wchar(current, ret);
		}
		inc_syscw(current);
		file_end_write(file);
	}
	set_fs(saved);
	return ret;
#else /* LINUX_VERSION_CODE */
	return kernel_write(file, buf, count, pos);
#endif /* LINUX_VERSION_CODE */
}

/* similar to write syscall in read_write.c */
ssize_t write(int fd, const void *buf, size_t count)
{
	struct fd f;
	ssize_t ret = -EBADF;

	// Should never write to standard in
	BUG_ON(fd == 0);

	/* Special-case a write to stderr or stdout */
	if (fd == 1 || fd == 2) {
		// We are trusting the caller not to pass a bad count or string
		printk(KERN_ERR "%s\n", (const char *) buf);
		ret = count;
	} else {

		f = sb_fdget_pos(fd);
		if (f.file) {
			loff_t pos = f.file->f_pos;
			ret = ftfs_write(f.file, buf, count, &pos);
			if (ret > 0)
				f.file->f_pos = pos;
			sb_fdput_pos(f);
		}
	}

	return ret;
}

/* mostly pwrite64 from read_write.c */
ssize_t pwrite64(int fd, const void *buf, size_t count, loff_t pos)
{
	struct fd f;
	ssize_t ret = -EBADF;

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "pwrite %d [%llu, %llu)\n", fd, pos, pos+count);
#endif // FTFS_DEBUG_IO

	if (pos < 0)
		return -EINVAL;

	f = sb_fdget(fd);
	if (f.file) {
		ret = -ESPIPE;
		if (f.file->f_mode & FMODE_PWRITE) {
			ret = ftfs_write(f.file, buf, count, &pos);
		} else {
			ftfs_error(__func__, "fmode not FMODE_PWRITE: %d", f.file->f_mode);
		}
		sb_fdput(f);
	}

	return ret;
}

/* (p)read(64) need this to be changed if we want to avoid the user copy */
static ssize_t ftfs_read(struct file *f, char *buf, size_t count, loff_t *pos)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	ssize_t ret;
	mm_segment_t saved = get_fs();
	set_fs(get_ds());
	ret = vfs_read(f, buf, count, pos);
	set_fs(saved);
	return ret;
#else /* LINUX_VERSION_CODE */
	return kernel_read(f, buf, count, pos);
#endif /* LINUX_VERSION_CODE */
}

/* pread64 syscall from read_write.c */
ssize_t pread64(int fd, void *buf, size_t count, loff_t pos)
{
	struct fd f;
	ssize_t ret = -EBADF;

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "pread %d [%llu, %llu)\n", fd, pos, pos+count);
#endif // FTFS_DEBUG_IO

	if (pos < 0)
		return -EINVAL;

	f = sb_fdget(fd);
	if (f.file) {
		ret = -ESPIPE;
		if (f.file->f_mode & FMODE_PREAD)
			ret = ftfs_read(f.file, buf, count, &pos);
		sb_fdput(f);
	}

	return ret;
}

ssize_t read(int fd, void *buf, size_t count)
{
	struct fd f;
	ssize_t ret = -EBADF;

	f = sb_fdget_pos(fd);
	if (f.file) {
		loff_t pos = f.file->f_pos;
#ifdef FTFS_DEBUG_IO
		printk(KERN_NOTICE "read %d [%llu, %llu)\n", fd, pos,
		       pos+count);
#endif // FTFS_DEBUG_IO
		ret = ftfs_read(f.file, buf, count, &pos);
		if (ret >= 0)
			f.file->f_pos = pos;
		sb_fdput_pos(f);
	}

	return ret;
}

/* do_fsync in fsycn.c */
static int sync_helper(int fd, int data_sync)
{
	struct fd f;
	int ret = -EBADF;

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "fsync (%d)\n", fd);
#endif //FTFS_DEBUG_IO

	f = sb_fdget(fd);

	if (f.file) {
		ret = vfs_fsync(f.file, data_sync); // fine in NB context
		sb_fdput(f);
	}

	return ret;
}

/* fsync in fsync.c */
int fsync(int fd)
{
  	return sync_helper(fd, 0);
}

/* fdatasync in fsync.c */
int fdatasync(int fd)
{
  	return sync_helper(fd, 1);
}

loff_t lseek64(int fd, loff_t offset, int whence) {
	loff_t retval;
	struct fd f;

	f = sb_fdget_pos(fd);
	if (!f.file)
		return -EBADF;
	retval = -EINVAL;

	if (whence <= SEEK_MAX)
		retval = vfs_llseek(f.file, offset, whence);

	sb_fdput_pos(f);

	return retval;
}

//purely for testing
long sync(void)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
	return ftfs_sys_sync();
#else /* LINUX_VERSION_CODE */
	return ftfs_ksys_sync();
#endif /* LINUX_VERSION_CODE */
}

int fcopy_dio(const char * src, const char * dest, int64_t size) {
	//int saved_error;
	int fd_src, fd_dest;
	char *buf = NULL;
	ssize_t offset = 0;
	int ret;

	fd_src = open64(src, O_RDONLY, 0755);
	if (fd_src < 0) {
		printk(KERN_ALERT "src %s failed to open", src);
		return -EINVAL;
	}

	fd_dest = open64(dest, O_WRONLY, 0755);
	if (fd_dest < 0) {
		printk(KERN_ALERT "dest %s failed to open", dest);
		return fd_dest;
	}

	ret = sb_sfs_truncate_page_cache(fd_src);
	BUG_ON(ret < 0);

	ret = sb_sfs_truncate_page_cache(fd_dest);
	BUG_ON(ret < 0);

	buf = kmalloc(4096, GFP_KERNEL);
	BUG_ON(!buf);

	while (offset < size) {
		ret = sb_sfs_dio_read_write(fd_src, buf, 4096, offset, 0, NULL);
		BUG_ON(ret != 4096);
		ret = sb_sfs_dio_read_write(fd_dest, buf, 4096, offset, 1, NULL);
		BUG_ON(ret != 4096);
		offset += 4096;
	}
	kfree(buf);

	fsync(fd_dest);
	close(fd_src);
	close(fd_dest);
	printk(KERN_ALERT "%s is done\n", __func__);
	return 0;
}

/* wkj: is this ever used? grepping found nothing in ft source.
 *
 *  answer: only used in tests... do not waste time optimizing this.
 *
 * YZJ: only copies first size bytes of src to dest, intead of copy
 * the whole file.
 */
int fcopy(const char * src, const char * dest, int64_t size) {
	//int saved_error;
	int fd_src, fd_dest;
	char buf[512];
	ssize_t nread;
	ssize_t offset = 0;

	fd_src = open64(src, O_RDONLY, 0755);
	if (fd_src < 0) {
		ftfs_log(__func__,"src %s failed to open", src);
		return fd_src;
	}

	fd_dest = open64(dest, O_WRONLY, 0755);
	if (fd_dest < 0) {
		ftfs_log(__func__,"dest %s failed to open", dest);
		return fd_dest;
	}
	while (nread = read(fd_src, buf, sizeof buf), nread > 0) {
		char * out = buf;
		int nwritten = 0;
		offset += nread;
		do {
			nwritten = write(fd_dest, out, nread);
			if (nwritten >= 0) {
				nread -= nwritten;
				out += nwritten;
			}
		} while (nread > 0);
		/* We copied enough byte, it is time to break */
		if (offset >= size)
			break;
	}
	if(nread == 0) {
		if(close(fd_dest) < 0) {
			fd_dest = -1;
			goto out_error;
		}
		close(fd_src);
		return 0;
	}
out_error:
	close(fd_src);
	if(fd_dest > 0)
		close(fd_dest);
	return -1;
}
