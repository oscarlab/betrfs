/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

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
#include "ftfs_files.h"
#include "ftfs_malloc.h"
#include "ftfs_southbound.h"
#include "ftfs.h"
#include "ftfs_dir.h"
#include "ftfs_error.h"

//typedef int (* do_flock_t)(struct fd, unsigned int);
typedef int (* do_truncate_t)(struct dentry *, loff_t, unsigned int,
			      struct file *);
typedef int (* __close_fd_t)(struct files_struct *, unsigned);
typedef void (* __set_open_fd_t)(int, struct fdtable *);
typedef int (* __alloc_fd_t)(struct files_struct *, unsigned, unsigned, 
			     unsigned);
typedef int (* __fd_install_t)(struct files_struct *, unsigned int,
			     struct file *);
typedef int (* rw_verify_area_t)(int, struct file *, const loff_t *, size_t);
typedef long (* sys_close_t)(unsigned int fd);
typedef int (* kern_path_t)(const char *, unsigned, struct path *);
typedef struct dentry *(* kern_path_locked_t)(const char *, struct path *);
typedef int (* sys_symlink_t)(const char*, const char *);
typedef int (* user_path_at_empty_t)(int, const char *, unsigned,
				     struct path *, int *);
typedef long (*sys_sync_t) (void);
typedef long (*sys_link_t) (const char*, const char*);
typedef long (*do_utimes_t) (int, const char*, struct timespec *, int);

DECLARE_SYMBOL_FTFS(do_truncate);
//DECLARE_SYMBOL_FTFS(do_flock);
DECLARE_SYMBOL_FTFS(__close_fd);
DECLARE_SYMBOL_FTFS(__alloc_fd);
DECLARE_SYMBOL_FTFS(__fd_install);
DECLARE_SYMBOL_FTFS(rw_verify_area);
DECLARE_SYMBOL_FTFS(sys_close);
DECLARE_SYMBOL_FTFS(kern_path);
DECLARE_SYMBOL_FTFS(kern_path_locked);
DECLARE_SYMBOL_FTFS(sys_symlink);
DECLARE_SYMBOL_FTFS(user_path_at_empty);
DECLARE_SYMBOL_FTFS(sys_sync);
DECLARE_SYMBOL_FTFS(sys_link);
DECLARE_SYMBOL_FTFS(do_utimes);

#if BITS_PER_LONG == 32
typedef long (* sys_ftruncate64_t)(unsigned int, loff_t);
DECLARE_SYMBOL_FTFS(sys_ftruncate64);
#else
typedef long (* sys_ftruncate_t)(unsigned int, loff_t);
DECLARE_SYMBOL_FTFS(sys_ftruncate);
#endif

//#ifdef CONFIG_SECURITY
//typedef int (* security_inode_readlink_t)(struct dentry *);
//DECLARE_SYMBOL_FTFS(security_inode_readlink);
//#else
//static inline int ftfs_security_inode_readlink(struct dentry *dentry)
//{ return 0; }
//#endif

int resolve_ftfs_files_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(do_truncate);
//	LOOKUP_SYMBOL_FTFS(do_flock);
	LOOKUP_SYMBOL_FTFS(__close_fd);
	LOOKUP_SYMBOL_FTFS(__alloc_fd);
	LOOKUP_SYMBOL_FTFS(__fd_install);
	LOOKUP_SYMBOL_FTFS(rw_verify_area);
	LOOKUP_SYMBOL_FTFS(sys_close);
	LOOKUP_SYMBOL_FTFS(kern_path);
	LOOKUP_SYMBOL_FTFS(kern_path_locked);
	LOOKUP_SYMBOL_FTFS(sys_symlink);
	LOOKUP_SYMBOL_FTFS(user_path_at_empty);
	LOOKUP_SYMBOL_FTFS(sys_sync);
	LOOKUP_SYMBOL_FTFS(sys_link);
	LOOKUP_SYMBOL_FTFS(do_utimes);
#if BITS_PER_LONG == 32
	LOOKUP_SYMBOL_FTFS(sys_ftruncate64);
#else
	LOOKUP_SYMBOL_FTFS(sys_ftruncate);
#endif

//#ifdef CONFIG_SECURITY
//	LOOKUP_SYMBOL_FTFS(security_inode_readlink);
//#endif
	return 0;
}

/*
 * this is a hack. we should instead change the kernel to have a
 * helper that takes a files_struct
 */
struct file *ftfs_fget_light(unsigned int fd, int *fput_needed)
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

int ftfs_get_unused_fd_flags(unsigned flags)
{
	return ftfs___alloc_fd(ftfs_files, 0, rlimit(RLIMIT_NOFILE), flags);
}

/* see put_unused_fd */
void ftfs_put_unused_fd(unsigned int fd)
{
	struct files_struct *files = ftfs_files;
	struct fdtable *fdt;
	spin_lock(&files->file_lock);
	fdt = files_fdtable(files);
	__clear_bit(fd, fdt->open_fds);
	if (fd < files->next_fd)
		files->next_fd = fd;
	spin_unlock(&files->file_lock);
}

void ftfs_fd_install(unsigned int fd, struct file *file)
{
	ftfs___fd_install(ftfs_files, fd, file);
}

//extern int expand_files(struct files_struct *files, int nr);
//extern int do_dup2(struct files_struct *files,
//		   struct file *file, unsigned fd, unsigned flags);

/* sys_dup3 (file.c) */
/*static int ftfs_dup3(unsigned int oldfd, unsigned int newfd, int flags)
{
	int err = -EBADF;
	struct file *file;
	struct files_struct *files = ftfs_files;

	if ((flags & ~O_CLOEXEC) != 0)
		return -EINVAL;

	if (newfd >= rlimit(RLIMIT_NOFILE))
		return -EBADF;

	spin_lock(&files->file_lock);
	err = expand_files(files, newfd);
	file = fcheck(oldfd);
	if (unlikely(!file))
		goto Ebadf;
	if (unlikely(err < 0)) {
		if (err == -EMFILE)
			goto Ebadf;
		goto out_unlock;
	}
	return do_dup2(files, file, newfd, flags);

Ebadf:
	err = -EBADF;
out_unlock:
	spin_unlock(&files->file_lock);
	return err;
}
*/

/* sys_dup2 (file.c) */
/*static int ftfs_dup2(int oldfd, int newfd)
{
	int retval;
	if (unlikely(newfd == oldfd)) {
		struct files_struct *files = ftfs_files;
		retval = oldfd;
		
		rcu_read_lock();
		if (!fcheck_files(files, oldfd))
			retval = -EBADF;
		rcu_read_unlock();
		return retval;
	}
	retval = ftfs_dup3(oldfd, newfd, 0);
	return return_errno_pos(retval);
}
*/

/*
 * wkj: so that anyone who hasn't yet switched to the new kernel can
 * still compile and run. It just might not work correctly for files
 * opened with O_DIRECT */
#ifndef __O_KERNFS
#warning "you should compile and run the modified ftfs kernel"
#define __O_KERNFS 0
#endif

int open(const char *pathname, int flags, umode_t mode)
{
	int fd;
	struct file *f;
	SOUTHBOUND_VARS;

	flags |= __O_KERNFS;

	fd = ftfs_get_unused_fd_flags(flags);
	if(fd < 0)
		return fd;

	SOUTHBOUND_ATTACH;
	f = filp_open(pathname, flags, mode);
	SOUTHBOUND_RESTORE;

	if (IS_ERR(f)) {
		ftfs_put_unused_fd(fd);
#ifdef FTFS_DEBUG_IO
		ftfs_error(__func__, "filp_open (%s) failed:%d",pathname,
			   PTR_ERR(f));
#endif
		return PTR_ERR(f);
	} else {
		fsnotify_open(f);
		ftfs_fd_install(fd, f);
	}

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "open(%s): %d\n", pathname, fd);
#endif //FTFS_DEBUG_IO

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
#endif	// FTFS_DEBUG_IO

	return ret;
}

/* one line change from open.c */
static long do_sb_ftruncate(unsigned int fd, loff_t length, int small)
{
	struct inode *inode;
	struct dentry *dentry;
	struct fd f;
	int error;

	error = -EINVAL;
	if (length < 0)
		goto out;
	error = -EBADF;
	f = ftfs_fdget(fd);
	if (!f.file)
		goto out;

	/* explicitly opened as large or we are on 64-bit box */
	if (f.file->f_flags & O_LARGEFILE)
		small = 0;

	dentry = f.file->f_path.dentry;
	inode = dentry->d_inode;
	error = -EINVAL;
	if (!S_ISREG(inode->i_mode) || !(f.file->f_mode & FMODE_WRITE))
		goto out_putf;

	error = -EINVAL;
	/* Cannot ftruncate over 2^31 bytes without large file support */
	if (small && length > MAX_NON_LFS)
		goto out_putf;

	error = -EPERM;
	if (IS_APPEND(inode))
		goto out_putf;

	sb_start_write(inode->i_sb);
	error = locks_verify_truncate(inode, f.file, length);
	//if (!error)
	//	error = security_path_truncate(&f.file->f_path);
	if (!error)
		error = ftfs_do_truncate(dentry, length,
					 ATTR_MTIME|ATTR_CTIME, f.file);
	sb_end_write(inode->i_sb);
out_putf:
	ftfs_fdput(f);
out:
	return error;
}

int ftruncate64(int fd, loff_t length)
{
	int err;
	err = do_sb_ftruncate(fd, length, 0);
	return err;
}

/* similar to do_sys_truncate, but without user path lookup */
int truncate64(const char *path, loff_t length)
{
	unsigned int lookup_flags = LOOKUP_FOLLOW;
	struct path p;
	int err;
	SOUTHBOUND_VARS;
	if (length < 0)
		return -EINVAL;
	SOUTHBOUND_ATTACH;
retry:
	err = ftfs_kern_path(path, lookup_flags, &p);
	if (!err) {
		err = vfs_truncate(&p, length); // I think this is safe in NB
		path_put(&p);
	}
	if (retry_estale(err, lookup_flags)) {
		lookup_flags |= LOOKUP_REVAL;
		goto retry;
	}
	SOUTHBOUND_RESTORE;

	return err;
}


int do_sb_fallocate(struct file *file, int mode, loff_t offset, loff_t len)
{
         struct inode *inode = file_inode(file);
         long ret;

         if (offset < 0 || len <= 0)
                 return -EINVAL;
         /* Return error if mode is not supported */
         if (mode & ~(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE))
                 return -EOPNOTSUPP;

         /* Punch hole must have keep size set */
         if ((mode & FALLOC_FL_PUNCH_HOLE) &&
             !(mode & FALLOC_FL_KEEP_SIZE))
                 return -EOPNOTSUPP;

         if (!(file->f_mode & FMODE_WRITE))
                 return -EBADF;

         /* It's not possible punch hole on append only file */
         if (mode & FALLOC_FL_PUNCH_HOLE && IS_APPEND(inode))
                 return -EPERM;
         if (IS_IMMUTABLE(inode))
                 return -EPERM;

         /*
          * Revalidate the write permissions, in case security policy has
          * changed since the files were opened.
          */
         //ret = security_file_permission(file, MAY_WRITE);
         //if (ret)
         //        return ret;

         if (S_ISFIFO(inode->i_mode))
                 return -ESPIPE;

         /*
          * Let individual file system decide if it supports preallocation
          * for directories or not.
          */
         if (!S_ISREG(inode->i_mode) && !S_ISDIR(inode->i_mode))
                 return -ENODEV;

            /* Check for wrap through zero too */
            if (((offset + len) > inode->i_sb->s_maxbytes) || ((offset + len) < 0))
                   return -EFBIG;

            if (!file->f_op->fallocate)
                   return -EOPNOTSUPP;

            sb_start_write(inode->i_sb);
            ret = file->f_op->fallocate(file, mode, offset, len);
            sb_end_write(inode->i_sb);
            return ret;
}
int fallocate64(int fd, int mode, loff_t offset, loff_t len)
{
         struct fd f = ftfs_fdget(fd);
         int error = -EBADF;

         if (f.file) {
                 error = do_sb_fallocate(f.file, mode, offset, len);
                 fdput(f);
         }
         return error;
}
#ifdef FTFS_DEBUG_WRITES
void ftfs_print_file_flags(struct file *file)
{
	/* check select flags */
	printk(KERN_CRIT "file flags: ");
	if (file->f_flags & O_APPEND)
		printk(KERN_CRIT "O_APPEND ");
	if (file->f_flags & O_DIRECT)
		printk(KERN_CRIT "O_DIRECT ");
	if (file->f_flags & O_DSYNC)
		printk(KERN_CRIT "O_DSYNC ");
	if (file->f_flags & O_SYNC)
		printk(KERN_CRIT "O_SYNC ");

	printk(KERN_CRIT "\n");
}

void ftfs_verify_buf(const char *buf, size_t count)
{
	int i;
	long int addr = (long int) buf;
	const char *aligned =
		(char *) (PAGE_ALIGN(addr) > addr ?
			  PAGE_ALIGN(addr-PAGE_SIZE) : PAGE_ALIGN(addr));

	printk(KERN_CRIT "aligned: %pK\n", aligned);
	printk(KERN_CRIT "buf:     %pK\n", buf);
	printk(KERN_CRIT "masked:  %pK\n", (void *)(addr & ~(PAGE_SIZE-1)));

	printk(KERN_CRIT "touching pages of buf (%ld bytes:)\n\t", count);
	for (i = 0; i < count; i += PAGE_SIZE) {
		printk("%pK", buf + i);
	}
	printk(KERN_CRIT "\n\ttouched\n");
}

void ftfs_debug_write(struct file *file, const char *buf, size_t count,
			  loff_t *pos)
{
	ftfs_print_file_flags(file);
	ftfs_verify_buf(buf, count);
}
#endif //FTFS_DEBUG_WRITES


/* exactly vfs_write, minus the userspace access check. maybe use vfs_write? */
static ssize_t ftfs_write(struct file *file, const char *buf, size_t count,
			  loff_t *pos)
{
	ssize_t ret;

	/* wkj: 4/22 figure out if we can remove this */
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
#ifdef FTFS_DEBUG_WRITES
		ftfs_debug_write(file, buf, count, pos);
#endif
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
}

ssize_t write(int fd, const void *buf, size_t count)
{
	struct fd f;
	ssize_t ret = -EBADF;

	f = ftfs_fdget(fd);

	if (f.file) {
		loff_t pos = f.file->f_pos;
#ifdef FTFS_DEBUG_IO
		printk(KERN_NOTICE "write %d [%llu, %llu)\n", fd, pos,
		       pos+count);
#endif //FTFS_DEBUG_IO
		ret = ftfs_write(f.file, buf, count, &pos);
		if (ret > 0)
			f.file->f_pos = pos;
		ftfs_fdput(f);
	}

	return ret;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, loff_t pos)
{
	struct fd f;
	ssize_t ret = -EBADF;

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "pwrite %d [%llu, %llu)\n", fd, pos, pos+count);
#endif // FTFS_DEBUG_IO

	if (pos < 0)
		return -EINVAL;

	f = ftfs_fdget(fd);
	if (f.file) {
		ret = -ESPIPE;
		if (f.file->f_mode & FMODE_PWRITE) {
			ret = ftfs_write(f.file, buf, count, &pos);
		} else {
			ftfs_error(__func__, "fmode not FMODE_PWRITE: %d",
				   f.file->f_mode);
		}
		ftfs_fdput(f);
	}

	return ret;
}

/* (p)read(64) need this to be changed if we want to avoid the user copy */
static ssize_t ftfs_read(struct file *f, char *buf, size_t count, loff_t *pos)
{
	int ret;
	mm_segment_t saved = get_fs();

	set_fs(get_ds());

	ret = vfs_read(f, buf, count, pos);

	set_fs(saved);

	return ret;
}

ssize_t pread64(int fd, void *buf, size_t count, loff_t pos)
{
	struct fd f;
	ssize_t ret = -EBADF;

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "pread %d [%llu, %llu)\n", fd, pos, pos+count);
#endif // FTFS_DEBUG_IO

	if (pos < 0)
		return -EINVAL;

	f = ftfs_fdget(fd);
	if (f.file) {
		ret = -ESPIPE;
		if (f.file->f_mode & FMODE_PREAD)
			ret = ftfs_read(f.file, buf, count, &pos);
		ftfs_fdput(f);
	}

	return ret;
}

ssize_t read(int fd, void *buf, size_t count)
{
	struct fd f;
	ssize_t ret = -EBADF;

	f = ftfs_fdget(fd);
	if (f.file) {
		loff_t pos = f.file->f_pos;
#ifdef FTFS_DEBUG_IO
		printk(KERN_NOTICE "read %d [%llu, %llu)\n", fd, pos,
		       pos+count);
#endif // FTFS_DEBUG_IO
		ret = ftfs_read(f.file, buf, count, &pos);
		if (ret >= 0)
			f.file->f_pos = pos;
		ftfs_fdput(f);
	}

	return ret;
}


/* not entirely unlike do_unlinkat.
 * only caller is unlink() */
static int ftfs_sb_unlink(const char *pathname)
{
	int err;
	struct path path;
	struct dentry *dentry;
	struct inode *inode = NULL;
	unsigned int lookup_flags = 0;
	SOUTHBOUND_VARS;

	if (pathname == NULL)
		return -EINVAL;

	/* should we lock here? */
	err = mnt_want_write(ftfs_vfs);
	if (err)
		return err;

	SOUTHBOUND_ATTACH;
retry:
	dentry = ftfs_kern_path_locked(pathname, &path);
	if (IS_ERR(dentry)) {
		err = PTR_ERR(dentry);
		goto out;
	}

	/*
	 * todo: kern_path_locked does not return -EISDIR if last path
	 * component type != LAST_NORM. It instead returns -EINVAL...
	 */

	/* Why not before? Because we want correct error value */
	//wkj TODO: understand this. is this taken care of in kern_path_locked?
	//i see it checked in link_path_walk...
	//if (nd.last.name[nd.last.len])
	//goto slashes;

	inode = dentry->d_inode;
	if (!inode)
		goto slashes;

	ihold(inode);
	//err = security_path_unlink(&path, dentry);
	//if (err)
	//	goto exit;
	err = vfs_unlink(path.dentry->d_inode, dentry);
exit:
	dput(dentry);
	mutex_unlock(&path.dentry->d_inode->i_mutex);
	if (inode)
		iput(inode); /* truncate the inode here */
	mnt_drop_write(ftfs_vfs);

	path_put(&path);
	if (retry_estale(err, lookup_flags)) {
		lookup_flags |= LOOKUP_REVAL;
		inode = NULL;
		goto retry;
	}
out:
	SOUTHBOUND_RESTORE;
	return err;

slashes:
	err = !dentry->d_inode ? -ENOENT :
	      S_ISDIR(dentry->d_inode->i_mode) ? -EISDIR : -ENOTDIR;
	goto exit;
}

int unlink(const char *pathname)
{
	return ftfs_sb_unlink(pathname);
}

static int sync_helper(int fd, int data_sync)
{
	struct fd f;
	int ret = -EBADF;

#ifdef FTFS_DEBUG_IO
	printk(KERN_NOTICE "fsync (%d)\n", fd);
#endif //FTFS_DEBUG_IO

	f = ftfs_fdget(fd);

	if (f.file) {
		ret = vfs_fsync(f.file, data_sync); // fine in NB context
		ftfs_fdput(f);
	}

	return ret;
}

int fsync(int fd)
{
  	return sync_helper(fd, 0);
}

int fdatasync(int fd)
{
  	return sync_helper(fd, 1);
}

//this is simply to test readlink
int symlink(const char * oldname, const char * newname)
{
	mm_segment_t saved;
	int ret;
	SOUTHBOUND_VARS;

	SOUTHBOUND_ATTACH;
	saved = get_fs();
	set_fs(get_ds());
	ret = ftfs_sys_symlink(oldname, newname);
	set_fs(saved);
	SOUTHBOUND_RESTORE;

	return ret;
}

// no code in ft-index check errno when readlink gets wrong
ssize_t readlink(const char *pathname, char *buf, size_t bufsiz)
{
	struct path path;
	int error;
	int empty = 0;
	unsigned int lookup_flags = LOOKUP_EMPTY;
	SOUTHBOUND_VARS;
	if (bufsiz <= 0)
		return -1;
		//return -EINVAL;

	SOUTHBOUND_ATTACH;
retry:
	error = ftfs_user_path_at_empty(AT_FDCWD, pathname, lookup_flags,
					&path, &empty);
	if (!error) {
		struct inode *inode = path.dentry->d_inode;
		error = empty ? -ENOENT : -EINVAL;
		if (inode->i_op->readlink) {
			touch_atime(&path);
			error = inode->i_op->readlink(path.dentry,buf, bufsiz);
		}
		path_put(&path);
		if (retry_estale(error, lookup_flags)) {
			lookup_flags |= LOOKUP_REVAL;
			goto retry;
		}
	}
	SOUTHBOUND_RESTORE;

	return error ? -1 : 0;
}

static inline void stream_lock_init(FILE *stream)
{
	mutex_init(&stream->lock);
}

static inline void stream_lock(FILE *stream)
{
	mutex_lock(&stream->lock);
}

static inline void stream_unlock(FILE *stream)
{
	mutex_unlock(&stream->lock);
}


loff_t lseek64(int fd, loff_t offset, int whence) {
	loff_t retval;
	struct fd f;

	f = ftfs_fdget(fd);
	if (!f.file)
		return -EBADF;
	retval = -EINVAL;

	if (whence <= SEEK_MAX)
		retval = vfs_llseek(f.file, offset, whence);

	ftfs_fdput(f);

	return retval;
}

static off_t lseek(int fd, off_t offset, int whence) {
	off_t retval;
	struct fd f;

	f = ftfs_fdget(fd);
	if (!f.file)
		return -EBADF;
	retval = -EINVAL;

	if (whence <= SEEK_MAX) {
		loff_t res = vfs_llseek(f.file, offset, whence);
		retval = res;
		if (res != (loff_t)retval)
			retval = -EOVERFLOW;
	}
	ftfs_fdput(f);

	return retval;
}

int __fseek(FILE *fp, long offset, int whence) {
	int fd;
	int ret = 0;

	/* Adjust relative offset for unread data in buffer, if any.*/
	if (whence == SEEK_CUR) offset -= fp->rend - fp->rpos;
	/* Flush write buffer, and report error on failure. */
	if (fp->wpos > fp->wbase) {
		ret = fp->write(fp, 0, 0);
		if (!fp->wpos) {
			stream_unlock(fp);
			return ret;
		}
	}
	/*  Leave writing mode */
	fp->wpos = fp->wbase = fp->wend = 0;
	fd = fp->fd;
	ret = lseek(fd, offset, whence);
	if(ret < 0) {
		stream_unlock(fp);
		ftfs_error(__func__,"fseek failed: %d", ret);
		return ret;
	}
	/* If seek succeed, file is seekable and we discard read buffer */
	fp->rpos = fp->rend = 0;
	fp->flags &= ~F_EOF;
	return 0;
}

int fseek(FILE *fp, long offset, int whence)
{
	int ret;

	stream_lock(fp);
	ret = __fseek(fp, offset, whence);
	stream_unlock(fp);

	return ret;
}

long ftell(FILE * fp) {
	int fd;
	int ret = 0;
	stream_lock(fp);

	fd = fp->fd;
	ret = lseek(fd, 0, SEEK_CUR);
	if(ret < 0) {
		stream_unlock(fp);
		ftfs_log(__func__, "ftell failed: %d",ret);
		return ret;
	}

	/*  Adjust for data in buffer. */
	ret = ret - (fp->rend - fp->rpos) + (fp->wpos - fp->wbase);
	stream_unlock(fp);
	return ret;
}

off64_t ftello64(FILE * fp) {
	int fd;
	int ret = 0;
	stream_lock(fp);

	fd = fp->fd;
	ret = lseek64(fd, 0, SEEK_CUR);
	if(ret < 0) {
		stream_unlock(fp);
		ftfs_log(__func__, "ftello64 failed: %d", ret);
		return ret;
	}
	ret -= (fp->rend - fp->rpos) + (fp->wpos - fp->wbase);
	stream_unlock(fp);
	return ret;
}

off_t ftello(FILE * fp) {
	int fd;
	int ret = 0;
	stream_lock(fp);
	fd = fp->fd;
	ret = lseek(fd, 0, SEEK_CUR);
	if(ret < 0) {
		stream_unlock(fp);
		ftfs_log(__func__,"ftello failed :%d", ret);
		return ret;
	}
	ret -= (fp->rend - fp->rpos) + (fp->wpos - fp->wbase);
	stream_unlock(fp);
	return ret;
}

static ssize_t __ftfs_stream_writebuf(FILE *f, const unsigned char *buf,
				     size_t len)
{
	int rem = len + f->wpos - f->wbase;
	ssize_t count;

	/* phase 1: writeout our buffer */
	count = write(f->fd, f->wbase, f->wpos - f->wbase);
	if (count == (f->wpos - f->wbase)) {
		f->wend = f->buf + f->bufsize;
		f->wpos = f->wbase = f->buf;
	}
	if (count < 0) {
		f->wpos = f->wbase = f->wend = 0;
		f->flags |= F_ERR;
		return len - rem;
	}
	rem -= count;

	if (!rem)
		return len;

	/* phase two: writeout the passed in buffer */
	for (;;) {
		count = write(f->fd, buf, rem);
		if (count == rem)
			return len;

		if (count < 0) {
			f->flags |= F_ERR;
			return len - rem;
		}
		rem -= count;
		buf += count;
	}
}

static ssize_t ftfs_readv(struct file *file, const struct iovec *vec,
			  unsigned long vlen, loff_t *pos)
{
	int ret;
	mm_segment_t saved = get_fs();
	set_fs(get_ds());
	ret = vfs_readv(file, vec, vlen, pos);
	set_fs(saved);
	return ret;
}

ssize_t readv(int fd, const struct iovec *vec, unsigned int vlen)
{
	struct fd f;
	ssize_t ret = -EBADF;

	f = ftfs_fdget(fd);
	if (f.file) {
		loff_t pos = f.file->f_pos;
		ret = ftfs_readv(f.file, vec, vlen, &pos);
		if (ret >= 0)
			f.file->f_pos = pos;
		ftfs_fdput(f);
	}

	if (ret > 0)
		add_rchar(current, ret);
	inc_syscr(current);
	return ret;
}

/* __stdio_read from musl/tree/src/stdio/__stdio_read.c */
static ssize_t __ftfs_stream_readbuf(FILE *f, unsigned char *buf, size_t len)
{
	struct iovec iov[2] = {
		{ .iov_base = buf, .iov_len = len - !!f->bufsize },
		{ .iov_base = f->buf, .iov_len = f->bufsize }
	};
	ssize_t cnt;

	cnt = readv(f->fd, iov, 2);
	if (cnt <= 0) {
		f->flags |= F_EOF ^ ((F_ERR^F_EOF) & cnt);
		f->rpos = f->rend = 0;
		return cnt;
	}

	if (cnt <= iov[0].iov_len)
		return cnt;

	cnt -= iov[0].iov_len;
	f->rpos = f->buf;
	f->rend = f->buf + cnt; //-1 or remove = from <= in frpos < frend in _ftfs_getc
	if (f->bufsize)
		buf[len-1] = *f->rpos++;
	return len;
}

int fileno(FILE *stream)
{
	return stream->fd;
}

/**
 * wkj: would all of the FILE * operations be faster if we stored a
 * struct file * instead of an fd?
 */
FILE *ftfs_fdopen(int fd)
{
	FILE *file = (FILE *)kzalloc(sizeof(FILE) + FSTREAM_BUFSZ, GFP_KERNEL);
	if (!file) {
		ftfs_error(__func__, "kzalloc failed to allocate a FILE");
		return NULL;
	}

	file->fd = fd;
	stream_lock_init(file);
	file->buf = (unsigned char *)file + sizeof *file;
	file->bufsize = FSTREAM_BUFSZ;
	file->read = __ftfs_stream_readbuf;
	file->write = __ftfs_stream_writebuf;
	return file;
}

FILE *fopen64(const char * path, const char * mode)
{
	int fd;
	FILE *file;

	//ft-index only uses "wb", "rb", "r", "r+b" flags
	if(!strcmp(mode,"wb") || !strcmp(mode,"w"))
		fd = open64(path, O_WRONLY|O_TRUNC|O_CREAT, DEFAULT_PERMS);
	else if(!strcmp(mode,"rb")||!strcmp(mode,"r"))
		fd = open64(path, O_RDONLY, DEFAULT_PERMS);
	else if (!strcmp(mode, "r+b"))
		fd = open64(path, O_RDWR, DEFAULT_PERMS);
	else {
		ftfs_error(__func__, "fopen is called with wrong flags!!");
		BUG();
	}

	if (fd < 0)
		return NULL;

	/* internally sets errno if necessary */
	file = ftfs_fdopen(fd);
	if (!file)
		close(fd);
	return file;
}

int fclose(FILE * stream) {
	int ret = 0;
	int fd;

	stream_lock(stream);

	/*  If writing, flush output */
	if (stream->wpos > stream->wbase) {
		stream->write(stream, 0, 0);
		if (!stream->wpos) {
			stream_unlock(stream);
			return EOF;
		}
	}
	/*  If reading, sync position, per POSIX */
	if (stream->rpos < stream->rend)
		__fseek(stream, stream->rpos-stream->rend, SEEK_CUR);

	/*  Clear read and write modes */
	stream->wpos = stream->wbase = stream->wend = 0;
	stream->rpos = stream->rend = 0;

	fd = stream->fd;
	stream->fd = -1;
	ret = close(fd);

	stream_unlock(stream);

	kfree(stream);

	return ret;
}


/* changed fs/locks.c sys_flock for this to work --- added exported
 * function do_flock() */
/*
int flock(int fd, int cmd)
{
	struct fd f = ftfs_fdget(fd);
	int error;
	error = -EBADF;
	if (!f.file)
		goto out;

	error = ftfs_do_flock(f, (unsigned int)cmd);

	ftfs_fdput(f);
 out:
	return return_errno_nonzero(error);

	return 0;
}
*/

/* mostly copied from musl */

int __toread(FILE *f)
{
	/* wkj: writeback your buffer updates, if any */
	if (f->wpos > f->buf)
		f->write(f, 0, 0);

	f->wpos = f->wbase = f->wend = 0;
	if (f->flags & (F_EOF|F_NORD)) {
		if (f->flags & F_NORD)
			f->flags |= F_ERR;
		return EOF;
	}

	f->rpos = f->rend = f->buf;
	return 0;
}

int __towrite(FILE *f)
{
	if (f->flags & (F_NOWR)) {
		f->flags |= F_ERR;
		return EOF;
	}

	/* Clear read buffer (easier than summoning nasal demons) */
	f->rpos = f->rend = 0;

	/* Activate write through the buffer. */
	f->wpos = f->wbase = f->buf;
	f->wend = f->buf + f->bufsize;

	return 0;
}

int __uflow(FILE *f)
{
	unsigned char c;
	if ((f->rend || !__toread(f)) && f->read(f, &c, 1)==1) return c;
	return EOF;
}


/* @pre: f is locked */
static int __ftfs_getc(FILE *f)
{
	/* NULL ptr deref */
	if (f->rpos && (f->rpos < f->rend))
		return (int)*f->rpos++;
	else
		return __uflow (f);
}

int fgetc(FILE *f)
{
	int c;
	stream_lock(f);
	c = __ftfs_getc(f);
	stream_unlock(f);
	return c;
}


int feof(FILE *f)
{
	int ret;
	stream_lock(f);
	ret = !!(f->flags & F_EOF);
	stream_unlock(f);
	return ret;
}

extern void* realloc(void *ptr, size_t size);
ssize_t getdelim(char **s, size_t *n, int delim, FILE *f)
{
	char *tmp;
	unsigned char *z;
	size_t k;
	size_t i=0;
	int c;

	if (!n || !s)
		return -EINVAL;

	if (!*s)
		*n=0;

	stream_lock(f);

	for (;;) {
		z = memchr(f->rpos, delim, f->rend - f->rpos);
		k = z ? z - f->rpos + 1 : f->rend - f->rpos;
		if (i+k >= *n) {
			if (k >= SIZE_MAX/2-i)
				goto oom;
			*n = i+k+2;
			if (*n < SIZE_MAX/4)
				*n *= 2;
			tmp = realloc(*s, *n);
			if (!tmp) {
				*n = i+k+2;
				tmp = realloc(*s, *n);
				if (!tmp)
					goto oom;
			}
			*s = tmp;
		}
		memcpy(*s+i, f->rpos, k);
		f->rpos += k;
		i += k;
		if (z)
			break;
		if ((c = __ftfs_getc(f)) == EOF) {
			if (!i || !feof(f)) {
				stream_unlock(f);
				return EOF;
			}
			break;
		}
		if (((*s)[i++] = c) == delim)
			break;
	}
	(*s)[i] = 0;

	stream_unlock(f);

	return i;
oom:
	stream_unlock(f);
	return -ENOMEM;
}

ssize_t getline(char **s, size_t *n, FILE *f)
{
	return getdelim(s, n, '\n', f);
}

size_t fread(void *destv, size_t size, size_t nmemb, FILE *f)
{
	unsigned char *dest = destv;
	size_t len = size*nmemb;
	size_t l = len;
	size_t k;

	/* Never touch the file if length is zero.. */
	if (!l)
		return 0;

	stream_lock(f);

	if (f->rend - f->rpos > 0) {
		/* First exhaust the buffer. */
		k = min((size_t)(f->rend - f->rpos), l);
		memcpy(dest, f->rpos, k);
		f->rpos += k;
		dest += k;
		l -= k;
	}

	/* Read the remainder directly */
	for (; l; l-=k, dest+=k) {
		k = __toread(f) ? 0 : f->read(f, dest, l);
		if (k+1<=1) {
			stream_unlock(f);
			return (len-l)/size;
		}
	}

	stream_unlock(f);
	return nmemb;
}

size_t __fwritex(const unsigned char *s, size_t l, FILE *f)
{
	size_t i=0;

	if (!f->wend && __towrite(f))
		return 0;

	if (l > f->wend - f->wpos)
		return f->write(f, s, l);

	if (f->lbf >= 0) {
		/* Match /^(.*\n|)/ */
		for (i=l; i && s[i-1] != '\n'; i--);
		if (i) {
			if (f->write(f, s, i) < i)
				return i;
			s += i;
			l -= i;
		}
	}

	memcpy(f->wpos, s, l);
	f->wpos += l;
	return l+i;
}

size_t fwrite(const void *src, size_t size, size_t nmemb, FILE *f)
{
	size_t k, l = size*nmemb;
	//work around
	if(f->fd == 1) {
		fprintf(stdout, (char *)src);
		return l;
	} else if(f->fd == 2) {
		fprintf(stderr, (char *)src);
		return l;
	}

	if (!l) return l;
	stream_lock(f);
	k = __fwritex(src, l, f);
	stream_unlock(f);
	return k==l ? nmemb : k/size;
}

//purely for testing
long sync(void)
{
	return ftfs_sys_sync();
}


int remove(const char *path)
{
	int r = unlink(path);
	return (r == -EISDIR) ? rmdir(path) : r;
}

// quick implementation of link
int link(const char *oldpath, const char *newpath)
{
	int err;
	SOUTHBOUND_VARS;

	SOUTHBOUND_ATTACH;
	err = ftfs_sys_link(oldpath, newpath);
	SOUTHBOUND_RESTORE;

	return err;
}


/* wkj: is this ever used? grepping found nothing in ft source.
 *
 *  answer: only used in tests... do not waste time optimizing this.
 */
int fcopy(const char * src, const char * dest) {
	//int saved_error;
	int fd_src, fd_dest;
	char buf[512];
	ssize_t nread;

	fd_src = open64(src, O_RDONLY, 0755);
	if (fd_src < 0) {
		ftfs_log(__func__,"src %s failed to open", src);
		return fd_src;
	}

	fd_dest = open64(dest, O_CREAT|O_WRONLY|O_TRUNC, 0755);
	if (fd_dest < 0) {
		ftfs_log(__func__,"dest %s failed to open", dest);
		return fd_dest;
	}

	while (nread = read(fd_src, buf, sizeof buf), nread > 0) {
		char * out = buf;
		int nwritten = 0;
		do {
			nwritten = write(fd_dest, out, nread);
			if (nwritten >= 0) {
				nread -= nwritten;
				out += nwritten;
			}
		} while (nread > 0);
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


unsigned long dup2(int oldfd, unsigned int newfd)
{
	//return ftfs_dup2(oldfd, newfd);
	return 0;
}
