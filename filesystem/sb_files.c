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
#include "sb_files.h"
#include "ftfs_southbound.h"
#include "ftfs.h"

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

int resolve_sb_files_symbols(void)
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
	if (fd < files->next_fd)
		files->next_fd = fd;
	spin_unlock(&files->file_lock);
}

void sb_fd_install(unsigned int fd, struct file *file)
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

/*
similar to do_sys_open but with southbound context switch
 */
int open(const char *pathname, int flags, umode_t mode)
{
	int fd;
	struct file *f;
	SOUTHBOUND_VARS;

	flags |= __O_KERNFS;

	fd = sb_get_unused_fd_flags(flags);
	if(fd < 0)
		return fd;

	SOUTHBOUND_ATTACH;
	f = filp_open(pathname, flags, mode);
	SOUTHBOUND_RESTORE;

	if (IS_ERR(f)) {
		sb_put_unused_fd(fd);
#ifdef FTFS_DEBUG_IO
		ftfs_error(__func__, "filp_open (%s) failed:%d",pathname,
			   PTR_ERR(f));
#endif
		return PTR_ERR(f);
	} else {
		fsnotify_open(f);
		sb_fd_install(fd, f);
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

/* similar to write syscall in read_write.c */
ssize_t write(int fd, const void *buf, size_t count)
{
	struct fd f;
	ssize_t ret = -EBADF;

	f = sb_fdget(fd);

	if (f.file) {
		loff_t pos = f.file->f_pos;
#ifdef FTFS_DEBUG_IO
		printk(KERN_NOTICE "write %d [%llu, %llu)\n", fd, pos,
		       pos+count);
#endif //FTFS_DEBUG_IO
		ret = ftfs_write(f.file, buf, count, &pos);
		if (ret > 0)
			f.file->f_pos = pos;
		sb_fdput(f);
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
			ftfs_error(__func__, "fmode not FMODE_PWRITE: %d",
				   f.file->f_mode);
		}
		sb_fdput(f);
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

	f = sb_fdget(fd);
	if (f.file) {
		loff_t pos = f.file->f_pos;
#ifdef FTFS_DEBUG_IO
		printk(KERN_NOTICE "read %d [%llu, %llu)\n", fd, pos,
		       pos+count);
#endif // FTFS_DEBUG_IO
		ret = ftfs_read(f.file, buf, count, &pos);
		if (ret >= 0)
			f.file->f_pos = pos;
		sb_fdput(f);
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

	f = sb_fdget(fd);
	if (!f.file)
		return -EBADF;
	retval = -EINVAL;

	if (whence <= SEEK_MAX)
		retval = vfs_llseek(f.file, offset, whence);

	sb_fdput(f);

	return retval;
}

static off_t lseek(int fd, off_t offset, int whence) {
	off_t retval;
	struct fd f;

	f = sb_fdget(fd);
	if (!f.file)
		return -EBADF;
	retval = -EINVAL;

	if (whence <= SEEK_MAX) {
		loff_t res = vfs_llseek(f.file, offset, whence);
		retval = res;
		if (res != (loff_t)retval)
			retval = -EOVERFLOW;
	}
	sb_fdput(f);

	return retval;
}

int __fseek(FILE *fp, long offset, int whence) {
	int fd;
	int ret = 0;
	off_t lseek_ret;

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
	lseek_ret = lseek(fd, offset, whence);
	if(lseek_ret < 0) {
		ftfs_error(__func__,"fseek failed: %d", lseek_ret);
		return (int) lseek_ret;
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

/* readv syscall from read_write.c */
ssize_t readv(int fd, const struct iovec *vec, unsigned int vlen)
{
	struct fd f;
	ssize_t ret = -EBADF;

	f = sb_fdget(fd);
	if (f.file) {
		loff_t pos = f.file->f_pos;
		ret = ftfs_readv(f.file, vec, vlen, &pos);
		if (ret >= 0)
			f.file->f_pos = pos;
		sb_fdput(f);
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
		fd = open64(path, O_WRONLY, DEFAULT_PERMS);
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
	struct fd f = sb_fdget(fd);
	int error;
	error = -EBADF;
	if (!f.file)
		goto out;

	error = ftfs_do_flock(f, (unsigned int)cmd);

	sb_fdput(f);
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
