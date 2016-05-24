#include <linux/kernel.h>
#include <linux/time.h>
#include <linux/err.h>
#include <linux/syscalls.h>
#include <linux/file.h>
#include <linux/path.h>
#include <linux/dirent.h>
#include <asm/segment.h>
#include <asm/uaccess.h>
#include <linux/mount.h>
#include <linux/statfs.h>
#include <linux/dcache.h>
#include <linux/fs.h>
#include <linux/fs_struct.h>
#include <linux/namei.h>
#include <linux/fcntl.h>
#include <linux/version.h>
#include "ftfs_files.h"
#include "ftfs_southbound.h"
#include "ftfs.h"
#include "ftfs_error.h"

static int ftfs_vfs_fstat(unsigned int fd, struct kstat *stat)
{
	struct fd f = ftfs_fdget(fd);
	int error = -EBADF;

	if (f.file) {
		error = vfs_getattr(&f.file->f_path, stat);
		ftfs_fdput(f);
	}
	return error;
}

static int ftfs_stat(const char *name, struct stat *statbuf)
{
	struct kstat stat;
	struct path  path;
	int error = -EINVAL;
	SOUTHBOUND_VARS;

	if (!name) {
		ftfs_log(__func__, "file name is NULL");
		return error;
	}

	SOUTHBOUND_ATTACH;
	error = kern_path(name, 0, &path);
	SOUTHBOUND_RESTORE;

	if (error) {
		ftfs_log(__func__, "kern_path failed:%d, pathname:%s",
			error,name);
		return error;
	}

	error = vfs_getattr(&path, &stat); // NB context fine
	path_put(&path);
	if (error) {
		ftfs_error(__func__, "vfs_getattr failed:%d, pathname:%s",
			error, name);
		return error;
	}

	statbuf->st_dev = stat.dev;
	statbuf->st_ino = stat.ino;
	statbuf->st_nlink = stat.nlink;
	statbuf->st_mode = stat.mode;
#if LINUX_VERSION_CODE == KERNEL_VERSION(3, 11, 10)

#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	statbuf->st_uid = stat.uid.val;
	statbuf->st_gid = stat.gid.val;
#else
	statbuf->st_uid = stat.uid;
	statbuf->st_gid = stat.gid;
#endif
#else
	statbuf->st_uid = stat.uid;
	statbuf->st_gid = stat.gid;
#endif

	statbuf->st_rdev = stat.rdev;
	statbuf->st_size = stat.size;
	statbuf->st_blksize = stat.blksize;
	statbuf->st_blocks = stat.blocks;

	statbuf->st_atime = stat.atime.tv_sec;
	statbuf->st_atime_nsec = stat.atime.tv_nsec;
	statbuf->st_mtime = stat.mtime.tv_sec;
	statbuf->st_mtime_nsec = stat.mtime.tv_nsec;
	statbuf->st_ctime = stat.ctime.tv_sec;
	statbuf->st_ctime_nsec = stat.ctime.tv_nsec;

	return 0;
}

int stat(const char *filename, struct stat *statbuf)
{
	return ftfs_stat(filename, statbuf);
}

/* wkj: this will only work on x86_64. however, this only shows up if
 * we compile cmake wtih Debug */
int stat64(const char *filename, struct stat *statbuf)
{
#if BITS_PER_LONG == 32
	BUG();
#endif
	return ftfs_stat(filename, statbuf);
}

int fstat(int fd, struct stat *statbuf)
{
	struct kstat stat;
	int error = 0;

	error = ftfs_vfs_fstat(fd, &stat);
	if (error) {
		ftfs_error(__func__, "vfs_fstat(fd:%d, ...): %d\n", fd, error);
		return error;
	}

	statbuf->st_dev = stat.dev;
	statbuf->st_ino = stat.ino;
	statbuf->st_nlink = stat.nlink;
	statbuf->st_mode = stat.mode;
#if LINUX_VERSION_CODE == KERNEL_VERSION(3, 11, 10)

#ifdef CONFIG_UIDGID_STRICT_TYPE_CHECKS
	statbuf->st_uid = stat.uid.val;
	statbuf->st_gid = stat.gid.val;
#else
	statbuf->st_uid = stat.uid;
	statbuf->st_gid = stat.gid;
#endif
#else
	statbuf->st_uid = stat.uid;
	statbuf->st_gid = stat.gid;
#endif

	statbuf->st_rdev = stat.rdev;
	statbuf->st_size = stat.size;
	statbuf->st_blksize = stat.blksize;
	statbuf->st_blocks = stat.blocks;
	statbuf->st_atime = stat.atime.tv_sec;
	statbuf->st_atime_nsec = stat.atime.tv_nsec;
	statbuf->st_mtime = stat.mtime.tv_sec;
	statbuf->st_mtime_nsec = stat.mtime.tv_nsec;
	statbuf->st_ctime = stat.ctime.tv_sec;
	statbuf->st_ctime_nsec = stat.ctime.tv_nsec;

	return 0;
}

/* wkj: this will only work on x86_64. however, this only shows up if
 * we compile cmake wtih Debug */
int fstat64(int fd, struct stat *statbuf)
{
#if BITS_PER_LONG == 32
	BUG();
#endif
	return fstat(fd, statbuf);
}


int __fxstat64(int ver, int fildes, struct stat * stat_buf)
{
	ftfs_log(__func__, "ver: %d");
	FTFS_DEBUG_ON(ver == 3);

	return fstat(fildes, stat_buf);
}


int __xstat64(int ver, const char *filename, struct stat *statbuf)
{
	ftfs_log(__func__, "ver: %d");
	FTFS_DEBUG_ON(ver == 3);

	return stat(filename, statbuf);
}

static int ftfs_statfs(const char *name, struct statfs *buf)
{
	struct kstatfs stat;
	struct path path;
	int error = -EINVAL;
	SOUTHBOUND_VARS;

	if (!name) {
		ftfs_error(__func__, "file name is NULL");
		return error;
	}

	SOUTHBOUND_ATTACH;
	error = kern_path(name, 0, &path);
	SOUTHBOUND_RESTORE;

	if (error) {
		ftfs_log(__func__, "kern_path failed, error:%d,pathname:%s",
			name);
		return error;
	}

	error = vfs_statfs(&path, &stat); // wkj: ok in NB context
	path_put(&path);
	if (error) {
		ftfs_error(__func__, "vfs_statfs: error=%d, pathname=:%s",
			name);
		return error;
	}


	buf->f_type = stat.f_type;
	buf->f_bsize = stat.f_bsize;
	buf->f_blocks = stat.f_blocks;
	buf->f_bfree = stat.f_bfree;
	buf->f_bavail = stat.f_bavail;
	buf->f_files = stat.f_files;
	buf->f_ffree = stat.f_ffree;
	buf->f_fsid = stat.f_fsid;
	buf->f_namelen = stat.f_namelen;
	buf->f_frsize = stat.f_frsize;
	buf->f_flags = stat.f_flags;

	//memset(buf->f_spare, 0, sizeof(buf->f_spare));

	return 0;
}

int statfs(const char *name, struct statfs *buf)
{
	return  ftfs_statfs(name, buf);
}

int fstatfs(int fd, struct statfs *buf)
{
	struct kstatfs stat;
	struct fd f;
	int error = 0;

	f = ftfs_fdget(fd);
	if (!f.file)
		return -EBADF;

	error = vfs_statfs(&f.file->f_path, &stat);
	ftfs_fdput(f);
	if (error) {
		ftfs_log(__func__, "vfs_statfs: err=%d", error);
		return error;
	}

	buf->f_type = stat.f_type;
	buf->f_bsize = stat.f_bsize;
	buf->f_blocks = stat.f_blocks;
	buf->f_bfree = stat.f_bfree;
	buf->f_bavail = stat.f_bavail;
	buf->f_files = stat.f_files;
	buf->f_ffree = stat.f_ffree;
	buf->f_fsid = stat.f_fsid;
	buf->f_namelen = stat.f_namelen;
	buf->f_frsize = stat.f_frsize;
	buf->f_flags = stat.f_flags;

	//memset(buf->f_spare, 0, sizeof(buf->f_spare));

	return 0;
}

int statvfs64 (const char *name, struct statfs *buf)
{
	return  ftfs_statfs(name, buf);
}
