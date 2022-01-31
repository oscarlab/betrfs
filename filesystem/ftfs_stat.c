/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
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

// fs/stat.c vfs_statat
static int ftfs_vfs_fstat(unsigned int fd, struct kstat *stat)
{
    struct fd f = ftfs_fdget(fd);
    int error = -EBADF;

    if (f.file) {
        error = vfs_getattr(&f.file->f_path, stat, STATX_ALL, AT_STATX_SYNC_AS_STAT);
        ftfs_fdput(f);
    }
    return error;
}

static inline void __cp_stat(struct kstat *stat, struct stat *statbuf)
{
    statbuf->st_dev = stat->dev;
    statbuf->st_ino = stat->ino;
    statbuf->st_nlink = stat->nlink;
    statbuf->st_mode = stat->mode;
    statbuf->st_uid = __kuid_val(stat->uid);
    statbuf->st_gid = __kgid_val(stat->gid);

    statbuf->st_rdev = stat->rdev;
    statbuf->st_size = stat->size;
    statbuf->st_blksize = stat->blksize;
    statbuf->st_blocks = stat->blocks;

    statbuf->st_atime = stat->atime.tv_sec;
    statbuf->st_atime_nsec = stat->atime.tv_nsec;
    statbuf->st_mtime = stat->mtime.tv_sec;
    statbuf->st_mtime_nsec = stat->mtime.tv_nsec;
    statbuf->st_ctime = stat->ctime.tv_sec;
    statbuf->st_ctime_nsec = stat->ctime.tv_nsec;

}

// fs/stat.c SYS_stat
static int ftfs_stat(const char *name, struct stat *statbuf)
{
    struct kstat stat;
    struct path path;
    int error;
    SOUTHBOUND_VARS;

    if (!name) {
        return -EINVAL;
    }

    SOUTHBOUND_ATTACH;
    error = kern_path(name, 0, &path);
    SOUTHBOUND_RESTORE;

    if (error) {
        return error;
    }

    error = vfs_getattr(&path, &stat,  STATX_ALL, AT_STATX_SYNC_AS_STAT); // NB context fine
    path_put(&path);

    if (!error) {
        __cp_stat(&stat, statbuf);
    }

    return error;
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

// fs/stat.c SYS_fstat
int fstat(int fd, struct stat *statbuf)
{
    struct kstat stat;
    int error;

    error = ftfs_vfs_fstat(fd, &stat);
    if (!error) {
        __cp_stat(&stat, statbuf);
    }

    return error;
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

// fs/statfs.c do_statfs_native
static inline void __do_statfs_native(struct kstatfs *st, struct statfs *buf)
{
    buf->f_type = st->f_type;
    buf->f_bsize = st->f_bsize;
    buf->f_blocks = st->f_blocks;
    buf->f_bfree = st->f_bfree;
    buf->f_bavail = st->f_bavail;
    buf->f_files = st->f_files;
    buf->f_ffree = st->f_ffree;
    buf->f_fsid = st->f_fsid;
    buf->f_namelen = st->f_namelen;
    buf->f_frsize = st->f_frsize;
    buf->f_flags = st->f_flags;
}

// fs/statfs.c SYS_statfs
static int ftfs_statfs(const char *name, struct statfs *buf)
{
    struct kstatfs stat;
    struct path path;
    int error;
    SOUTHBOUND_VARS;

    if (!name) {
        return -EINVAL;
    }

    SOUTHBOUND_ATTACH;
    error = kern_path(name, 0, &path);
    SOUTHBOUND_RESTORE;

    if (error) {
        return error;
    }

    error = vfs_statfs(&path, &stat); // wkj: ok in NB context
    path_put(&path);

    if (!error) {
        __do_statfs_native(&stat, buf);
    }

    return error;
}

int statfs(const char *name, struct statfs *buf)
{
    return ftfs_statfs(name, buf);
}

int statvfs64 (const char *name, struct statfs *buf)
{
    return ftfs_statfs(name, buf);
}

// fs/statfs.c SYS_fstatfs
int fstatfs(int fd, struct statfs *buf)
{
    struct kstatfs stat;
    struct fd f;
    int error;

    f = ftfs_fdget(fd);
    if (!f.file)
        return -EBADF;

    error = vfs_statfs(&f.file->f_path, &stat);
    ftfs_fdput(f);

    if (!error) {
        __do_statfs_native(&stat, buf);
    }

    return error;
}

