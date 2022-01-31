/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound file system.
 *
 * It implements some simple directory operations for the ft code.
 */

#include <linux/kernel.h>
#include <linux/syscalls.h>
#include <linux/bug.h>
#include <linux/file.h>
#include <linux/dirent.h>
#include <asm/segment.h>
#include <asm/uaccess.h>
#include <linux/mount.h>
#include <linux/fsnotify.h>
#include <linux/statfs.h>
#include <linux/dcache.h>
#include <linux/fs.h>
#include <linux/fs_struct.h>
#include <linux/namei.h>
#include <linux/security.h>
#include <linux/kallsyms.h>
#include <linux/fcntl.h>
#include "ftfs_southbound.h"
#include "sb_files.h"
#include "ftfs.h"

char *basename(char *name)
{
	char *beg, *end;
	beg = name;
	end = name;

	while (*beg == '/')
		beg++;
	if (!*beg)
		goto exit_root;

	while (*end++)
		;
	end--;
	end--;

	while (*end == '/')
		end--;
	end++;
	if (*end == '/')
		*end = '\0';
	end--;
	while (*end != '/' && end > beg)
		end--;

	if (end == beg)
		return beg;
	else
		return ++end;
exit_root:

	return --beg;
}

/*
 * the only difference from open() is to check this is REALLY a dir
 */
int opendir_helper(const char *name, int flags)
{
	int fd;
	struct file *f;
	struct inode * inode;
	SOUTHBOUND_VARS;

	//flags |= __O_KERNFS;

	fd = sb_get_unused_fd_flags(flags);
	if(fd < 0)
		return fd;

	SOUTHBOUND_ATTACH;
	f = filp_open(name, flags, 0755);
	SOUTHBOUND_RESTORE;

	if (IS_ERR(f)) {
		sb_put_unused_fd(fd);
		ftfs_error(__func__, "filp_open (%s) failed:%d", name,
			PTR_ERR(f));
		return PTR_ERR(f);
	}

	inode = f->f_inode;
	if(S_ISDIR(inode->i_mode)) {
		fsnotify_open(f);
		sb_fd_install(fd, f);
		return fd;
	}

	sb_put_unused_fd(fd);
	ftfs_error(__func__, "%s is not a dir", name);
	sb_filp_close(f);

	return -ENOTDIR;
}

DIR *opendir_helper_fd(int fd)
{
	DIR *dirp;
	dirp = (DIR *)kzalloc(sizeof(DIR), GFP_KERNEL);
	if (!dirp)
		return NULL;

	dirp->fd = fd;
	dirp->buf_pos = dirp->buf_end = 0;
	return dirp;
}

DIR *opendir(const char *name)
{
	int fd;
	DIR *dir;

	fd = opendir_helper(name, O_RDONLY|O_NDELAY|O_DIRECTORY|O_LARGEFILE);
	if (fd < 0)
		return NULL;

	dir = opendir_helper_fd(fd);
	if (!dir)
		close(fd);
	return dir;
}

DIR *fdopendir(int fd)
{
	return opendir_helper_fd(fd);
}



int closedir(DIR * dirp)
{
	int fd;

	if(!dirp || IS_ERR(dirp))
		return -EINVAL;

	fd = dirp->fd;
	kfree(dirp);
	return close(fd);
}

int dirfd(DIR * dirp)
{
	if(!dirp || IS_ERR(dirp))
		return -EINVAL;
	return dirp->fd;
}
