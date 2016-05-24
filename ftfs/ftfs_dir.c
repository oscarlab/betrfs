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
#include "ftfs_files.h"
#include "ftfs.h"
#include "ftfs_error.h"

struct getdents_callback64 {
	struct dir_context ctx;
	struct linux_dirent64 * current_dir;
	struct linux_dirent64 * previous;
	int count;
	int error;
};

//#ifdef CONFIG_SECURITY
//typedef int (* security_path_rmdir_t)(struct path *dir, struct dentry *dentry);
//DECLARE_SYMBOL_FTFS(security_path_rmdir);
//#else
//static inline int ftfs_security_path_rmdir(struct path *dir,
//					   struct dentry *dentry){ return 0; }
//#endif

int resolve_ftfs_dir_symbols(void)
{
//#ifdef CONFIG_SECURITY
//	LOOKUP_SYMBOL_FTFS(security_path_rmdir);
//#endif
	return 0;
}

static int filldir64(void * __buf, const char * name,
		     int namlen, loff_t offset,
		     u64 ino, unsigned int d_type)
{
	struct linux_dirent64 *dirent;
	struct getdents_callback64 *buf = (struct getdents_callback64 *) __buf;
	int reclen = ALIGN(offsetof(struct linux_dirent64, d_name) + namlen + 1,
		sizeof(u64));

	buf->error = -EINVAL;	/* only used if we fail.. */
	if (reclen > buf->count)
		return -EINVAL;
	dirent = buf->previous;
	if (dirent)
		dirent->d_off = offset;

	dirent = buf->current_dir;
	dirent->d_ino = ino;
	dirent->d_off = 0;
	dirent->d_reclen = reclen;
	dirent->d_type = d_type;
	memcpy(dirent->d_name, name, namlen);
	*(dirent->d_name + namlen) = 0;
	buf->previous = dirent;
	dirent = (void *)dirent + reclen;
	buf->current_dir = dirent;
	buf->count -= reclen;
	return 0;
}


int getdents64(unsigned int fd, struct linux_dirent64 *dirent,
	       unsigned int count)
{
	struct fd f;
	struct linux_dirent64 *lastdirent;
	struct getdents_callback64 buf = {
		.ctx.actor = filldir64,
		.count = count,
		.current_dir = dirent
	};
	int error;


	f = ftfs_fdget(fd);
	if (!f.file)
		return -EBADF;

	error = iterate_dir(f.file, &buf.ctx);
	if (error >= 0)
		error = buf.error;
	lastdirent = buf.previous;
	if (lastdirent) {
		typeof(lastdirent->d_off) d_off = buf.ctx.pos;
		lastdirent->d_off = d_off;
		error = count - buf.count;
	}
	ftfs_fdput(f);

	return error;
}

int mkdir(const char *pathname, umode_t mode)
{
	struct dentry *dentry;
	int error;
	struct path path;
	unsigned int lookup_flags = LOOKUP_DIRECTORY;
	SOUTHBOUND_VARS;

	SOUTHBOUND_ATTACH;
retry:
	dentry = kern_path_create(AT_FDCWD, pathname, &path, lookup_flags);
	if (IS_ERR(dentry)) {
		SOUTHBOUND_RESTORE;
		return PTR_ERR(dentry);
	}

	/* wkj: we are in the southbound context, so ignore permissions? */
	//if (!IS_POSIXACL(path.dentry->d_inode))
	//mode &= ~current_umask();

	//error = security_path_mkdir(&path, dentry, mode);
	//if (!error)
	error = vfs_mkdir(path.dentry->d_inode, dentry, mode);

	done_path_create(&path, dentry);
	if (retry_estale(error, lookup_flags)) {
		lookup_flags |= LOOKUP_REVAL;
		goto retry;
	}
	//if(error) {
	//	ftfs_error(__func__, "mkdir(%s,...): %d", error, pathname);
	//}

	SOUTHBOUND_RESTORE;

	return error;
}
#if 0
char *getcwd(char *buf, int buflen)
{
	char *res;
	struct path pwd;

	ftfs_log(__func__, "getcwd probably wrong");
	ftfs_log(__func__, "getcwd will always return root dir since chdir was elimited");
	/* the problem with this approach is that pwd is not tracked
	 * accross the lifetime of our thread since we are only
	 * southbound swapping last minute... only used in logger,
	 * logcursor, and recover, so can we just rely on
	 * pthread_create to have swapped? */

	//pwd = current->fs->pwd;
	pwd = ftfs_fs->pwd; /* wkj: potentially racey? we do not hold
			     * ftfs_southbound_loc, but ftfs_fs only
			     * written to on module load/unload, so I
			     * think this is safe */

	path_get(&pwd);

	res = d_path(&pwd, buf, buflen);
	if(IS_ERR(res)) {
	    ftfs_set_errno(PTR_ERR(res));
	    return NULL;
	}

	path_put(&pwd);

	return res;
}
#endif

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

int rmdir(const char *pathname)
{
	int res;
	unsigned int lookup_flags = 0;
	struct path path;
	struct dentry *d;
	struct dentry *dchild;
	char *base, *name;
	int len;

	SOUTHBOUND_VARS;

	len = strlen(pathname) + 1;
	name = (char *)kmalloc(len, GFP_KERNEL);
	if (!name)
		return -ENOMEM;
	memcpy(name, pathname, len);
	base = basename(name);

	SOUTHBOUND_ATTACH;
retry:

	res = kern_path(pathname, LOOKUP_PARENT, &path);
	if (res) {
		ftfs_error(__func__, "path lookup failed");
		return res;
	}

	d = path.dentry;
	res = mnt_want_write(path.mnt);
	if (res)
		goto exit1;

	mutex_lock_nested(&path.dentry->d_inode->i_mutex, I_MUTEX_PARENT);

	dchild = lookup_one_len(base, d, strlen(base));
	res = PTR_ERR(dchild);
	if (IS_ERR(dchild))
		goto exit2;

	if (!dchild->d_inode) {
		res = -ENOENT;
		goto exit3;
	}
	//res = ftfs_security_path_rmdir(&path, dchild);
	//if(res)
	//	goto exit3;
	res = vfs_rmdir(path.dentry->d_inode, dchild);

exit3:
	dput(dchild);
exit2:
	mutex_unlock(&path.dentry->d_inode->i_mutex);
	mnt_drop_write(path.mnt);

exit1:
	path_put(&path);
	if (retry_estale(res, lookup_flags)) {
		lookup_flags |= LOOKUP_REVAL;
		goto retry;
	}

	kfree(name);
	SOUTHBOUND_RESTORE;

	return res;
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

	flags |= __O_KERNFS;

	fd = ftfs_get_unused_fd_flags(flags);
	if(fd < 0)
		return fd;

	SOUTHBOUND_ATTACH;
	f = filp_open(name, flags, 0755);
	SOUTHBOUND_RESTORE;

	if (IS_ERR(f)) {
		ftfs_put_unused_fd(fd);
		ftfs_error(__func__, "filp_open (%s) failed:%d", name,
			PTR_ERR(f));
		return PTR_ERR(f);
	}

	inode = f->f_dentry->d_inode;
	if(S_ISDIR(inode->i_mode)) {
		fsnotify_open(f);
		ftfs_fd_install(fd, f);
		return fd;
	}

	ftfs_put_unused_fd(fd);
	ftfs_error(__func__, "%s is not a dir", name);
	ftfs_filp_close(f);

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

struct dirent64 * readdir64(DIR* dirp) {
	int length;
	struct dirent64* dirent;

	// Yang: It seems that although ft-index checks readdir return NULL,
	//       their code never does anything about it.
	if (!dirp || IS_ERR(dirp))
		return NULL;

	if(dirp->buf_pos >= dirp->buf_end) {
		length = getdents64(dirp->fd, (struct linux_dirent64 *)dirp->buf,
			    sizeof dirp->buf);
		if(length <= 0)
			return NULL;

		dirp->buf_pos = 0;
		dirp->buf_end = length;
	}

	dirent = (void *)(dirp->buf + dirp->buf_pos);
	dirp->buf_pos += dirent->d_reclen;

	return dirent;
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


