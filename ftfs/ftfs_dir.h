#ifndef _FTFS_DIR_H
#define _FTFS_DIR_H

#include <linux/dirent.h>

int resolve_ftfs_dir_symbols(void);

int mkdir(const char *pathname, umode_t mode);
int rmdir(const char *pathname);
int getdents64(unsigned int fd, struct linux_dirent64 *dirent,
		      unsigned int count);
//char *getcwd(char *buf, int buflen);
char *basename(char *name);

#endif /* _FTFS_DIR_H */
