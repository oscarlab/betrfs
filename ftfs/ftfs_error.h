#ifndef _FTFS_ERROR_H
#define _FTFS_ERROR_H

void ftfs_set_errno(int ret);
int ftfs_get_errno(void);

void set_errno(int ret);
int get_errno(void);
void perror(const char *s);

#endif
