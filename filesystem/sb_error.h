/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef _FTFS_ERROR_H
#define _FTFS_ERROR_H

void ftfs_set_errno(int ret);
int ftfs_get_errno(void);

void set_errno(int ret);
int get_errno(void);
void perror(const char *s);

void print_day_time(void);
#endif
