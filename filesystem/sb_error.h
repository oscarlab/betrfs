/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef _FTFS_ERROR_H
#define _FTFS_ERROR_H

/* This file is part of the "southbound" code in BetrFS (aka klibc).
 *
 * In particular, it provides "stubs" for implementing errno.
 */
void sb_set_errno(int ret);
int sb_get_errno(void);

#endif
