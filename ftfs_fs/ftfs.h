/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef _FTFS_H
#define _FTFS_H

#  define FTFS_DEBUG_ON(err)
static inline void ftfs_error (const char * function, const char * fmt, ...) {}
static inline void ftfs_log (const char * function, const char * fmt, ...) {}

#endif
