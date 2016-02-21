#ifndef _FTFS_H
#define _FTFS_H

#  define FTFS_DEBUG_ON(err) 
static inline void ftfs_error (const char * function, const char * fmt, ...) {}
static inline void ftfs_log (const char * function, const char * fmt, ...) {}

#endif
