#ifndef FTFS_PTHREAD_UNION_H
#define FTFS_PTHREAD_UNION_H

#include <pthread.h>
#include "util/list.h"
#include "sb_pthread_sizes.h"

typedef union pthread_mutex_union {
	pthread_mutex_t pmutex;
	char kmutex[MUTEX_SIZE];
} pthread_mutex_union_t;

typedef union pthread_cond_union {
	pthread_cond_t pcond;
	char kcond[COND_SIZE];
} pthread_cond_union_t;

typedef union pthread_rwlock_union {
	pthread_rwlock_t prwlock;
	char krwlock[RW_LOCK_SIZE];
} pthread_rwlock_union_t;

#endif
