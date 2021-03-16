/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#ifndef FTFS_FRWLOCK_H
#define FTFS_FRWLOCK_H
#include "sb_rwsem.h"
#include <linux/linkage.h>
#include <linux/types.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/spinlock.h>
#include <linux/atomic.h>
#include <linux/rwsem.h>
#include <linux/sched.h>
#define ftfs_init_rwsem(sem)                                         \
do {                                                            \
                                                                 \
         __ftfs_init_rwsem((sem), #sem);                      \
} while (0)

void __sched ftfs_down_read(struct rw_semaphore *sem, pthread_mutex_t * mux);
int ftfs_down_read_trylock(struct rw_semaphore *sem);
void __sched ftfs_down_write(struct rw_semaphore *sem, pthread_mutex_t * mux);
int ftfs_down_write_trylock(struct rw_semaphore *sem);
void ftfs_up_read(struct rw_semaphore *sem);
void ftfs_up_write(struct rw_semaphore *sem);

#endif
