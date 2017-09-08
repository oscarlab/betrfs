/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include "ftfs_frwlock.h"
#include <linux/types.h>
#include <linux/kernel.h>
#include <linux/sched.h>
#include <linux/export.h>
#include <linux/rwsem.h>
#include <linux/atomic.h>

void __sched ftfs_down_read(struct rw_semaphore *sem)
{
	might_sleep();
	LOCK_CONTENDED(sem, __ftfs_down_read_trylock, __ftfs_down_read);
}


/*
 * trylock for reading -- returns 1 if successful, 0 if contention
 */
int ftfs_down_read_trylock(struct rw_semaphore *sem)
{
	int ret = __ftfs_down_read_trylock(sem);
	return ret;
}

/*
 * lock for writing
 */
void __sched ftfs_down_write(struct rw_semaphore *sem)
{
	might_sleep();
	LOCK_CONTENDED(sem, __ftfs_down_write_trylock, __ftfs_down_write);
}


/*
 * trylock for writing -- returns 1 if successful, 0 if contention
 */
int ftfs_down_write_trylock(struct rw_semaphore *sem)
{
	int ret = __ftfs_down_write_trylock(sem);
	return ret;
}

/*
 * release a read lock
 */
void ftfs_up_read(struct rw_semaphore *sem)
{
	__ftfs_up_read(sem);
}


/*
 * release a write lock
 */
void ftfs_up_write(struct rw_semaphore *sem)
{
	__ftfs_up_write(sem);
}


