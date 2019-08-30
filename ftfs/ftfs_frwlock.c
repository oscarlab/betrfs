/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include "ftfs_frwlock.h"
#include <linux/types.h>
#include <linux/kernel.h>
#include <linux/sched.h>
#include <linux/export.h>
#include <linux/rwsem.h>
#include <linux/atomic.h>
//Rename LOCK_CONTENDED to LOCK_CONTENDED2 to avoid name clash with kernel.
//LOCK_CONTENDED2 takes 2 params as opposed to LOCK_CONTENDED
#define LOCK_CONTENDED2(_sem, _mux,  try, lock)			\
do {								\
	if (!try(_sem)) {					\
		lock_contended(&(_sem)->dep_map, _RET_IP_);	\
		lock(_sem, _mux);					\
	}							\
	lock_acquired(&(_sem)->dep_map, _RET_IP_);			\
} while (0)
//Now the mutex used to protect the frwlock at the user level is passed down
//to the kernel along with rwsem. rwsem makes sure the mutex is only unlocked
//after it is queued into the waitqueue, which (hopefully) guarantees the
//integrity of num_*(num_users/num_readers, etc) at the user level
void __sched ftfs_down_read(struct rw_semaphore *sem, pthread_mutex_t * mux)
{
	might_sleep();
	LOCK_CONTENDED2(sem, mux,  __ftfs_down_read_trylock, __ftfs_down_read);
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
void __sched ftfs_down_write(struct rw_semaphore *sem, pthread_mutex_t * mux)
{
	might_sleep();
	LOCK_CONTENDED2(sem, mux, __ftfs_down_write_trylock, __ftfs_down_write);
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


