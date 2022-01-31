/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
/*as values and the counter type limits the number of
 * potential readers/writers to 32767 for 32 bits and 2147483647
 * for 64 bits.
 */

#define FTFS_RWSEM_ACTIVE_MASK		0xffffffffL
#define FTFS_RWSEM_UNLOCKED_VALUE		0x00000000L
#define FTFS_RWSEM_ACTIVE_BIAS		0x00000001L
#define FTFS_RWSEM_WAITING_BIAS		(-FTFS_RWSEM_ACTIVE_MASK-1)
#define FTFS_RWSEM_ACTIVE_READ_BIAS		FTFS_RWSEM_ACTIVE_BIAS
#define FTFS_RWSEM_ACTIVE_WRITE_BIAS		(FTFS_RWSEM_WAITING_BIAS + RWSEM_ACTIVE_BIAS)

#include <linux/version.h>
#include <linux/rwsem.h>
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,19,99)
#include <linux/sched.h>
#else
#include <linux/sched/debug.h>
#endif
#include "sb_pthread.h"
/*
 * lock for reading
 */

struct rw_semaphore __sched *sb_rwsem_down_read_failed(struct rw_semaphore *sem, pthread_mutex_t * mux);
struct rw_semaphore __sched *sb_rwsem_down_write_failed(struct rw_semaphore *sem, pthread_mutex_t * mux);
struct rw_semaphore *sb_rwsem_wake(struct rw_semaphore *sem);
void __sb_init_rwsem(struct rw_semaphore *sem, const char *name);

static inline void __sb_down_read(struct rw_semaphore *sem, pthread_mutex_t * mux)
{
	if (unlikely(atomic_long_inc_return((atomic_long_t *)&sem->count) <= 0))
		sb_rwsem_down_read_failed(sem, mux);
}

/*
 * trylock for reading -- returns 1 if successful, 0 if contention
 * copied from asm-generic/rwsem.h
 */
static inline int __sb_down_read_trylock(struct rw_semaphore *sem)
{
	long tmp;
#if LINUX_VERSION_CODE <  KERNEL_VERSION(4,19,99)
 	while ((tmp = sem->count) >= 0) {
             if (tmp == cmpxchg(&sem->count, tmp,
                               tmp + FTFS_RWSEM_ACTIVE_READ_BIAS)) {
                    return 1;
		}
	}
#else
	while ((tmp = atomic_long_read(&sem->count)) >= 0) {
		if (tmp == atomic_long_cmpxchg_acquire(&sem->count, tmp,
				   tmp + FTFS_RWSEM_ACTIVE_READ_BIAS)) {
			return 1;
		}
	}


#endif
	return 0;

}

/*
 * lock for writing
 */
static inline void __sb_down_write_nested(struct rw_semaphore *sem,  pthread_mutex_t * mux, int subclass)
{


        long tmp;
      tmp = atomic_long_add_return(FTFS_RWSEM_ACTIVE_WRITE_BIAS,
                                      (atomic_long_t *)&sem->count);
      if (unlikely(tmp != FTFS_RWSEM_ACTIVE_WRITE_BIAS))
	      sb_rwsem_down_write_failed(sem, mux);
}

static inline void __sb_down_write(struct rw_semaphore *sem, pthread_mutex_t * mux)
{
	__sb_down_write_nested(sem, mux, 0);
}

/*
 * trylock for writing -- returns 1 if successful, 0 if contention
 * copied from asm-generic/rwsem.h
 */
static inline int __sb_down_write_trylock(struct rw_semaphore *sem)
{
#if LINUX_VERSION_CODE <  KERNEL_VERSION(4,19,99)
	long ret = cmpxchg(&sem->count, FTFS_RWSEM_UNLOCKED_VALUE,
			   FTFS_RWSEM_ACTIVE_WRITE_BIAS);
	if (ret == FTFS_RWSEM_UNLOCKED_VALUE)
		return 1;
	return 0;
#else
	long tmp;

	tmp = atomic_long_cmpxchg_acquire(&sem->count, FTFS_RWSEM_UNLOCKED_VALUE,
		      FTFS_RWSEM_ACTIVE_WRITE_BIAS);
	return (tmp == FTFS_RWSEM_UNLOCKED_VALUE) ? 1 : 0;

#endif
}

/*
 * unlock after reading
 */
static inline void __sb_up_read(struct rw_semaphore *sem)
{
	long tmp;
 	tmp = atomic_long_dec_return((atomic_long_t *)&sem->count);
 	if (unlikely(tmp < -1 && (tmp & FTFS_RWSEM_ACTIVE_MASK) == 0))
		sb_rwsem_wake(sem);
}

/*
 * unlock after writing
 */
static inline void __sb_up_write(struct rw_semaphore *sem) {

	if (unlikely(atomic_long_sub_return(FTFS_RWSEM_ACTIVE_WRITE_BIAS,
                             (atomic_long_t *)&sem->count) < 0))
		sb_rwsem_wake(sem);
}


/*
 * implement atomic add functionality
 */
static inline void sb_rwsem_atomic_add(long delta, struct rw_semaphore *sem)
{
	asm volatile(LOCK_PREFIX _ASM_ADD "%1,%0"
		     : "+m" (sem->count)
		     : "er" (delta));
}

/*
 * implement exchange and add functionality
 *
 * FIXME: rwsem_atomic_update no long exist after 4.8.0
 * Is it still safe to use this API?
 */
static inline long sb_rwsem_atomic_update(long delta, struct rw_semaphore *sem)
{
#if LINUX_VERSION_CODE <  KERNEL_VERSION(4,19,99)
	return delta + xadd(&sem->count, delta);
#else
	return atomic_long_add_return(delta, (atomic_long_t *)&sem->count);
#endif
}
