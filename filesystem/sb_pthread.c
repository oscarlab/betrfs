/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound interface (really, klibc),
 * which implements the pthread APIs needed by the key-value store
 * (and some testing frameworks), using kernel threads and synchronization
 * primitives.
 */

#include <linux/mutex.h>
#include <linux/slab.h>
#include <linux/sched.h>
#include <linux/kthread.h>
#include <linux/hashtable.h>
#include <linux/time.h>

#include "ftfs.h"
#include "ftfs_southbound.h"
#include "sb_pthread.h"
#include "sb_rwsem.h"
/* dummy phtread_mutexattr */
int
pthread_mutexattr_init(pthread_mutexattr_t *attr) {
	return 0;
}

int
pthread_mutexattr_settype(pthread_mutexattr_t *attr, int type) {
	return 0;
}

int
pthread_mutexattr_destroy(pthread_mutexattr_t *attr) {
	return 0;
}

/* only deal with mutex of type PTHREAD_MUTEX_ADAPTIVE_NP */
int
pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr) {
	mutex_init(&mutex->mutex);
	mutex->init = PTHREAD_INIT_MAGIC;
	return 0;
}

int
pthread_mutex_destroy(pthread_mutex_t *mutex) {
	return 0;
}

int pthread_bug(char *error)
{
	ftfs_error(__func__,  error);
	BUG();
	return -ENOSYS;
}

int
pthread_mutex_lock(pthread_mutex_t *mutex) {
	if (mutex->init != PTHREAD_INIT_MAGIC)
		return pthread_bug("lock call with uninitialized mutex");

	mutex_lock(&mutex->mutex);
	return 0;
}

int
pthread_mutex_unlock(pthread_mutex_t *mutex) {
	FTFS_DEBUG_ON(mutex->init != PTHREAD_INIT_MAGIC);
	mutex_unlock(&mutex->mutex);
	return 0;
}

int
pthread_cond_init(pthread_cond_t *cond, const pthread_condattr_t *attr) {
	init_waitqueue_head(&cond->wq);
	cond->init = PTHREAD_INIT_MAGIC;
	return 0;
}

int
pthread_cond_destroy(pthread_cond_t *cond) {
	return 0;
}

int pthread_cond_bug(char *error)
{
	ftfs_error(__func__,  error);
	BUG();
	return -ENOSYS;
}


int
pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
	int ret = 0;
	DEFINE_WAIT(__wait);

	/* Grr.  Sometimes code initializes these with
	   PTHREAD_COND_INITIALIZER, which is all zeros.  So we have to
	   catch that and initialize it now. */
	if (cond->init != PTHREAD_INIT_MAGIC)
		return pthread_cond_bug("pthread_cond_wait");

	prepare_to_wait_exclusive(&cond->wq, &__wait, TASK_INTERRUPTIBLE);
	mutex_unlock(&mutex->mutex);
	schedule();
	mutex_lock(&mutex->mutex);
	finish_wait(&cond->wq, &__wait);

	return ret;
}


/* hope timespec has the same structure in kernel and user space */
int
pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex,
		       const struct timespec *abstime) {

	int ret = 0;
	struct timespec now, diff;
	unsigned long timeout;

	DEFINE_WAIT(__wait);

	if (cond->init != PTHREAD_INIT_MAGIC)
		return pthread_cond_bug("pthread_cond_timedwait");

	getnstimeofday(&now);
	if (timespec_compare(abstime, &now) < 1)
		return ETIMEDOUT;

	diff = timespec_sub(*abstime, now);
	timeout = timespec_to_jiffies(&diff);

	prepare_to_wait_exclusive(&cond->wq, &__wait, TASK_INTERRUPTIBLE);

	mutex_unlock(&mutex->mutex);
	ret = schedule_timeout(timeout);

	mutex_lock(&mutex->mutex);
	finish_wait(&cond->wq, &__wait);
	//return (ret > 0) ? ETIMEDOUT : 0;
	return (ret > 0)? 0: ETIMEDOUT;
}

int
pthread_cond_signal(pthread_cond_t *cond) {
	FTFS_DEBUG_ON(cond->init != PTHREAD_INIT_MAGIC);
	wake_up(&cond->wq);
	return 0;
}

int
pthread_cond_broadcast(pthread_cond_t *cond) {
	FTFS_DEBUG_ON(cond->init != PTHREAD_INIT_MAGIC);
	wake_up_all(&cond->wq);
	return 0;
}

/* XXX: currently don't deal with pthread_rwlockattr_t, code in tokukv uses
 * pthread_rwlockattr_setkind_np to make rwlock prefer writer
 */

/* YZJ: ft/checkpointer.cc use it */

int
pthread_rwlockattr_init(pthread_rwlockattr_t *attr) {
	char str[] = "pthread rwlock";
	attr->rwattr_flags = PTHREAD_PROCESS_PRIVATE;
	if (!attr->rwattr_name)
		memcpy(attr->rwattr_name, str, sizeof(str));
	ftfs_log(__func__, "rwattr addr:%lx\n", (unsigned long)attr);
	return 0;
}

/* YZJ: everything is in stack, so nothing to do */

int
pthread_rwlockattr_destroy(pthread_rwlockattr_t *attr) {
	return 0;
}

int
pthread_rwlockattr_setkind_np(pthread_rwlockattr_t *attr, int *type) {
	return 0;
}

int
pthread_rwlock_init(pthread_rwlock_t *rwlock,
		    const pthread_rwlockattr_t *attr) {
	__sb_init_rwsem(&rwlock->lock, "sem");
	rwlock->init = PTHREAD_INIT_MAGIC;
	debug_init_semaphore_holders(rwlock);
	return 0;
}

int
pthread_rwlock_set_mutex(pthread_rwlock_t *rwlock,
			pthread_mutex_t * mutex){
      rwlock->mutex = mutex;
      return 0;
}
int
pthread_rwlock_destroy(pthread_rwlock_t *rwlock) {
	return 0;
}

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


/*
 * trylock for reading -- returns 1 if successful, 0 if contention
 */
int
pthread_rwlock_try_rdlock(pthread_rwlock_t *rwlock) {
	int ret = 0;
	FTFS_DEBUG_ON(rwlock->init != PTHREAD_INIT_MAGIC);
	debug_nested_semaphore(rwlock);
	ret = __sb_down_read_trylock(&rwlock->lock);
	debug_add_semaphore_owner(rwlock);
	return ret;
}

int
pthread_rwlock_try_wrlock(pthread_rwlock_t *rwlock) {
	int ret = 0;
	FTFS_DEBUG_ON(rwlock->init != PTHREAD_INIT_MAGIC);
	debug_nested_semaphore(rwlock);
	ret = __sb_down_write_trylock(&rwlock->lock);
	debug_add_semaphore_owner(rwlock);
	return ret;
}

int
pthread_rwlock_rdlock(pthread_rwlock_t *rwlock) {
	FTFS_DEBUG_ON(rwlock->init != PTHREAD_INIT_MAGIC);
	debug_nested_semaphore(rwlock);
	// Now the mutex used to protect the frwlock at the user level
	// is passed down to the kernel along with rwsem. rwsem makes
	// sure the mutex is only unlocked after it is queued into the
	// waitqueue, which (hopefully) guarantees the integrity of
	// num_*(num_users/num_readers, etc) at the user level
	might_sleep();
	LOCK_CONTENDED2(&rwlock->lock, rwlock->mutex,  __sb_down_read_trylock, __sb_down_read);
	debug_add_semaphore_owner(rwlock);
	return 0;
}

int
pthread_rwlock_wrlock(pthread_rwlock_t *rwlock) {
	FTFS_DEBUG_ON(rwlock->init != PTHREAD_INIT_MAGIC);
	debug_nested_semaphore(rwlock);
	might_sleep();
	LOCK_CONTENDED2(&rwlock->lock, rwlock->mutex, __sb_down_write_trylock, __sb_down_write);
	debug_add_semaphore_owner(rwlock);
	return 0;
}

int
pthread_rwlock_unlock(pthread_rwlock_t *rwlock) {
	/* no such thing in kernel */
	ftfs_error(__func__, "DO NOT CALL ME!!!"
		   "pthread_rwlock_unlock() not supported.\n"
		   "Use pthread_rwlock_rdunlock()/pthread_rwlock_wrunlock()");
	BUG();
	return -EINVAL;
}

int
pthread_rwlock_rdunlock(pthread_rwlock_t *rwlock) {
	FTFS_DEBUG_ON(rwlock->init != PTHREAD_INIT_MAGIC);
	__sb_up_read(&rwlock->lock);
	debug_release_semaphore_owner(rwlock);
	return 0;
}

int
pthread_rwlock_wrunlock(pthread_rwlock_t *rwlock) {
	FTFS_DEBUG_ON(rwlock->init != PTHREAD_INIT_MAGIC);
	__sb_up_write(&rwlock->lock);
	debug_release_semaphore_owner(rwlock);
	return 0;
}

static int kthread_func(void *arg) {
	struct true_pthread_struct *tps = (struct true_pthread_struct *)arg;

	tps->t = current;

	/* adjust the priority of kthread */
	set_user_nice(current, 0);

	tps->ret_val = tps->func(tps->arg);

	/*
	 * pthread_join should be called by only 1 thread, do
	 * wake_up_all anyway
	 */

	//wake_up_all(&tps->wq);
	complete_all(&tps->event);
	return 0;
}

/* pthread_attr_t is never used in tokukv */
int
pthread_create_debug(pthread_t *thread, const pthread_attr_t *attr,
	       void *(*start_routine) (void *), void *arg, char * debug_str)
{
	thread->tps = kmalloc(sizeof(struct true_pthread_struct), GFP_KERNEL);
	if (thread->tps == NULL) {
		return EAGAIN;
	}

	init_completion(&thread->tps->event);
	thread->tps->func = start_routine;
	thread->tps->arg = arg;
	thread->tps->ret_val = 0;
	thread->tps->errno = 0;
	thread->tps->t = kthread_run(kthread_func, thread->tps, debug_str);
	if (IS_ERR(thread->tps->t)) {
		kfree(thread->tps);
		return EAGAIN;
	}
	return 0;
}
int
pthread_create(pthread_t *thread, const pthread_attr_t *attr,
	       void *(*start_routine) (void *), void *arg)
{
	thread->tps = kmalloc(sizeof(struct true_pthread_struct), GFP_KERNEL);
	if (thread->tps == NULL) {
		return EAGAIN;
	}

	init_completion(&thread->tps->event);
	thread->tps->func = start_routine;
	thread->tps->arg = arg;
	thread->tps->ret_val = 0;
	thread->tps->errno = 0;
	thread->tps->t = kthread_run(kthread_func, thread->tps, "ftfs_thread");
	if (IS_ERR(thread->tps->t)) {
		kfree(thread->tps);
		return EAGAIN;
	}
	return 0;
}

void *kthread_data(struct task_struct *task)
{
	return to_kthread(task)->data;
}

pthread_t pthread_self(void) {
	struct task_struct * kthread_task = get_current();
	struct true_pthread_struct * true_ps =
		(struct true_pthread_struct *) kthread_data(kthread_task);
	pthread_t p = {.tps = true_ps};
	return p;
}

int pthread_detach(pthread_t thread)
{
	return 0;
}

/*
void pthread_exit(void *retval)
{
	pthread_t self = pthread_self();
	self.tps->ret_val = retval;
}
*/


int
pthread_join(pthread_t thread, void **retval) {
	wait_for_completion(&thread.tps->event);
	if (retval != NULL) {
		*retval = thread.tps->ret_val;
		ftfs_log(__func__, "thread.tps->ret_val: %p\n", *retval);
	}
	kfree(thread.tps);

	return 0;
}
