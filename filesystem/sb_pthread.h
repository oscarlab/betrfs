/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef FTFS_PTHREAD_H
#define FTFS_PTHREAD_H

#include <linux/sched.h>
#include <linux/completion.h>
#include <linux/time.h>

#define PTHREAD_MUTEX_ADAPTIVE_NP 3

#define PTHREAD_INIT_MAGIC 71356028

/* YZJ: copy from pthread.h */
#define PTHREAD_PROCESS_PRIVATE   0

struct true_pthread_struct {
	int errno;
	void *(*func) (void *);
	void *arg;
	struct task_struct *t;
	void *ret_val;
	struct completion event;
};


typedef struct pthread_condattr_t {
} pthread_condattr_t;

typedef struct pthread_cond_t {
	wait_queue_head_t wq;
	int init;
} pthread_cond_t;

typedef struct pthread_attr_t {
} pthread_attr_t;

//struct true_pthread_struct;

typedef struct pthread_t {
  struct true_pthread_struct *tps;
} pthread_t;

typedef struct pthread_mutexattr_t {
} pthread_mutexattr_t;


#if defined(CONFIG_DEBUG_LOCK_ALLOC)
	#error "One or more CONFIG_* set and inconsistent with pthread_union.h"
#endif

typedef struct pthread_mutex_t {
	struct mutex mutex;
	int init;
} pthread_mutex_t;

//#define DEBUG_NESTED_SEMAPHORES 0
#if defined (DEBUG_NESTED_SEMAPHORES)
#include <linux/slab.h>
#endif

typedef struct pthread_rwlock_t {
	struct rw_semaphore lock;
        struct pthread_mutex_t* mutex;
#if defined (DEBUG_NESTED_SEMAPHORES)
	struct list_head holders; //struct true_pthread_struct *owner;
	struct spinlock list_lock;
#endif
	int init;
} pthread_rwlock_t;

/* YZJ: copy from website */

typedef struct pthread_rwlockattr_t {
	unsigned long rwattr_flags;
	int rwattr_spare1;
	int rwattr_priority;
	int rwattr_prioceiling;
	int rwattr_protocol;
	int rwattr_spare2[6];
	int rwattr_name[31+1];
} pthread_rwlockattr_t;

typedef struct pthread_key_t {
} pthread_key_t;

extern int pthread_mutexattr_init(pthread_mutexattr_t *);
extern int pthread_mutexattr_settype(pthread_mutexattr_t *, int);
extern int pthread_mutexattr_destroy(pthread_mutexattr_t *);

extern int pthread_mutex_init(pthread_mutex_t *, const pthread_mutexattr_t *);
extern int pthread_mutex_destroy(pthread_mutex_t *);
extern int pthread_mutex_lock(pthread_mutex_t *);
extern int pthread_mutex_unlock(pthread_mutex_t *);

extern int pthread_cond_init(pthread_cond_t *, const pthread_condattr_t *);
extern int pthread_cond_destroy(pthread_cond_t *);
extern int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *);
extern int pthread_cond_timedwait(pthread_cond_t *, pthread_mutex_t *,
                                  const struct timespec *);
extern int pthread_cond_signal(pthread_cond_t *);
extern int pthread_cond_broadcast(pthread_cond_t *);

extern int pthread_rwlockattr_destroy(pthread_rwlockattr_t *);
extern int pthread_rwlockattr_init(pthread_rwlockattr_t *);
extern int pthread_rwlockattr_setkind_np(pthread_rwlockattr_t *, int *);
extern int
pthread_rwlock_init(pthread_rwlock_t *, const pthread_rwlockattr_t *);

extern int 
pthread_rwlock_set_mutex(pthread_rwlock_t *rwlock, pthread_mutex_t * mutex);
extern int pthread_rwlock_destroy(pthread_rwlock_t *);
extern int pthread_rwlock_rdlock(pthread_rwlock_t *);
extern int pthread_rwlock_unlock(pthread_rwlock_t *);
extern int pthread_rwlock_wrlock(pthread_rwlock_t *);
extern int pthread_rwlock_rdunlock(pthread_rwlock_t *);
extern int pthread_rwlock_wrunlock(pthread_rwlock_t *);

extern pthread_t pthread_self(void);
extern int
pthread_create_debug(pthread_t *, const pthread_attr_t *,
               void *(*start_routine) (void *), void *arg, char * str);
//extern void pthread_exit(void *);
extern int
pthread_create(pthread_t *, const pthread_attr_t *,
               void *(*start_routine) (void *), void *arg);
//extern void pthread_exit(void *);
extern int pthread_join(pthread_t, void **);
extern int pthread_detach(pthread_t);
extern int pthread_key_create(pthread_key_t *, void (*destructor)(void *));
extern int pthread_key_delete(pthread_key_t);
extern void *pthread_getspecific(pthread_key_t);
extern int pthread_setspecific(pthread_key_t, const void *);

struct kthread {
	unsigned long flags;
	unsigned int cpu;
	void *data;
	struct completion parked;
	struct completion exited;
};

#define __to_kthread(vfork)  container_of(vfork, struct kthread, exited)
static inline struct kthread *to_kthread(struct task_struct *k) {
	return __to_kthread(k->vfork_done);
}


#if defined (DEBUG_NESTED_SEMAPHORES)
struct lock_holder {
	struct true_pthread_struct *tps;
	struct list_head list;
};

static inline void debug_init_semaphore_holders(pthread_rwlock_t *rwlock)
{
	spin_lock_init(&rwlock->list_lock);
	INIT_LIST_HEAD(&rwlock->holders);
}

static inline void debug_nested_semaphore(struct pthread_rwlock_t *rwlock)
{
	struct true_pthread_struct *tps = pthread_self().tps;
	struct lock_holder *holder, *next;
	spin_lock(&rwlock->list_lock);
	list_for_each_entry_safe(holder, next, &rwlock->holders, list) {
		BUG_ON(holder->tps == tps);
	}
	spin_unlock(&rwlock->list_lock);
}
static inline void debug_add_semaphore_owner(struct pthread_rwlock_t *rwlock)
{
	struct lock_holder *holder, *next;
	struct lock_holder *to_add = kmalloc(sizeof(*to_add), GFP_KERNEL);
	INIT_LIST_HEAD(&to_add->list);
	to_add->tps = pthread_self().tps;
	spin_lock(&rwlock->list_lock);
	list_for_each_entry_safe(holder, next, &rwlock->holders, list) {
		BUG_ON(holder->tps == to_add->tps);
	}
	list_add(&to_add->list, &rwlock->holders);
	spin_unlock(&rwlock->list_lock);
}

static inline 
void debug_release_semaphore_owner(struct pthread_rwlock_t *rwlock)
{
	struct lock_holder *holder, *next;
	struct lock_holder *found = NULL;
	struct true_pthread_struct *tps = pthread_self().tps;
	spin_lock(&rwlock->list_lock);
	list_for_each_entry_safe(holder, next, &rwlock->holders, list) {
		if(holder->tps == tps) {
			found = holder;
			break;
		}
	}
	BUG_ON(!found);
	list_del(&found->list);
	spin_unlock(&rwlock->list_lock);
	kfree(found);
}

#else
#define debug_init_semaphore_holders(rwlock)
#define debug_nested_semaphore(rwlock)
#define debug_add_semaphore_owner(rwlock)
#define debug_release_semaphore_owner(rwlock)
#endif

#endif /* FTFS_PTHREAD_H */
