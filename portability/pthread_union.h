#ifndef FTFS_PTHREAD_UNION_H
#define FTFS_PTHREAD_UNION_H

#include <pthread_union_config_options.h>
#include <pthread.h>
#include "util/list.h"


typedef struct {
	long counter;
} atomic_t;

typedef unsigned char u8;
typedef unsigned short u16;

/* x86-spinlock definition from arch/x86/include/asm/spinlock_types.h */
typedef u8  __ticket_t;
typedef u16 __ticketpair_t;

#define TICKET_SHIFT	(sizeof(__ticket_t) * 8)

typedef struct arch_spinlock {
	union {
		__ticketpair_t head_tail;
		struct __raw_tickets {
			__ticket_t head;
			__ticket_t tail;
		} tickets;
	} blah;
} arch_spinlock_t;

#define __ARCH_SPIN_LOCK_UNLOCKED	{ { 0 } }

#define NR_LOCKDEP_CACHING_CLASSES      2
struct lock_class_key;
struct lock_class;
struct lockdep_map {
	struct lock_class_key	*key;
	struct lock_class	*class_cache[NR_LOCKDEP_CACHING_CLASSES];
	const char		*name;
#ifdef CONFIG_LOCK_STAT
	int			cpu;
	unsigned long		ip;
#endif
};


typedef struct raw_spinlock {
	arch_spinlock_t raw_lock;
#ifdef CONFIG_GENERIC_LOCKBREAK
	unsigned int break_lock;
#endif
#ifdef CONFIG_DEBUG_SPINLOCK
	unsigned int magic, owner_cpu;
	void *owner;
#endif
#ifdef CONFIG_DEBUG_LOCK_ALLOC
	struct lockdep_map dep_map;
#endif
} raw_spinlock_t;


#undef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)

typedef struct spinlock {
	union {
		struct raw_spinlock rlock;

#ifdef CONFIG_DEBUG_LOCK_ALLOC
# define LOCK_PADSIZE (offsetof(struct raw_spinlock, dep_map))
		struct {
			u8 __padding[LOCK_PADSIZE];
			struct lockdep_map dep_map;
		};
#endif
	};
} spinlock_t;


/* perhaps larger than necessary... */
//#define DEBUG_NESTED_SEMAPHORES 1 /* this is purely an ftfs thing */
struct task_struct;
struct mutex {
	/* 1: unlocked, 0: locked, negative: locked, possible waiters */
	atomic_t		count;
	spinlock_t		wait_lock;
	struct list_head	wait_list;
#if defined(CONFIG_DEBUG_MUTEXES) || defined(CONFIG_SMP)
	struct task_struct	*owner;
#endif
#ifdef CONFIG_MUTEX_SPIN_ON_OWNER
	void			*spin_mlock;	/* Spinner MCS lock */
#endif
#ifdef CONFIG_DEBUG_MUTEXES
	const char 		*name;
	void			*magic;
#endif
#ifdef CONFIG_DEBUG_LOCK_ALLOC
	struct lockdep_map	dep_map;
#endif
};

struct kernel_pthread_mutex {
	struct mutex mutex;
	int init;
};

typedef union pthread_mutex_union {
	pthread_mutex_t pmutex;
	struct kernel_pthread_mutex kmutex;
} pthread_mutex_union_t;


struct __wait_queue_head {
	spinlock_t lock;
	struct list_head task_list;
};
typedef struct __wait_queue_head wait_queue_head_t;


struct kernel_pthread_cond {
	wait_queue_head_t wq;
	int init;
};

typedef union pthread_cond_union {
	pthread_cond_t pcond;
	struct kernel_pthread_cond kcond;
} pthread_cond_union_t;



#ifdef CONFIG_RWSEM_GENERIC_SPINLOCK
/*
 * the rw-semaphore definition
 * - if activity is 0 then there are no active readers or writers
 * - if activity is +ve then that is the number of active readers
 * - if activity is -1 then there is one active writer
 * - if wait_list is not empty, then there are processes waiting for the semaphore
 */
typedef signed int __s32;
struct rw_semaphore {
	__s32                   activity;
	spinlock_t              wait_lock;
	struct list_head        wait_list;
#ifdef CONFIG_DEBUG_LOCK_ALLOC
	struct lockdep_map dep_map;
#endif
};
#else
/* x86 version */
typedef signed long rwsem_count_t;
struct rw_semaphore {
	rwsem_count_t           count;
	spinlock_t              wait_lock;
	struct list_head        wait_list;
#ifdef CONFIG_DEBUG_LOCK_ALLOC
	struct lockdep_map dep_map;
#endif
};
#endif //CONFIG_RWSEM_GENERIC_SPINLOCK
struct true_pthread_struct;
// The mux needs to be held until we either have the semaphore, or are in the wait queue, 
// otherwise there is a risk of non-fifo behavior. See the complementary ftfs_down_read.
struct kernel_rwlock {
	struct rw_semaphore lock;
        struct kernel_pthread_mutex * mutex;
	int init;
#ifdef DEBUG_NESTED_SEMAPHORES	
	struct list_head holders;
	spinlock_t list_lock;
#endif
	struct true_pthread_struct *owner;
};

typedef union pthread_rwlock_union {
	pthread_rwlock_t prwlock;
	struct kernel_rwlock krwlock;
} pthread_rwlock_union_t;

#endif
