/*
 * Percpu refcounts:
 * (C) 2012 Google, Inc.
 * Author: Kent Overstreet <koverstreet@google.com>
 *
 * This implements a refcount with similar semantics to atomic_t - atomic_inc(),
 * atomic_dec_and_test() - but percpu.
 *
 * There's one important difference between percpu refs and normal atomic_t
 * refcounts; you have to keep track of your initial refcount, and then when you
 * start shutting down you call percpu_ref_kill() _before_ dropping the initial
 * refcount.
 *
 * The refcount will have a range of 0 to ((1U << 31) - 1), i.e. one bit less
 * than an atomic_t - this is because of the way shutdown works, see
 * percpu_ref_kill()/PCPU_COUNT_BIAS.
 *
 * Before you call percpu_ref_kill(), percpu_ref_put() does not check for the
 * refcount hitting 0 - it can't, if it was in percpu mode. percpu_ref_kill()
 * puts the ref back in single atomic_t mode, collecting the per cpu refs and
 * issuing the appropriate barriers, and then marks the ref as shutting down so
 * that percpu_ref_put() will check for the ref hitting 0.  After it returns,
 * it's safe to drop the initial ref.
 *
 * USAGE:
 *
 * See fs/aio.c for some example usage; it's used there for struct kioctx, which
 * is created when userspaces calls io_setup(), and destroyed when userspace
 * calls io_destroy() or the process exits.
 *
 * In the aio code, kill_ioctx() is called when we wish to destroy a kioctx; it
 * calls percpu_ref_kill(), then hlist_del_rcu() and sychronize_rcu() to remove
 * the kioctx from the proccess's list of kioctxs - after that, there can't be
 * any new users of the kioctx (from lookup_ioctx()) and it's then safe to drop
 * the initial ref with percpu_ref_put().
 *
 * Code that does a two stage shutdown like this often needs some kind of
 * explicit synchronization to ensure the initial refcount can only be dropped
 * once - percpu_ref_kill() does this for you, it returns true once and false if
 * someone else already called it. The aio code uses it this way, but it's not
 * necessary if the code has some other mechanism to synchronize teardown.
 * around.
 */

#ifndef _LINUX_PERCPU_REFCOUNT_H
#define _LINUX_PERCPU_REFCOUNT_H

#include <linux/atomic.h>
#include <linux/kernel.h>
#include <linux/percpu.h>
#include <linux/rcupdate.h>

struct percpu_ref;
typedef void (percpu_ref_func_t)(struct percpu_ref *);

struct percpu_ref {
	atomic_t		count;
	/*
	 * The low bit of the pointer indicates whether the ref is in percpu
	 * mode; if set, then get/put will manipulate the atomic_t (this is a
	 * hack because we need to keep the pointer around for
	 * percpu_ref_kill_rcu())
	 */
	unsigned __percpu	*pcpu_count;
	percpu_ref_func_t	*release;
	percpu_ref_func_t	*confirm_kill;
	struct rcu_head		rcu;
};

int __must_check percpu_ref_init(struct percpu_ref *ref,
				 percpu_ref_func_t *release);
void percpu_ref_cancel_init(struct percpu_ref *ref);
void percpu_ref_kill_and_confirm(struct percpu_ref *ref,
				 percpu_ref_func_t *confirm_kill);

/**
 * percpu_ref_kill - drop the initial ref
 * @ref: percpu_ref to kill
 *
 * Must be used to drop the initial ref on a percpu refcount; must be called
 * precisely once before shutdown.
 *
 * Puts @ref in non percpu mode, then does a call_rcu() before gathering up the
 * percpu counters and dropping the initial ref.
 */
static inline void percpu_ref_kill(struct percpu_ref *ref)
{
	return percpu_ref_kill_and_confirm(ref, NULL);
}

#define PCPU_STATUS_BITS	2
#define PCPU_STATUS_MASK	((1 << PCPU_STATUS_BITS) - 1)
#define PCPU_REF_PTR		0
#define PCPU_REF_DEAD		1

#define REF_STATUS(count)	(((unsigned long) count) & PCPU_STATUS_MASK)

/**
 * percpu_ref_get - increment a percpu refcount
 * @ref: percpu_ref to get
 *
 * Analagous to atomic_inc().
  */
static inline void percpu_ref_get(struct percpu_ref *ref)
{
	unsigned __percpu *pcpu_count;

	rcu_read_lock_sched();

	pcpu_count = ACCESS_ONCE(ref->pcpu_count);

	if (likely(REF_STATUS(pcpu_count) == PCPU_REF_PTR))
		__this_cpu_inc(*pcpu_count);
	else
		atomic_inc(&ref->count);

	rcu_read_unlock_sched();
}

/**
 * percpu_ref_tryget - try to increment a percpu refcount
 * @ref: percpu_ref to try-get
 *
 * Increment a percpu refcount unless it has already been killed.  Returns
 * %true on success; %false on failure.
 *
 * Completion of percpu_ref_kill() in itself doesn't guarantee that tryget
 * will fail.  For such guarantee, percpu_ref_kill_and_confirm() should be
 * used.  After the confirm_kill callback is invoked, it's guaranteed that
 * no new reference will be given out by percpu_ref_tryget().
 */
static inline bool percpu_ref_tryget(struct percpu_ref *ref)
{
	unsigned __percpu *pcpu_count;
	int ret = false;

	rcu_read_lock_sched();

	pcpu_count = ACCESS_ONCE(ref->pcpu_count);

	if (likely(REF_STATUS(pcpu_count) == PCPU_REF_PTR)) {
		__this_cpu_inc(*pcpu_count);
		ret = true;
	}

	rcu_read_unlock_sched();

	return ret;
}

/**
 * percpu_ref_put - decrement a percpu refcount
 * @ref: percpu_ref to put
 *
 * Decrement the refcount, and if 0, call the release function (which was passed
 * to percpu_ref_init())
 */
static inline void percpu_ref_put(struct percpu_ref *ref)
{
	unsigned __percpu *pcpu_count;

	rcu_read_lock_sched();

	pcpu_count = ACCESS_ONCE(ref->pcpu_count);

	if (likely(REF_STATUS(pcpu_count) == PCPU_REF_PTR))
		__this_cpu_dec(*pcpu_count);
	else if (unlikely(atomic_dec_and_test(&ref->count)))
		ref->release(ref);

	rcu_read_unlock_sched();
}

#endif
