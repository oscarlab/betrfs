/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LNET
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/fs_struct.h>
#include <linux/sched.h>

#include <linux/libcfs/libcfs.h>

#if defined(CONFIG_KGDB)
#include <asm/kgdb.h>
#endif

#define LINUX_WAITQ(w) ((wait_queue_t *) w)
#define LINUX_WAITQ_HEAD(w) ((wait_queue_head_t *) w)

void
init_waitqueue_entry_current(wait_queue_t *link)
{
	init_waitqueue_entry(LINUX_WAITQ(link), current);
}
EXPORT_SYMBOL(init_waitqueue_entry_current);

/**
 * wait_queue_t of Linux (version < 2.6.34) is a FIFO list for exclusively
 * waiting threads, which is not always desirable because all threads will
 * be waken up again and again, even user only needs a few of them to be
 * active most time. This is not good for performance because cache can
 * be polluted by different threads.
 *
 * LIFO list can resolve this problem because we always wakeup the most
 * recent active thread by default.
 *
 * NB: please don't call non-exclusive & exclusive wait on the same
 * waitq if add_wait_queue_exclusive_head is used.
 */
void
add_wait_queue_exclusive_head(wait_queue_head_t *waitq, wait_queue_t *link)
{
	unsigned long flags;

	spin_lock_irqsave(&LINUX_WAITQ_HEAD(waitq)->lock, flags);
	__add_wait_queue_exclusive(LINUX_WAITQ_HEAD(waitq), LINUX_WAITQ(link));
	spin_unlock_irqrestore(&LINUX_WAITQ_HEAD(waitq)->lock, flags);
}
EXPORT_SYMBOL(add_wait_queue_exclusive_head);

void
waitq_wait(wait_queue_t *link, cfs_task_state_t state)
{
	schedule();
}
EXPORT_SYMBOL(waitq_wait);

int64_t
waitq_timedwait(wait_queue_t *link, cfs_task_state_t state,
		    int64_t timeout)
{
	return schedule_timeout(timeout);
}
EXPORT_SYMBOL(waitq_timedwait);

void
schedule_timeout_and_set_state(cfs_task_state_t state, int64_t timeout)
{
	set_current_state(state);
	schedule_timeout(timeout);
}
EXPORT_SYMBOL(schedule_timeout_and_set_state);

/* deschedule for a bit... */
void
cfs_pause(cfs_duration_t ticks)
{
	set_current_state(TASK_UNINTERRUPTIBLE);
	schedule_timeout(ticks);
}
EXPORT_SYMBOL(cfs_pause);

void cfs_init_timer(timer_list_t *t)
{
	init_timer(t);
}
EXPORT_SYMBOL(cfs_init_timer);

void cfs_timer_init(timer_list_t *t, cfs_timer_func_t *func, void *arg)
{
	init_timer(t);
	t->function = func;
	t->data = (unsigned long)arg;
}
EXPORT_SYMBOL(cfs_timer_init);

void cfs_timer_done(timer_list_t *t)
{
	return;
}
EXPORT_SYMBOL(cfs_timer_done);

void cfs_timer_arm(timer_list_t *t, cfs_time_t deadline)
{
	mod_timer(t, deadline);
}
EXPORT_SYMBOL(cfs_timer_arm);

void cfs_timer_disarm(timer_list_t *t)
{
	del_timer(t);
}
EXPORT_SYMBOL(cfs_timer_disarm);

int  cfs_timer_is_armed(timer_list_t *t)
{
	return timer_pending(t);
}
EXPORT_SYMBOL(cfs_timer_is_armed);

cfs_time_t cfs_timer_deadline(timer_list_t *t)
{
	return t->expires;
}
EXPORT_SYMBOL(cfs_timer_deadline);

void cfs_enter_debugger(void)
{
#if defined(CONFIG_KGDB)
//	BREAKPOINT();
#else
	/* nothing */
#endif
}


sigset_t
cfs_block_allsigs(void)
{
	unsigned long	  flags;
	sigset_t	old;

	SIGNAL_MASK_LOCK(current, flags);
	old = current->blocked;
	sigfillset(&current->blocked);
	recalc_sigpending();
	SIGNAL_MASK_UNLOCK(current, flags);

	return old;
}

sigset_t cfs_block_sigs(unsigned long sigs)
{
	unsigned long  flags;
	sigset_t	old;

	SIGNAL_MASK_LOCK(current, flags);
	old = current->blocked;
	sigaddsetmask(&current->blocked, sigs);
	recalc_sigpending();
	SIGNAL_MASK_UNLOCK(current, flags);
	return old;
}

/* Block all signals except for the @sigs */
sigset_t cfs_block_sigsinv(unsigned long sigs)
{
	unsigned long flags;
	sigset_t old;

	SIGNAL_MASK_LOCK(current, flags);
	old = current->blocked;
	sigaddsetmask(&current->blocked, ~sigs);
	recalc_sigpending();
	SIGNAL_MASK_UNLOCK(current, flags);

	return old;
}

void
cfs_restore_sigs (sigset_t old)
{
	unsigned long  flags;

	SIGNAL_MASK_LOCK(current, flags);
	current->blocked = old;
	recalc_sigpending();
	SIGNAL_MASK_UNLOCK(current, flags);
}

int
cfs_signal_pending(void)
{
	return signal_pending(current);
}

void
cfs_clear_sigpending(void)
{
	unsigned long flags;

	SIGNAL_MASK_LOCK(current, flags);
	clear_tsk_thread_flag(current, TIF_SIGPENDING);
	SIGNAL_MASK_UNLOCK(current, flags);
}

int
libcfs_arch_init(void)
{
	return 0;
}

void
libcfs_arch_cleanup(void)
{
	return;
}

EXPORT_SYMBOL(libcfs_arch_init);
EXPORT_SYMBOL(libcfs_arch_cleanup);
EXPORT_SYMBOL(cfs_enter_debugger);
EXPORT_SYMBOL(cfs_block_allsigs);
EXPORT_SYMBOL(cfs_block_sigs);
EXPORT_SYMBOL(cfs_block_sigsinv);
EXPORT_SYMBOL(cfs_restore_sigs);
EXPORT_SYMBOL(cfs_signal_pending);
EXPORT_SYMBOL(cfs_clear_sigpending);
