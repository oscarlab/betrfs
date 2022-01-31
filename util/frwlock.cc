/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*
COPYING CONDITIONS NOTICE:

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation, and provided that the
  following conditions are met:

      * Redistributions of source code must retain this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below).

      * Redistributions in binary form must reproduce this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below) in the documentation and/or other materials
        provided with the distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301, USA.

COPYRIGHT NOTICE:

  TokuDB, Tokutek Fractal Tree Indexing Library.
  Copyright (C) 2007-2013 Tokutek, Inc.

DISCLAIMER:

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

UNIVERSITY PATENT NOTICE:

  The technology is licensed by the Massachusetts Institute of
  Technology, Rutgers State University of New Jersey, and the Research
  Foundation of State University of New York at Stony Brook under
  United States of America Serial No. 11/760379 and to the patents
  and/or patent applications resulting from it.

PATENT MARKING NOTICE:

  This software is covered by US Patent No. 8,185,551.
  This software is covered by US Patent No. 8,489,638.

PATENT RIGHTS GRANT:

  "THIS IMPLEMENTATION" means the copyrightable works distributed by
  Tokutek as part of the Fractal Tree project.

  "PATENT CLAIMS" means the claims of patents that are owned or
  licensable by Tokutek, both currently or in the future; and that in
  the absence of this license would be infringed by THIS
  IMPLEMENTATION or by using or running THIS IMPLEMENTATION.

  "PATENT CHALLENGE" shall mean a challenge to the validity,
  patentability, enforceability and/or non-infringement of any of the
  PATENT CLAIMS or otherwise opposing any of the PATENT CLAIMS.

  Tokutek hereby grants to you, for the term and geographical scope of
  the PATENT CLAIMS, a non-exclusive, no-charge, royalty-free,
  irrevocable (except as stated in this section) patent license to
  make, have made, use, offer to sell, sell, import, transfer, and
  otherwise run, modify, and propagate the contents of THIS
  IMPLEMENTATION, where such license applies only to the PATENT
  CLAIMS.  This grant does not include claims that would be infringed
  only as a consequence of further modifications of THIS
  IMPLEMENTATION.  If you or your agent or licensee institute or order
  or agree to the institution of patent litigation against any entity
  (including a cross-claim or counterclaim in a lawsuit) alleging that
  THIS IMPLEMENTATION constitutes direct or contributory patent
  infringement, or inducement of patent infringement, then any rights
  granted to you under this License shall terminate as of the date
  such litigation is filed.  If you or your agent or exclusive
  licensee institute or order or agree to the institution of a PATENT
  CHALLENGE, then Tokutek may terminate any rights granted to you
  under this License.
*/

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."

#include <toku_assert.h>
#include <util/frwlock.h>
namespace toku {

void frwlock::init(toku_mutex_t *const mutex) {
    m_mutex = mutex;

    m_num_readers = 0;
    m_num_writers = 0;
    m_num_want_write = 0;
    m_num_want_read = 0;
    m_num_expensive_want_write = 0;
    m_current_writer_expensive = false;
    m_read_wait_expensive = false;
    fair_lock_init();
    fair_lock_set_mutex(mutex);
}

void frwlock::deinit(void) {
    fair_lock_deinit();
}

void frwlock::fair_lock_set_mutex(toku_mutex_t* const mutex) {
    toku_pthread_rwlock_set_mutex(&m_fair_lock, mutex);
}
void frwlock::fair_lock_init(void) {

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
#if defined(HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP)
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#else
    // TODO: need to figure out how to make writer-preferential rwlocks
    // happen on osx
#endif
    toku_pthread_rwlock_init(&m_fair_lock, &attr);
    pthread_rwlockattr_destroy(&attr);
}

void frwlock::fair_lock_deinit(void) {
    toku_pthread_rwlock_destroy(&m_fair_lock);
}
// Prerequisite: Holds m_mutex.
void frwlock::write_lock(bool expensive) {
    toku_mutex_assert_locked(m_mutex);
    if (this->try_write_lock(expensive)) {
        return;
    }

    ++m_num_want_write;
    if(expensive) {
	++m_num_expensive_want_write;
    }
    toku_pthread_rwlock_wrlock(&m_fair_lock);
    // Now it's our turn.
    paranoid_invariant(m_num_want_write > 0);
    paranoid_invariant_zero(m_num_readers);
    paranoid_invariant_zero(m_num_writers);

    // Not waiting anymore; grab the lock.
    --m_num_want_write;
    if (expensive) {
        --m_num_expensive_want_write;
    }
    m_num_writers = 1;
    m_current_writer_expensive = expensive;
}

bool frwlock::try_write_lock(bool expensive) {
    toku_mutex_assert_locked(m_mutex);
    if (m_num_readers > 0 || m_num_writers > 0 ||  m_num_want_write > 0) {
        return false;
    }
   //TODO:try lock with rw_sem return 1 if succeeds
	
    int r = toku_pthread_rwlock_try_wrlock(&m_fair_lock);
    if(!r) return false;
    //No one holds the lock.  Grant the write lock.
    paranoid_invariant_zero(m_num_want_write);
    //paranoid_invariant_zero(m_num_want_read);
    m_num_writers = 1;
    m_current_writer_expensive = expensive;
    return true;
}

void frwlock::read_lock(void) {
    	toku_mutex_assert_locked(m_mutex);
        m_read_wait_expensive = (m_current_writer_expensive ||
                (m_num_expensive_want_write > 0));

        // Wait for our turn.
        ++m_num_want_read;
	toku_pthread_rwlock_rdlock(&m_fair_lock);
        paranoid_invariant(m_num_want_read > 0);
        paranoid_invariant_zero(m_num_writers);

        // Not waiting anymore; grab the lock.
        --m_num_want_read;
    	++m_num_readers;
}

bool frwlock::try_read_lock(void) {
    toku_mutex_assert_locked(m_mutex);
    if (m_num_writers > 0 || m_num_want_write > 0) {
//	printf("m_num_writers=%d, m_num_want_write=%d\n",m_num_writers, m_num_want_write);
        return false;
    }
    int r = toku_pthread_rwlock_try_rdlock(&m_fair_lock);
  //  printf("r=%d\n", r);
    if(!r) return false;  
 // No writer holds the lock.
    // No writers are waiting.
    // Grant the read lock.
    ++m_num_readers;
    return true;
}

void frwlock::read_unlock(void) {
    toku_mutex_assert_locked(m_mutex);
    paranoid_invariant(m_num_writers == 0);
    paranoid_invariant(m_num_readers > 0);
    --m_num_readers;
    toku_pthread_rwlock_rdunlock(&m_fair_lock);
}

bool frwlock::read_lock_is_expensive(void) {
    toku_mutex_assert_locked(m_mutex);
    return m_current_writer_expensive || (m_num_expensive_want_write > 0);
}

void frwlock::write_unlock(void) {
    toku_mutex_assert_locked(m_mutex);
    paranoid_invariant(m_num_writers == 1);
    m_num_writers = 0;
    m_current_writer_expensive = false;
    toku_pthread_rwlock_wrunlock(&m_fair_lock);
}
bool frwlock::write_lock_is_expensive(void) {
    toku_mutex_assert_locked(m_mutex);
    return (m_num_expensive_want_write > 0) || (m_current_writer_expensive);
}


uint32_t frwlock::users(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_readers + m_num_writers + m_num_want_read + m_num_want_write;
}

uint32_t frwlock::writers(void) const {
    // this is sometimes called as "assert(lock->writers())" when we
    // assume we have the write lock.  if that's the assumption, we may
    // not own the mutex, so we don't assert_locked here
    return m_num_writers;
}

uint32_t frwlock::readers(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_readers;
}


} // namespace toku
