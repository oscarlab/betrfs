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

#include <toku_portability.h>
#include "minicron.h"
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <toku_htonl.h>
#include <toku_assert.h>

#include <stdio.h>
#include <memory.h>
#include <string.h>
#include <portability/toku_path.h>

#include "ft.h"
#include "key.h"
#include "block_table.h"
#include "log-internal.h"
#include "logger.h"
#include "fttypes.h"
#include "ft-ops.h"
#include "cachetable.h"
#include "cachetable-internal.h"


int verbose1 = 1;
struct timeval starttime;


static unsigned long
tdiff (struct timeval *a, struct timeval *b) {
    return (a->tv_sec-b->tv_sec) * 1e6 + (a->tv_usec-b->tv_usec);
}


static unsigned long elapsed (void) {
    struct timeval now;
    gettimeofday(&now, 0);
    return tdiff(&now, &starttime);
}

static int
#ifndef GCOV
__attribute__((__noreturn__))
#endif
never_run (void *a) {
    assert(a==0);
    assert(0);
#if TOKU_WINDOWS || defined(GCOV)
    return 0; //ICC ignores the noreturn attribute.
#endif
}

static void* test1 (void* v)
{
    struct minicron m;
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 0, never_run, 0);   assert(r==0);
    sleep(1);
    r = toku_minicron_shutdown(&m);                     assert(r==0);
    return v;
}

static void* test2 (void* v)
{
    struct minicron m;
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 10000, never_run, 0);   assert(r==0);
    sleep(2);
    r = toku_minicron_shutdown(&m);                     assert(r==0);
    return v;
}

struct tenx {
    struct timeval tv;
    int counter;
};

static int run_5x (void *v)
{
    struct tenx *CAST_FROM_VOIDP(tx, v);
    struct timeval now;
    gettimeofday(&now, 0);

    unsigned long diff = tdiff(&now, &tx->tv);
    if (verbose1) printf(  "T=%lu tx->counter=%d\n", diff, tx->counter);
    if (!(diff>500000UL + (unsigned long)tx->counter)) {
      printf(  "T=%lu tx->counter=%d\n", diff, tx->counter);
      assert(0);
    }

    tx->counter++;
    return 0;
}

static void* test3 (void* v)
{
    struct minicron m;
    struct tenx tx;
    gettimeofday(&tx.tv, 0);
    tx.counter=0;
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 1000, run_5x, &tx);   assert(r==0);
    sleep(5);
    r = toku_minicron_shutdown(&m);                     assert(r==0);
    assert(tx.counter>=4 && tx.counter<=5);
    return v;
}

struct run_3sec_arg {
    int counter;
    const char *fn_name;
};

static int run_3sec (void *v)
{
    struct run_3sec_arg *CAST_FROM_VOIDP(arg, v);
    if (verbose1) printf(  "start3sec at %lu, fn=%s\n", elapsed(), arg->fn_name);
    arg->counter++;
    sleep(3);
    if (verbose1) printf(  "end3sec at %lu, counter=%d, fn=%s\n", elapsed(), arg->counter, arg->fn_name);
    return 0;
}

static void* test4 (void *v)
{
    struct minicron m;
    struct run_3sec_arg arg = {0, __func__};
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 2000, run_3sec, &arg); assert(r==0);
    sleep(11);

    if (verbose1) printf(  "test4 shutdown at %lu\n", elapsed());

    r = toku_minicron_shutdown(&m);                     assert(r==0);
    // The 3rd run_3sec is supposed to complete at 11 sec.
    // The 4th execution can be launched if minicron_shutdown
    // is scheduled to run after that. Eventually, we get value 4.
    if (arg.counter != 3 && arg.counter != 4) {
        printf("%s: counter: %d\n", __func__, arg.counter);
        assert(false);
    }
    return v;
}

static void* test5 (void *v)
{
    struct minicron m;
    struct run_3sec_arg arg = {0, __func__};
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 10000, run_3sec, &arg); assert(r==0);
    toku_minicron_change_period(&m, 2000);
    sleep(10);
    r = toku_minicron_shutdown(&m);                     assert(r==0);
    assert(arg.counter==3);
    return v;
}

static void* test6 (void *v)
{
    struct minicron m;
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 5000, never_run, 0); assert(r==0);
    toku_minicron_change_period(&m, 0);
    sleep(7);
    r = toku_minicron_shutdown(&m);                          assert(r==0);
    return v;
}

static void* test7 (void *v)
{
    struct minicron m;
    struct run_3sec_arg arg = {0, __func__};
    ZERO_STRUCT(m);
    int r = toku_minicron_setup(&m, 5000, run_3sec, &arg); assert(r==0);
    sleep(17);
    r = toku_minicron_shutdown(&m);                     assert(r==0);
    assert(arg.counter==3);
    return v;
}


typedef void*(*ptf)(void*);

extern "C" int test_minicron(void);

//int test_minicron2 (void);
int test_minicron (void)
{
    gettimeofday(&starttime, 0);
    ptf testfuns[] = {test1,
                      test2,
                      test3,
                      test4,
                      test5,
                      test6,
                      test7
    };
#define N (sizeof(testfuns)/sizeof(testfuns[0]))
    toku_pthread_t tests[N];

    unsigned int i;
    for (i=0; i<N; i++) {
        int r=toku_pthread_create(tests+i, 0, testfuns[i], 0);
        assert(r==0);
    }
    for (i=0; i<N; i++) {
        void *v;
        int r=toku_pthread_join(tests[i], &v);
        assert(r==0);
        assert(v==0);
    }

    return 0;
}

