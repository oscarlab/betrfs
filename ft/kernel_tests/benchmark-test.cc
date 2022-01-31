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

#ident "Copyright (c) 2007, 2008 Tokutek Inc.  All rights reserved."

#include "test_benchmark.h"

/* Insert a bunch of stuff */
#include <toku_time.h>
static int verbose = 1;
static const char *fname = NULL;

enum { SERIAL_SPACING = 1<<6 };
int64_t ITEMS_TO_INSERT_PER_ITERATION = 1<<20;
int64_t BOUND_INCREASE_PER_ITERATION = SERIAL_SPACING*ITEMS_TO_INSERT_PER_ITERATION;

enum { NODE_SIZE = 1<<20 };
enum { BASEMENT_NODE_SIZE = 128 * 1024 };

static int nodesize = NODE_SIZE;
static int basementnodesize = BASEMENT_NODE_SIZE;
static enum toku_compression_method compression_method = TOKU_DEFAULT_COMPRESSION_METHOD;
static int keysize = sizeof (long long);
static int valsize = sizeof (long long);
static int do_verify =0; /* Do a slow verify after every k inserts. */
static int verify_period = 256; /* how many inserts between verifies. */

static int do_serial = 1;
static int do_random = 1;

static CACHETABLE ct;
static FT_HANDLE t;

static void setup (void) {
    int r;
    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &t, nodesize, basementnodesize, compression_method, ct, NULL_TXN, toku_builtin_compare_fun); assert(r==0);
}

static void toku_shutdown (void) {
    int r;
    r = toku_close_ft_handle_nolsn(t, 0); assert(r==0);
    toku_cachetable_close(&ct);
}

static void long_long_to_array (unsigned char *a, unsigned long long l) {
    int i;
    for (i=0; i<8; i++)
	a[i] = (l>>(56-8*i))&0xff;
}

static void insert (long long v) {
    unsigned char kc[keysize], vc[valsize];
    DBT  kt, vt;
    memset(kc, 0, sizeof kc);
    long_long_to_array(kc, v);
    memset(vc, 0, sizeof vc);
    long_long_to_array(vc, v);
    toku_ft_insert(t, toku_fill_dbt(&kt, kc, keysize), toku_fill_dbt(&vt, vc, valsize), 0);
    if (do_verify) {
        static int inserts_since_last_verify = 0;
        inserts_since_last_verify++;
        if (inserts_since_last_verify % verify_period == 0) {
            toku_cachetable_verify(ct);
        }
    }
}

static void serial_insert_from (long long from) {
    long long i;
    for (i=0; i<ITEMS_TO_INSERT_PER_ITERATION; i++) {
	insert((from+i)*SERIAL_SPACING);
    }
}

static long long llrandom (void) {
    return (((long long)(random()))<<32) + random();
}

static void random_insert_below (long long below) {
    long long i;
    assert(0 < below);
    for (i=0; i<ITEMS_TO_INSERT_PER_ITERATION; i++) {
	insert(llrandom()%below);
    }
}

static void biginsert (long long n_elements, struct timeval *starttime) {
    long long i;
    struct timeval t1,t2;
    int iteration;
    printf("\nstart big insert \n");
    for (i=0, iteration=0; i<n_elements; i+=ITEMS_TO_INSERT_PER_ITERATION, iteration++) {
	gettimeofday(&t1,0);
	if (do_serial)
            serial_insert_from(i);
	gettimeofday(&t2,0);
	if (verbose && do_serial) {
	    printf("serial %" PRIu64 "s %" PRIu64 "/s    ", toku_tdiff(&t2, &t1), ITEMS_TO_INSERT_PER_ITERATION/toku_tdiff(&t2, &t1));
	    fflush(stdout);
	}
	gettimeofday(&t1,0);
        if (do_random)
            random_insert_below((i+ITEMS_TO_INSERT_PER_ITERATION)*SERIAL_SPACING);
	gettimeofday(&t2,0);
	if (verbose && do_random) {
	    printf("random %" PRIu64 "s %" PRIu64 "/s    ", toku_tdiff(&t2, &t1), ITEMS_TO_INSERT_PER_ITERATION/toku_tdiff(&t2, &t1));
	    fflush(stdout);
        }
        if (verbose && (do_serial || do_random)) {
            uint64_t f = 0;
            if (do_serial) f += 1.0;
            if (do_random) f += 1.0;
	    printf("cumulative %" PRIu64 "s %" PRIu64 "/s\n", toku_tdiff(&t2, starttime), (ITEMS_TO_INSERT_PER_ITERATION*f/toku_tdiff(&t2, starttime))*(iteration+1));
	    fflush(stdout);
	}
    }
}


extern "C" int test_benchmark_test(void);

int
test_benchmark_test (void) {
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);

    verbose=1; //Default
    /* parse parameters */
    fname = TOKU_TEST_FILENAME_DATA;

    struct timeval t1,t2,t3;
    long long total_n_items = 1LL<<6; // 1LL<<16
    if (verbose) {
	printf("nodesize=%d\n", nodesize);
	printf("keysize=%d\n", keysize);
	printf("valsize=%d\n", valsize);
	printf("Serial and random insertions of %" PRId64 " per batch\n", ITEMS_TO_INSERT_PER_ITERATION);
        fflush(stdout);
    }
    setup();
    printf("\nsuccessfully setup\n");
    gettimeofday(&t1,0);
    biginsert(total_n_items, &t1);
    printf("\n successfully biginsert\n");
    gettimeofday(&t2,0);
    printf("\n successfully shutdown\n");
    toku_shutdown();
    gettimeofday(&t3,0);
    if (verbose) {
        int f = 0;
        if (do_serial) f += 1;
        if (do_random) f += 1;
	printf("Shutdown %" PRIu64 "s\n", toku_tdiff(&t3, &t2));
	printf("Total time %" PRIu64 "s for %lld insertions = %lld/s\n", toku_tdiff(&t3, &t1), f*total_n_items, f*total_n_items/toku_tdiff(&t3, &t1));
        fflush(stdout);
    }
    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);

#ifdef __SUPPORT_DIRECT_IO
    printf("\n INFO: direct io is turned on \n");
#endif

    toku_ft_layer_destroy();

    return 0;
}

