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

#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include "test.h"
#include "helper.h"

#ifndef MIN
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#endif
//const double USECS_PER_SEC = 1000000.0;

static int
long_key_cmp(DB *UU(e), const DBT *a, const DBT *b)
{
    const long *CAST_FROM_VOIDP(x, a->data);
    const long *CAST_FROM_VOIDP(y, b->data);
    return (*x > *y) - (*x < *y);
}

static void
run_test(unsigned long eltsize, unsigned long nodesize, unsigned long repeat)
{
    int cur = 0;
    int r = 0;
    long *keys = (long *)toku_xmalloc(1024 * sizeof(long));
    char **vals = (char **)toku_xmalloc(1024 * sizeof(char*));
    long long unsigned nbytesinserted = 0;
    struct timeval t[2];
    XIDS xids_0;
    XIDS xids_123;
    NONLEAF_CHILDINFO bnc;
    long int i = 0;

    assert(keys);
    assert(vals);

    for (i = 0; i < 1024; ++i) {
        keys[i] = (long) rand();

        vals[i] = (char *) toku_xmalloc(eltsize - sizeof(keys[i]));
        assert(vals[i]);

        unsigned int long j = 0;
        char *val = vals[i];
        for (;j< eltsize - sizeof(keys[i]) - sizeof(int); j+=sizeof(int)) {
            int *p = (int *) &val[j];
            *p = (int) rand();
        }
        for (; j < eltsize - sizeof(keys[i]); ++j) {
            char *p = &val[j];
            *p =  (char) (rand() & 0xff);
        }
    }

    xids_0 = xids_get_root_xids();
    r = xids_create_child(xids_0, &xids_123, (TXNID)123);
    CKERR(r);
    gettimeofday(&t[0], NULL);

    INT(r);
    DBG;
    assert_zero(r);

    for (unsigned int j = 0; j < repeat; ++j) {
        bnc = toku_create_empty_nl();
        for (; toku_bnc_nbytesinbuf(bnc) <= nodesize; ++cur) {
            FT_MSG_S msg;
            DBT key, val;
            toku_fill_dbt(&key, &keys[cur % 1024], sizeof keys[cur % 1024]);
            toku_fill_dbt(&val,vals[cur % 1024], eltsize - (sizeof keys[cur % 1024]));
            ft_msg_init(&msg, FT_NONE, next_dummymsn(), xids_123, &key, &val);

            toku_bnc_insert_msg(bnc,
                                NULL,
                                &msg,
                                true,
                                NULL,
                                long_key_cmp);
            assert_zero(r);
        }
        nbytesinserted += toku_bnc_nbytesinbuf(bnc);
        destroy_nonleaf_childinfo(bnc);
    }


    xids_destroy(&xids_123);

    for (i=0; i< 1024; i++) {
	if(vals[i] != NULL)
		toku_free(vals[i]);
    }
    toku_free(vals);
    toku_free(keys);
}

extern "C" int test_bnc_insert_benchmark(void);

int test_bnc_insert_benchmark()
{
    unsigned long eltsize, nodesize, repeat;

    initialize_dummymsn();
    eltsize = 2048;
    nodesize = 2048;
    repeat = 100;

    run_test(eltsize, nodesize, repeat);

    return 0;
}
