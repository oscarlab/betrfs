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

static int verbose = 1;
// create a brt and put n rows into it
// write the brt to the file
// verify the rows in the brt
static void test_sub_block(int n) {
    if (verbose) printf("%s:%d %d\n", __FUNCTION__, __LINE__, n);

    const char *fname = TOKU_TEST_FILENAME_DATA;
    const int nodesize = 4*1024*1024;
    const int basementnodesize = 128*1024;
    const enum toku_compression_method compression_method = TOKU_DEFAULT_COMPRESSION_METHOD;

    TOKUTXN const null_txn = 0;

    int error;
    CACHETABLE ct;
    FT_HANDLE brt;
    int i;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);

    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);

    error = toku_open_ft_handle(fname, true, &brt, nodesize, basementnodesize, compression_method, ct, null_txn, toku_builtin_compare_fun);
    assert(error == 0);

    // insert keys 0, 1, 2, .. (n-1)
    for (i=0; i<n; i++) {
        int k = toku_htonl(i);
        int v = i;
	DBT key, val;
        toku_fill_dbt(&key, &k, sizeof k);
        toku_fill_dbt(&val, &v, sizeof v);
        toku_ft_insert(brt, &key, &val, 0);
        assert(error == 0);
    }

    // write to the file
    error = toku_close_ft_handle_nolsn(brt, 0);
    assert(error == 0);

    // verify the brt by walking a cursor through the rows
    error = toku_open_ft_handle(fname, false, &brt, nodesize, basementnodesize, compression_method, ct, null_txn, toku_builtin_compare_fun);
    assert(error == 0);

    FT_CURSOR cursor;
    error = toku_ft_cursor(brt, &cursor, NULL, false, false);
    assert(error == 0);

    for (i=0; ; i++) {
        int k = htonl(i);
        int v = i;
	struct check_pair pair = {sizeof k, &k, sizeof v, &v, 0};
        error = toku_ft_cursor_get(cursor, NULL, lookup_checkf, &pair, DB_NEXT);
        if (error != 0) {
	    assert(pair.call_count==0);
            break;
	}
	assert(pair.call_count==1);
    }
    assert(i == n);

    toku_ft_cursor_close(cursor);

    error = toku_close_ft_handle_nolsn(brt, 0);
    assert(error == 0);

    toku_cachetable_close(&ct);
}

extern "C" int test_ft_serialize_sub_block(void);

int test_ft_serialize_sub_block(void) {
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);

    const int meg = 1024*1024;
    const int row = 32;
    const int rowspermeg = meg/row;

    test_sub_block(1);
    test_sub_block(rowspermeg-1);
    int i;
    for (i=1; i<8; i++)
        test_sub_block(rowspermeg*i);

    if (verbose) printf("test ok\n");
    toku_ft_layer_destroy();
    #ifdef __SUPPORT_DIRECT_IO
    printf("\n INFO: direct io is turned on \n");
    #endif

    return 0;
}
