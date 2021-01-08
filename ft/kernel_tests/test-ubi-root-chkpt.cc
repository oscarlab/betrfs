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

  TokuFT, Tokutek Fractal Tree Indexing Library.
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
/* The goal of this test.  Make sure that inserts stay behind deletes. */


#include "test_benchmark.h"
#include "ft/checkpoint.h"
#include "test-ft-txns.h"

static TOKUTXN const null_txn = 0;

enum { NODESIZE = 1024, KSIZE=NODESIZE-100, TOKU_PSIZE=20 };

static CACHETABLE ct;
static FT_HANDLE t;
static TOKULOGGER logger;
static const char *fname;

static void
doit (void) {
    BLOCKNUM nodea,nodeb;
    int r;

    //toku_cachetable_create(&ct, 16*1024, ZERO_LSN, nullptr);
    r=test_setup(TOKU_TEST_ENV_DIR_NAME, &logger, &ct);
    assert(r==0);
    r = toku_open_ft_handle(fname, 1, &t, NODESIZE, NODESIZE, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert(r==0);

    toku_testsetup_initialize();  // must precede any other toku_testsetup calls

    r = toku_testsetup_leaf(t, &nodea, 1, NULL, NULL);
    assert(r==0);

    r = toku_testsetup_nonleaf(t, 1, &nodeb, 1, &nodea, 0, 0);
    assert(r==0);

    r = toku_testsetup_root(t, nodeb);
    assert(r==0);
    //insert key-value pair in the range [a - e]
    DBT k,v;

    TOKUTXN txn = NULL;
    r = toku_txn_begin_txn(
        (DB_TXN *)NULL,
        NULL,
        &txn,
        logger,
        TXN_SNAPSHOT_CHILD,
        false
        );
    CKERR(r);
    toku_ft_seq_insert(t,
                   toku_fill_dbt(&k, "a", 2),
                   toku_fill_dbt(&v, "aa", 3),
                   txn);
    toku_ft_seq_insert(t,
                   toku_fill_dbt(&k, "b", 2),
                   toku_fill_dbt(&v, "bb", 3),
                   txn);
    toku_ft_seq_insert(t,
                   toku_fill_dbt(&k, "c", 2),
                   toku_fill_dbt(&v, "cc", 3),
                   txn);
    toku_ft_seq_insert(t,
                   toku_fill_dbt(&k, "d", 2),
                   toku_fill_dbt(&v, "dd", 3),
                   txn);
    toku_ft_seq_insert(t,
                   toku_fill_dbt(&k, "e", 2),
                   toku_fill_dbt(&v, "ee", 3),
                   txn);

    r = toku_txn_commit_txn(txn, true, NULL, NULL);
    CKERR(r);
    toku_txn_close_txn(txn);

    
    // now run a checkpoint to get everything clean
    CHECKPOINTER cp = toku_cachetable_get_checkpointer(ct);
    r = toku_checkpoint(cp, NULL, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT, false);
    assert_zero(r);

     // close the ft-handle and cachetable
    r = toku_close_ft_handle_nolsn(t, 0);    assert(r==0);
    toku_cachetable_close(&ct);
}

extern "C" int test_ubi_root_chkpt(void);
int
test_ubi_root_chkpt (void) {
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);
    fname = TOKU_TEST_FILENAME_DATA;
    doit();
    toku_ft_layer_destroy();
    return 0;
}

