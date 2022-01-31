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

/* Test recovery for range cast messages
 * This is basic recovery test for range delete.
 * Create a file using txn and then commit it.
 * Start another txn and then enq_insert keys in the range [a - e].
 * enq a delete multicast message and then commit the txn.
 * close the logger.
 * Now recover using toku_recover. */

#include "test.h"


static int 
run_test(void) {
    int r;

    const int FSYNC = 1;
    const int NO_FSYNC = 0;

    LSN lsn = {0};
    TXNID_PAIR txnid = {.parent_id64 = TXNID_NONE, .child_id64 = TXNID_NONE};
    LSN beginlsn;

    BYTESTRING bs_name = { (uint32_t) strlen("a.db"), (char *) "a.db" };
    const FILENUM fn_name = {0};
     
    // setup the test dir
    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    // create/open the logger
    TOKULOGGER logger;
    r = toku_logger_create(&logger); assert(r == 0);
    r = toku_logger_open(TOKU_TEST_ENV_DIR_NAME, logger); assert(r == 0);
    
    // create a range of key-value pairs
    BYTESTRING a  = { (uint32_t) strlen("a"), (char *) "a" };
    BYTESTRING aa  = { (uint32_t) strlen("aa"), (char *) "aa" };
    BYTESTRING b  = { (uint32_t) strlen("b"), (char *) "b" };
    BYTESTRING bb  = { (uint32_t) strlen("bb"), (char *) "bb" };
    BYTESTRING c  = { (uint32_t) strlen("c"), (char *) "c" };
    BYTESTRING cc  = { (uint32_t) strlen("cc"), (char *) "cc" };
    BYTESTRING d  = { (uint32_t) strlen("d"), (char *) "d" };
    BYTESTRING dd  = { (uint32_t) strlen("dd"), (char *) "dd" };
    BYTESTRING e  = { (uint32_t) strlen("e"), (char *) "e" };
    BYTESTRING ee  = { (uint32_t) strlen("ee"), (char *) "ee" };
    BYTESTRING f  = { (uint32_t) strlen("f"), (char *) "f" };
    BYTESTRING ff  = { (uint32_t) strlen("ff"), (char *) "ff" };

    /* create a file to insert and delete BYTESTRING */
    // xbegin
    txnid.parent_id64 = 1;
    toku_log_xbegin(logger, &lsn, NO_FSYNC, txnid, TXNID_PAIR_NONE);
    // fcreate
    toku_log_fcreate(logger, &lsn, NO_FSYNC, NULL, txnid, fn_name, bs_name, 0x0777, 0, 0, TOKU_DEFAULT_COMPRESSION_METHOD, 0);
    // commit
    toku_log_xcommit(logger, &lsn, FSYNC, NULL, txnid);

    // enq the key value pairs
    txnid.parent_id64 = 4;
    toku_log_xbegin(logger, &lsn, NO_FSYNC, txnid, TXNID_PAIR_NONE);
    
    toku_log_enq_insert(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, a, aa);
    toku_log_enq_insert(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, b, bb);
    toku_log_enq_insert(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, c, cc);
    toku_log_enq_insert(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, d, dd);
    toku_log_enq_insert(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, e, ee);
    toku_log_enq_insert(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, f, ff);
    
    // enq a delete multicast
    // TODO: is_resetting is "false". Don't know what that implies
    toku_log_enq_delete_multi(logger, &lsn, NO_FSYNC, NULL, fn_name, txnid, b, d, false,PM_UNCOMMITTED,false);
    
    // commit
    toku_log_xcommit(logger, &lsn, FSYNC, NULL, txnid);
    
    // checkpoint 
    toku_log_begin_checkpoint(logger, &beginlsn, true, 0, 0);
    toku_log_end_checkpoint(logger, NULL, true, beginlsn, 0, 0, 0); 
    
    // close the logger
    r = toku_logger_close(&logger); assert(r == 0);

    // redirect stderr
   #if 0 
   int devnul = open(DEV_NULL_FILE, O_WRONLY);
    assert(devnul>=0);
    r = toku_dup2(devnul, fileno(stderr)); 	    assert(r==fileno(stderr));
    r = close(devnul);                      assert(r==0);
   #endif
    // run recovery
    struct toku_db_key_operations dummy_ftfs_key_ops;
    memset(&dummy_ftfs_key_ops, 0, sizeof(dummy_ftfs_key_ops));

    r = tokudb_recover(NULL,
		       NULL_prepared_txn_callback,
		       NULL_keep_cachetable_callback,
		       NULL_logger, TOKU_TEST_ENV_DIR_NAME, TOKU_TEST_ENV_DIR_NAME, &dummy_ftfs_key_ops, 0, 0, NULL, 0); 
    assert(r == 0);

    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    return 0;
}

extern "C" int test_recovery_range_delete(void);
int
test_recovery_range_delete(void) {
    int r;
    initialize_dummymsn();
    r = toku_ft_layer_init();
    CKERR(r);
    r = run_test();
    toku_ft_layer_destroy();
    return r;
}
