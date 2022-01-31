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
// test recovery of "hello" comments

#include "test.h"
#include "checkpoint.h"
#include "helper.h"
#include "logsuperblock.h"

static int 
run_test(void) {
    // leave this many bytes in file
    const int magic_sz =
                                         sizeof(struct log_super_block)
                                        +toku_log_begin_checkpoint_overhead
                                        +toku_log_end_checkpoint_overhead
                                        +toku_log_comment_overhead;

    int r;
    int trim = 1;

    while ( 1 ) {
        // setup the test dir
        r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

        // create the log
        TOKULOGGER logger;
        BYTESTRING hello = { (uint32_t) strlen("hello"), (char *) "hello" };
        BYTESTRING world = { (uint32_t) strlen("world"), (char *) "world" };
        BYTESTRING there = { (uint32_t) strlen("there"), (char *) "there" };
        r = toku_logger_create(&logger); assert(r == 0);
        r = toku_logger_open(TOKU_TEST_ENV_DIR_NAME, logger); assert(r == 0);
        LSN beginlsn;
        // all logs must contain a valid checkpoint
        toku_log_begin_checkpoint(logger, &beginlsn, true, 0, 0);
        toku_log_end_checkpoint(logger, NULL, true, beginlsn, 0, 0, 0);
        toku_log_comment(logger, NULL, true, 0, hello);
        toku_log_comment(logger, NULL, true, 0, world);
        toku_log_comment(logger, NULL, true, 0, hello);
        toku_log_comment(logger, NULL, true, 0, there);
        toku_logger_close(&logger);

        char fname[TOKU_PATH_MAX+1];
        sprintf(fname, "%s/%s%d", TOKU_TEST_ENV_DIR_NAME, "log000000000000.tokulog", TOKU_LOG_VERSION);

        int fd = open(fname, O_RDWR);
        assert(fd >= 0);
        uint32_t log_size;
        r = toku_verify_logmagic_read_log_end(fd, &log_size);
        assert(r==0);
        if ( log_size - trim > magic_sz ) {
            r = toku_update_logfile_end(fd, log_size - trim);
            CKERR(r);
            close(fd);
        } else {
            close(fd);
            break;
        }

        // run recovery
        struct toku_db_key_operations dummy_ftfs_key_ops;
        memset(&dummy_ftfs_key_ops, 0, sizeof(dummy_ftfs_key_ops));

        r = tokudb_recover(NULL,
			   NULL_prepared_txn_callback,
			   NULL_keep_cachetable_callback,
			   NULL_logger,
			   TOKU_TEST_ENV_DIR_NAME, 
                           TOKU_TEST_ENV_DIR_NAME, &dummy_ftfs_key_ops, 0, 0, NULL, 0); 
        assert(r == 0);
        trim += 1;
    }
    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);
    return 0;
}

extern "C" int recovery_bad_last_entry(void);

int recovery_bad_last_entry(void){
    int r;
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);
 
    r = run_test();
    toku_ft_layer_destroy();
    return r;
}
