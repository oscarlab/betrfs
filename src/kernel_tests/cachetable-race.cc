/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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

#ident "Copyright (c) 2010-2013 Tokutek Inc.  All rights reserved."
#ident "$Id$"

/* This test is a little bit ugly, but I think it's mostly harmless.
 * It's ugly because it requires a hook into cachetable.c, and that
 * hook is a little ugly.
 *
 * The way it works:
 *
 *  In the cachetable we set a variable that indicates that we are
 *  saving user data.  (This is toku_checkpointing_user_data_status).
 *
 *  In this test (cachetable-race), we close 2 of 5 dbs, and then busy
 *  wait for that status variable to go to 1 and close the others.
 *
 *  In this way, the close happens (with good chances) right in the
 *  middle of the checkpoint loop for saving all the user data, which
 *  seems to trigger the race error.
 */

#include "test.h"
#include "toku_pthread.h"
#include <db.h>
#include <sys/stat.h>
#include "ydb.h"
#include "ydb-internal.h"

static DB_ENV *env;
enum {NUM_DBS=5};

static const char *env_dir; // the default env_dir.

static const char* dbname[5] = {
    TOKU_TEST_DATA_DB_NAME,
    TOKU_TEST_META_DB_NAME,
    TOKU_TEST_ONE_DB_NAME,
    TOKU_TEST_TWO_DB_NAME,
    TOKU_TEST_THREE_DB_NAME
};

static void run_cachetable_race_test(void) 
{
    int r;
    r = toku_fs_reset(env_dir, S_IRWXU+S_IRWXG+S_IRWXO);                                                      CKERR(r);

    r = db_env_create(&env, 0);                                                                               CKERR(r);
    env->set_errfile(env, stderr);
    int envflags = DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE | DB_PRIVATE;
    r = env->open(env, env_dir, envflags, S_IRWXU+S_IRWXG+S_IRWXO);                                           CKERR(r);
    env->set_errfile(env, stderr);
    r = env->checkpointing_set_period(env, 5);                                                                CKERR(r);
    
    DB **XMALLOC_N(NUM_DBS, dbs);
    int idx[NUM_DBS];
    for(int i=0;i<NUM_DBS;i++) {
        idx[i] = i;
        r = db_create(&dbs[i], env, 0);                                                                       CKERR(r);
        dbs[i]->app_private = &idx[i];
        assert(i < 5);
        r = dbs[i]->open(dbs[i], NULL, dbname[i], NULL, DB_BTREE, DB_CREATE, 0666);                                CKERR(r);
    }

    for(int i=0;i<NUM_DBS;i++) {
        dbs[i]->close(dbs[i], 0);                                                                             CKERR(r);
        dbs[i] = NULL;
	
	if (i==2) {
	    if (verbose) printf("%s:%d c=%d\n", __FILE__, __LINE__, toku_test_get_checkpointing_user_data_status());
	    while (toku_test_get_checkpointing_user_data_status()==0)
		sched_yield();
	    if (verbose) printf("%s:%d c=%d\n", __FILE__, __LINE__, toku_test_get_checkpointing_user_data_status());
	}
    }
    r = env->close(env, 0);                                                                                   CKERR(r);
    toku_free(dbs);
}

// ------------ infrastructure ----------
static void do_args(void) {
    verbose = 1;
    //printf("\nTesting loader with size_factor=1\n");
    //toku_ft_loader_set_size_factor(1);            
}

extern "C" int test_cachetable_race(void);
int test_cachetable_race(void) {
    env_dir = TOKU_TEST_ENV_DIR_NAME;
    pre_setup();
    do_args();
    run_cachetable_race_test();
    post_teardown();
    return 0;
}
