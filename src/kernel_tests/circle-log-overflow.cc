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
// verify that the table lock log entry is handled

#include <sys/stat.h>
#include "test.h"
#include "ft/logsuperblock.h"

/*
This test tests some modified toku code but is ftfs-specific.
It is intended to test the circular log that is used with the SFS backend.
It just writes more than 2 GB to the log file and then ensures that recovery is still possible.
*/

static const int envflags = DB_INIT_MPOOL|DB_CREATE|DB_THREAD |DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_TXN|DB_PRIVATE;

static DB_ENV *env;
static DB_TXN *tid;
static DB     *db;
static DBT key,data;
static int i;
enum {N=10000};
// M_ONE and M_TWO have been chosen to overflow a 2 GB log
//   and to place the recovery checkpoint near the end of the file.
// START_BYTE is the location of the byte following the recovery checkpoint,
//   it changes if almost anything about the test changes. We track this value
//   because we need it to simulate a corrupted log without crashing the test.
const int M_ONE=170;
const int M_TWO=50;
const int START_BYTE=1653245745;
static char ** keys;
static char ** vals;

static void gracefully_shutdown(DB_TXN * oldest) {
    oldest->commit(oldest, 0);
    env->close(env, 0);
    printf("shutdown%s", "\n");
}

/*
    This function is intended to corrupt the log file to simulate
    a messy shutdown and force recovery
*/
static void gracefully_shutdown_and_corrupt(DB_TXN * oldest) {
    gracefully_shutdown(oldest);
    uint32_t last_byte;
    uint32_t start_byte = START_BYTE;
    int fd = open(TOKU_SFS_LOG_FILE, O_RDWR);
    off_t r = lseek(fd, TOKU_LOG_END_OFFSET, SEEK_SET);
    assert(r==TOKU_LOG_END_OFFSET);
    r = read(fd, (void *)&last_byte, 4);
    assert(r==4);

    // SCB (9/9/19): We have to do this to force recovery, it is very brittle
    r = lseek(fd, TOKU_LOG_START_OFFSET, SEEK_SET);
    assert(r==TOKU_LOG_START_OFFSET);
    toku_os_full_write(fd, (char *)&start_byte, 4);

    int amount_to_garb = 256;
    off_t pos = lseek(fd, last_byte - amount_to_garb, SEEK_SET);
    assert(pos == last_byte - amount_to_garb);
    char *buf;
    buf = (char *) toku_xmalloc(amount_to_garb);
    memset(buf, 0, amount_to_garb);
    toku_os_full_write(fd, buf, amount_to_garb);
    toku_free(buf);
    printf("Log corrupted%s", "\n");
}

static void
do_x1_shutdown (void) {
    DB_TXN *oldest;
    int r;
    int ind;
    char * vs;
    r=toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU+S_IRWXG+S_IRWXO);                                                     assert(r==0);
    assert(r==0);
    r=db_env_create(&env, 0);                                                  assert(r==0);
    env->set_errfile(env, stderr);
    r=env->open(env, TOKU_TEST_ENV_DIR_NAME, DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE|DB_THREAD, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);
   {

        r=env->txn_begin(env, 0, &oldest, 0);
        CKERR(r);
    }

    r=db_create(&db, env, 0);                                                  CKERR(r);
    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
    r=db->open(db, tid, TOKU_TEST_DATA_DB_NAME, 0, DB_BTREE, DB_CREATE, S_IRWXU+S_IRWXG+S_IRWXO);               CKERR(r);
    r=tid->commit(tid, 0);                                                     assert(r==0);

    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
    // Repeatedly write random values to db to fill up log
    vs = (char *) toku_xmalloc(sizeof(char) * 1000);
    for (i=0; i<M_ONE*N; i++) {
        memset(vs, 0, 1000);
        ind = i%N;
        snprintf(vs, 1000, "v%d.%0*d", ind, (int)(1000-100), (int) random());
        toku_free(vals[ind]);
	vals[ind]=toku_strdup(vs);

	r=db->put(db, tid, dbt_init(&key, keys[ind], strlen(keys[ind])+1), dbt_init(&data, vals[ind], strlen(vals[ind])+1), 0);    assert(r==0);
	if (i%500==499) {
	    r=tid->commit(tid, 0);                                                     assert(r==0);
	    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
	}
    }

    r=tid->commit(tid, 0);
    r=env->txn_checkpoint(env, 0, 0, 0);
    r=env->txn_begin(env, 0, &tid, 0);

    for (i=0; i<M_TWO*N; i++) {
        memset(vs, 0, 1000);
        ind = i%N;
        snprintf(vs, 1000, "v%d.%0*d", ind, (int)(1000-100), (int) random());
        toku_free(vals[ind]);
	vals[ind]=toku_strdup(vs);

	r=db->put(db, tid, dbt_init(&key, keys[ind], strlen(keys[ind])+1), dbt_init(&data, vals[ind], strlen(vals[ind])+1), 0);    assert(r==0);
	if (i%500==499) {
	    r=tid->commit(tid, 0);                                                     assert(r==0);
	    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
	}
    }

    // Return db to expected state
    for (i=0; i<N; i++) {
        memset(vs, 0, 1000);
        snprintf(vs, 1000, "v%d.%0*d", i, (int)(1000-100), i);
        toku_free(vals[i]);
	vals[i]=toku_strdup(vs);

	r=db->put(db, tid, dbt_init(&key, keys[i], strlen(keys[i])+1), dbt_init(&data, vals[i], strlen(vals[i])+1), 0);    assert(r==0);
	if (i%500==499) {
	    r=tid->commit(tid, 0);                                                     assert(r==0);
	    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
	}
    }
    toku_free(vs);

    r=tid->commit(tid, 0);                                                     assert(r==0);

    r=db->close(db, 0);                                                        assert(r==0);
    gracefully_shutdown_and_corrupt(oldest);
}

static void
do_x1_recover (bool UU(did_commit)) {
    int r;
    r=db_env_create(&env, 0);                                                  assert(r==0);
    env->set_errfile(env, stderr);
    r=env->open(env, TOKU_TEST_ENV_DIR_NAME, DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE|DB_THREAD|DB_RECOVER, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);
    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
    r=db_create(&db, env, 0);                                                  CKERR(r);
    r=db->open(db, tid, TOKU_TEST_DATA_DB_NAME, 0, DB_BTREE, 0, S_IRWXU+S_IRWXG+S_IRWXO);                       CKERR(r);
    for (i=0; i<N; i++) {
	r=db->get(db, tid, dbt_init(&key, keys[i], 1+strlen(keys[i])), dbt_init_malloc(&data), 0);     assert(r==0);
	assert(strcmp((char*)data.data, vals[i])==0);
	toku_free(data.data);
	data.data=0;
	if (i%500==499) {
	    r=tid->commit(tid, 0);                                                     assert(r==0);
	    r=env->txn_begin(env, 0, &tid, 0);                                         assert(r==0);
	}
    }
    r=tid->commit(tid, 0);                                                     assert(r==0);
    toku_free(data.data);
    r=db->close(db, 0);                                                        CKERR(r);
    r=env->close(env, 0);                                                      CKERR(r);
}



static void internal_circle_overflow_test(bool do_commit, bool do_recover_committed) {
    srandom(0xDEADBEEF);
    for (i=0; i<N; i++) {
        char * ks = (char *) toku_xmalloc(sizeof(char) * 100);
        snprintf(ks, 100, "k%09ld.%d", random(), i);
        char * vs = (char *) toku_xmalloc(sizeof(char) * 1000);
        snprintf(vs, 1000, "v%d.%0*d", i, (int)(1000-100), i);
	keys[i]=toku_strdup(ks);
	vals[i]=toku_strdup(vs);
        toku_free(ks);
        toku_free(vs);
    }
    if (do_commit) {
        do_x1_shutdown();
    } else if (do_recover_committed) {
	do_x1_recover(true);
    }
    for (i=0; i<N; i++) {
        toku_free(keys[i]);
        toku_free(vals[i]);
    }

}
extern "C" int test_circle_log_overflow(void);
int test_circle_log_overflow(void) {
    pre_setup();
    keys = (char **) toku_xmalloc(sizeof(char *) * (N));
    vals = (char **) toku_xmalloc(sizeof(char *) * (N));

    internal_circle_overflow_test(true,false);
    internal_circle_overflow_test(false,true);

    toku_free(keys);
    toku_free(vals);
    post_teardown();
    return 0;
}
