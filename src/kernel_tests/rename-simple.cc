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
#include "test.h"

// verify that renames on single dictionaries

static void
verify_locked(DB_ENV *env, DB *db, DBT *key) {
    int r;
    DB_TXN *txn = NULL;
    r = env->txn_begin(env, NULL, &txn, 0); assert_zero(r);;
    r = db->del(db, txn, key, DB_DELETE_ANY); assert(r == DB_LOCK_NOTGRANTED);
    r = txn->abort(txn); assert_zero(r);
}

/**
 * Calculate or verify that a value for a given key is correct
 * Returns 0 if the value is correct, nonzero otherwise.
 */
static void get_value_by_key(DBT * key, DBT * value)
{
    // keys/values are always stored in the DBT in net order
    char * CAST_FROM_VOIDP(k, key->data); 
    char c = k[0];
    char old_c = c-3;
    
    memcpy(value->data, &old_c, sizeof(char));
    memcpy((char*)value->data+1, k+1, sizeof(int));
}

static void verify_value_by_key(DBT * key, DBT * value)
{
    assert(key->size == sizeof(char) + sizeof(int));
    assert(value->size == sizeof(char) + sizeof(int));

    char * expected = (char *) toku_xmalloc(sizeof(char)+sizeof(int));
    DBT expected_dbt;
    expected_dbt.data = expected;
    expected_dbt.size = sizeof(int)+sizeof(char);
    get_value_by_key(key, &expected_dbt);

    char * CAST_FROM_VOIDP(v, value->data);
    assert_zero(memcmp(v, expected, sizeof(int)+sizeof(char)));
    toku_free(expected);
}

static void
verify_odd_to_even(DB_ENV *env, DB *db, int nrows) {
    int r;
    DB_TXN *txn = NULL;
    r = env->txn_begin(env, NULL, &txn, 0); assert_zero(r);

    DBC *cursor = NULL;
    r = db->cursor(db, txn, &cursor, 0); assert_zero(r);

    for(char c ='a'+3; c <= 'z'; c+=2) {
        for(int j = 0; j < nrows; j+=10) {
          DBT kdbt,vdbt;
	        int k = random() %nrows;
	        int nk = toku_htonl(k);
	        char * key = (char *) toku_xmalloc(sizeof(char) + sizeof(int));
	        key[0]=c;
	        memcpy(&key[1], &nk, sizeof(int));
	        dbt_init(&kdbt, key, sizeof(char)+sizeof(int));
          memset(&vdbt, 0, sizeof(DBT));
          r = db->get(db, txn, &kdbt, &vdbt, 0); { int chk_r = r; CKERR(chk_r); }
          verify_value_by_key(&kdbt, &vdbt);
        }
    }

    r = cursor->c_close(cursor); assert_zero(r);
    r = txn->commit(txn, 0); assert_zero(r);
}

static void
verify_rename(DB_ENV *env, DB *db, int nrows) {
    int r;
    DB_TXN *rmtxn = NULL;
    char * max_key = (char *) toku_xmalloc(sizeof(char) + sizeof(int));
    char min_key[1] ;
    char old_prefix[1];
    char new_prefix[1];
    
    for (char c ='a'; c <'z'-2; c +=2) {
        int k_max = toku_htonl(nrows-1);
        char new_c = c+3;
        min_key[0] =c;
        max_key[0] =c;
        memcpy(&max_key[1], &k_max, sizeof k_max);
        
        old_prefix[0] = c;
        new_prefix[0] = new_c;

        DBT min_kdbt = {min_key, 1};
        DBT max_kdbt = {max_key, sizeof(char)+sizeof(int)};
        DBT old_prefixdbt = {old_prefix, 1};
        DBT new_prefixdbt = {new_prefix, 1};
    
        r = env->txn_begin(env, NULL, &rmtxn, 0); assert_zero(r);
        r = db->rename(db, rmtxn, &min_kdbt, &max_kdbt, &old_prefixdbt,
                       &new_prefixdbt, 0); assert_zero(r);
        verify_locked(env, db, &max_kdbt);
        r = rmtxn->commit(rmtxn, 0); assert_zero(r);

    }

    verify_odd_to_even(env, db, nrows);
    toku_free(max_key);
}

static void
test_rename(int nrows) {
    int r;
    DB_ENV *env = NULL;
    r = db_env_create(&env, 0); assert_zero(r);

    r = env->open(env, TOKU_TEST_FILENAME, DB_INIT_MPOOL|DB_CREATE|DB_THREAD |DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_TXN|DB_PRIVATE, S_IRWXU+S_IRWXG+S_IRWXO); assert_zero(r);

    DB *db = NULL;
    r = db_create(&db, env, 0); assert_zero(r);

    r = db->open(db, NULL, "test.db", NULL, DB_BTREE, DB_AUTO_COMMIT+DB_CREATE, S_IRWXU+S_IRWXG+S_IRWXO); assert_zero(r);

    DB_TXN *txn = NULL;
    r = env->txn_begin(env, NULL, &txn, 0); assert_zero(r);

    char * key = (char *) toku_xmalloc(sizeof(char) + sizeof(int));;
    // populate
    for(char c = 'a'; c <= 'z';c+=2) {
        for (int i = 0; i < nrows; i++) {
          int k = htonl(i);
            key[0]=c;
            memcpy(&key[1], &k, sizeof k);
            DBT keydbt; dbt_init(&keydbt, key, sizeof(char) + sizeof(int));
            DBT valdbt; dbt_init(&valdbt, key, sizeof(char) + sizeof(int));
            r = db->put(db, txn, &keydbt, &valdbt, 0); assert_zero(r);
        }
    }
    toku_free(key);
    r = txn->commit(txn, 0); assert_zero(r);

    verify_rename(env, db, nrows);

    r = db->close(db, 0); assert_zero(r);

    r = env->close(env, 0); assert_zero(r);
}

extern "C" int test_rename_simple(void);

int
test_rename_simple(void) {
	pre_setup();
    int r;
    int nrows = 5000;

    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r = toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO); assert_zero(r);

    test_rename(nrows);
	post_teardown();
    return 0;
}

