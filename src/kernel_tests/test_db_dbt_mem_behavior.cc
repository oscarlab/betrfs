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
#include "test.h"

#include <memory.h>
#include <db.h>

#include <errno.h>
#include <sys/stat.h>


// TOKU_TEST_FILENAME is defined in the Makefile

typedef struct {
    int32_t pkey;
    char    waste[1024];
} DATA;

static DB* db;
static DB_TXN *const null_txn = 0;
static DB_ENV *dbenv;
static uint32_t set_ulen;
static int32_t key_1 = 1;

static void
setup(void) {
    int r;

    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU+S_IRWXG+S_IRWXO);
    assert(r==0);
    r = db_env_create(&dbenv, 0); assert(r == 0);
    r = dbenv->open(dbenv, TOKU_TEST_ENV_DIR_NAME, DB_CREATE+DB_PRIVATE+DB_INIT_MPOOL+DB_INIT_LOG+DB_INIT_TXN, 0); assert(r == 0);
    /* Open/create primary */
    r = db_create(&db, dbenv, 0);                                               CKERR(r);
    r = db->open(db, null_txn, TOKU_TEST_DATA_DB_NAME, NULL, DB_BTREE, DB_CREATE, 0600);  CKERR(r);
}

static void
insert_test (void) {
    int r;
    //DATA entry;
    DATA * p_entry = (DATA *) toku_xmalloc(sizeof(DATA));
    DBT data;
    DBT key;

    memset(p_entry, 0xFF, sizeof(DATA));
    p_entry->pkey = key_1;
    
    dbt_init(&key, &p_entry->pkey, sizeof(p_entry->pkey));
    dbt_init(&data, p_entry, sizeof(DATA));
    r = db->put(db, null_txn, &key, &data, 0);  CKERR(r);
    toku_free(p_entry);
}

static void
close_dbs (void) {
    int r;

    r = db->close(db, 0);       CKERR(r);
    r = dbenv->close(dbenv, 0); CKERR(r);
}


extern "C" int test_test_db_dbt_mem_behavior(void);

int test_test_db_dbt_mem_behavior(void) {
    int i;
    int r;
    key_1 = 1; 
    pre_setup();
    //Simple flags that require minimal setup.
    uint32_t flags[] = {
        0,
        DB_DBT_USERMEM,
        DB_DBT_MALLOC,
        DB_DBT_REALLOC,
    };
    int num_flags = sizeof(flags) / sizeof(flags[0]);

    int j;
    setup();
    insert_test();
    DBT key;
    DBT data;
    void* oldmem;
    
    for (j = 0; j < num_flags; j++) {
        for (i = 0; i < 2; i++) {
            if (i) set_ulen = sizeof(DATA) / 2;
            else   set_ulen = sizeof(DATA);

            unsigned int old_ulen;
            int was_truncated = 0;
            int ulen_changed;
            int size_full;
            int doclone = 0;
            //DATA fake;
            DATA * p_fake = (DATA *) toku_xmalloc(sizeof(DATA));
            int small_buffer = 0;
            
            memset(p_fake, 0xFF, sizeof(DATA));
            p_fake->pkey = key_1;
             
            
            dbt_init(&key, &key_1, sizeof(key_1));
            dbt_init(&data, 0, 0);
            data.flags = flags[j];
            oldmem = toku_malloc(set_ulen);
            data.data = oldmem;
            memset(oldmem, 0, set_ulen);
            if (flags[j] == DB_DBT_USERMEM) {
                data.ulen = set_ulen;
            }
            old_ulen = data.ulen;
            r = db->get(db, null_txn, &key, &data, 0);
            if (flags[j] == DB_DBT_USERMEM && set_ulen < sizeof(DATA)) CKERR2(r, DB_BUFFER_SMALL);
            else CKERR(r);
            
            if (r == DB_BUFFER_SMALL) {
                //The entire 'waste' is full of 0xFFs
                DATA* CAST_FROM_VOIDP(entry, data.data);
                was_truncated = entry->waste[0] != 0;
                small_buffer = 1;
            }
            ulen_changed = data.ulen != old_ulen;
            size_full = data.size == sizeof(DATA);
            
            unsigned int min = data.ulen < data.size ? data.ulen : data.size;
            min = min < sizeof(DATA) ? min : sizeof(DATA);
            //assert(min == sizeof(DATA));
            r = memcmp((DATA*)data.data, p_fake, min);
            doclone = r == 0;

            if (flags[j] != 0) {
                toku_free(data.data);
            }
            if (flags[j] == 0 || flags[j] == DB_DBT_MALLOC) {
                toku_free(oldmem);
            }
                
            assert(!was_truncated);

            bool ulen_should_change = false;
#if defined(USE_TDB)
            if (flags[j] == DB_DBT_REALLOC) {
                ulen_should_change = (bool)(old_ulen < sizeof(DATA));
            }
            else if (flags[j] == DB_DBT_MALLOC) {
                ulen_should_change = (bool)(old_ulen != sizeof(DATA)*2);
            }
#endif
            assert(ulen_should_change == (bool)ulen_changed);
            assert(size_full);
            assert(doclone == !small_buffer);
        
            toku_free(p_fake);
        }
    }
    oldmem = 0;
    dbt_init(&key, 0, 0);
    dbt_init(&data, 0, 0);
    close_dbs();
    post_teardown();
    return 0;
}

