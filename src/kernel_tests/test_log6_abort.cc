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


/* Like test_log6 except abort. */


#include <db.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <memory.h>

#ifndef DB_DELETE_ANY
#define DB_DELETE_ANY 0
#endif

// TOKU_TEST_FILENAME is defined in the Makefile

struct in_db;
struct in_db {
    long int r;
    int i;
    struct in_db *next;
};
static struct in_db *items=0, *deleted_items=0;

static void put_n (DB *db, DB_TXN *tid, int i) {
    char hello[30], there[30];
    DBT key,data;
    struct in_db *XMALLOC(newitem);
    newitem->r = random();
    newitem->i = i;
    newitem->next = items;
    items = newitem;
    snprintf(hello, sizeof(hello), "hello%ld.%d", newitem->r, newitem->i);
    snprintf(there, sizeof(hello), "there%d", i);
    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));
    key.data  = hello; key.size=strlen(hello)+1;
    data.data = there; data.size=strlen(there)+1;
    int r=db->put(db, tid, &key, &data, 0);  assert(r==0);
}

static void del_n (DB *db, DB_TXN *tid, int i) {
    // Move it to deleted items if it is present.
    struct in_db *present;
    struct in_db **prevp;
    for ((prevp=&items),         (present=items);
	 present;
	 (prevp=&present->next), (present=present->next)) {
	if (present->i==i) {
	    // Remove it
	    struct in_db *next = present->next;
	    present->next = deleted_items;
	    deleted_items = present;
	    *prevp = next;

	    char hello[30];
	    DBT key;
	    snprintf(hello, sizeof(hello), "hello%ld.%d", present->r, i);
	    memset(&key, 0, sizeof(key));
	    key.data = hello; key.size = strlen(hello)+1;
	    int r = db->del(db, tid, &key, DB_DELETE_ANY); assert(r==0);

	    return;
	}
    }
}

static void make_db (void) {
    DB_ENV *env;
    DB *db;
    DB_TXN *tid;
    int r;
    int i;

    r=toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU+S_IRWXG+S_IRWXO);
    assert(r==0);
    r=db_env_create(&env, 0); assert(r==0);
    r=env->open(env, TOKU_TEST_ENV_DIR_NAME, DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);
    r=db_create(&db, env, 0); CKERR(r);
    r=env->txn_begin(env, 0, &tid, 0); assert(r==0);
    r=db->open(db, tid, TOKU_TEST_DATA_DB_NAME, 0, DB_BTREE, DB_CREATE, S_IRWXU+S_IRWXG+S_IRWXO); CKERR(r);
    r=tid->commit(tid, 0);    assert(r==0);
    r=env->txn_begin(env, 0, &tid, 0); assert(r==0);
    
    for (i=0; i<1; i++) {
	put_n(db, tid, i);
	if (random()%3==0) {
	    del_n(db, tid, random()%(i+1));
	}
    }
    r=tid->abort(tid);    assert(r==0);
    {
	struct in_db *l=items;
	for (l=items; l; l=l->next) {
	    char hello[30];
	    DBT key,data;
	    memset(&key, 0, sizeof(key));
	    memset(&data, 0, sizeof(data));
	    snprintf(hello, sizeof(hello), "hello%ld.%d", l->r, i);
	    r = db->get(db, 0, &key, &data, 0);
	    assert(r==DB_NOTFOUND);
	}
    }

    r=db->close(db, 0);       assert(r==0);
    r=env->close(env, 0);     assert(r==0);
    while (items) {
	struct in_db *next=items->next;
	toku_free(items);
	items=next;
    }

    while (deleted_items) {
	struct in_db *next=deleted_items->next;
	toku_free(deleted_items);
	deleted_items=next;
    }
}

extern "C" int test_test_log6_abort(void);
int test_test_log6_abort(void) {
    pre_setup();
    make_db();
    post_teardown();
    return 0;
}
