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

static int verbose = 0;
#include "test.h"
struct iter_fn_args {
    int * p_i;
    MSN * p_startmsn;
    int * p_thekeylen;
    int * p_thevallen;
    char ** p_thekey;
    char ** p_theval;
};
static int iter_fn(FT_MSG msg, bool UU(is_fresh), void *args) {
    enum ft_msg_type type = ft_msg_get_type(msg);
    void* key = ft_msg_get_key(msg);
    void* val = ft_msg_get_val(msg);
    uint32_t keylen = ft_msg_get_keylen(msg);
    uint32_t vallen = ft_msg_get_vallen(msg);
    MSN msn = ft_msg_get_msn(msg);
    XIDS xids = ft_msg_get_xids(msg);
    struct iter_fn_args * fn_args = (struct iter_fn_args *) args;
    int * p_i = fn_args -> p_i;
    MSN * p_startmsn = fn_args -> p_startmsn;
    int* p_thekeylen = fn_args->p_thekeylen;
    int* p_thevallen = fn_args->p_thevallen;
    char** p_thekey = fn_args->p_thekey;
    char** p_theval = fn_args->p_theval;

    if (verbose) printf("checkit %d %d %" PRIu64 "\n", *p_i, type, msn.msn);
    assert(msn.msn == p_startmsn->msn + *p_i);
#define buildkey(len) { \
        XREALLOC_N(*p_thekeylen, len+1, *p_thekey);     \
        *p_thekeylen = len+1;                           \
        memset(*p_thekey, len, *p_thekeylen);           \
    }

#define buildval(len) { \
        XREALLOC_N(*p_thevallen, len+2, *p_theval);     \
        *p_thevallen = len+2;                           \
        memset(*p_theval, ~len, *p_thevallen);          \
    }

    buildkey(*p_i);
    buildval(*p_i);

    assert((int) keylen == *p_thekeylen); assert(memcmp(key, *p_thekey, keylen) == 0);
    if(type != FT_GOTO) {
        assert((int) vallen == *p_thevallen);
        assert(memcmp(val, *p_theval, vallen) == 0);
    }
    assert(cast_to_msg_type(*p_i) == type);
    assert((TXNID)*p_i==xids_get_innermost_xid(xids));
    *p_i = *p_i+1;
    return 0;
}

static void
test_fifo_create (void) {
    int r;
    FIFO f;

    f = 0;
    r = toku_fifo_create(&f);
    assert(r == 0); assert(f != 0);

    toku_fifo_free(&f);
    assert(f == 0);
}
static void
test_fifo_enq (int n) {
    int r;
    FIFO f;
    MSN startmsn = ZERO_MSN;

    f = 0;
    r = toku_fifo_create(&f);
    assert(r == 0); assert(f != 0);

    char *thekey = 0; int thekeylen;
    char *maxkey = 0; int maxkeylen;
    char *theval = 0; int thevallen;

    // this was a function but icc cant handle it
#define buildkey2(len) { \
        thekeylen = len+1;              \
        if (thekey) toku_free(thekey);  \
        XMALLOC_N(thekeylen, thekey);   \
        memset(thekey, len, thekeylen); \
    }

#define buildmaxkey2(len) {                     \
        maxkeylen = len+1;                      \
        if(maxkey) toku_free(maxkey);           \
        XMALLOC_N(maxkeylen, maxkey);           \
        memset(maxkey, len+1, maxkeylen);       \
    }

#define buildval2(len) {                        \
        thevallen = len+2;                      \
        if (thevallen) toku_free(theval);    \
        XMALLOC_N(thevallen, theval);           \
        memset(theval, ~len, thevallen);        \
    }

    for (int i=0; i<n; i++) {
        buildkey2(i);
        buildmaxkey2(i);
        buildval2(i);
       XIDS xids;
        if (i==0)
            xids = xids_get_root_xids();
        else {
            r = xids_create_child(xids_get_root_xids(), &xids, (TXNID)i);
            assert(r==0);
        }
        MSN msn = next_dummymsn();
        if (startmsn.msn == ZERO_MSN.msn)
            startmsn = msn;
        enum ft_msg_type type = cast_to_msg_type(i);
        FT_MSG_S msg;

        DBT kdbt, vdbt, mdbt;
        toku_fill_dbt(&kdbt,thekey,thekeylen);
        toku_fill_dbt(&mdbt,maxkey,maxkeylen);
        toku_fill_dbt(&vdbt,theval, thevallen);
        if (ft_msg_type_is_multicast(type)) {
            ft_msg_multicast_init(&msg, type, msn, xids, &kdbt, &mdbt, &vdbt, true, PM_UNCOMMITTED);
        } else if (FT_GOTO == type) {
            ft_msg_goto_init(&msg, msn, xids, &kdbt, &mdbt, &vdbt, {0}, 0);
        } else {
            ft_msg_init(&msg, type, msn, xids, &kdbt, &vdbt);
        }
        r = toku_fifo_enq(f, &msg, true, NULL); assert(r == 0);
        xids_destroy(&xids);
    }

    int i = 0;
    struct iter_fn_args args = {
    &i,
    &startmsn,
    &thekeylen,
    &thevallen,
    &thekey,
    &theval
    };
    toku_fifo_iterate(f, iter_fn, &args);
#if 0
    FIFO_ITERATE(f, key, keylen, val, vallen, type, msn, xids, UU(is_fresh), {
        if (verbose) printf("checkit %d %d %" PRIu64 "\n", i, type, msn.msn);
       assert(msn.msn == startmsn.msn + i);
        buildkey(i);
        buildval(i);
        assert((int) keylen == thekeylen); assert(memcmp(key, thekey, keylen) == 0);
        assert((int) vallen == thevallen); assert(memcmp(val, theval, vallen) == 0);
        assert(i % 256 == (int)type);
	assert((TXNID)i==xids_get_innermost_xid(xids));
        i += 1;
    });
#endif
    assert(i == n);

    if (thekey) toku_free(thekey);
    if (theval) toku_free(theval);
    if (maxkey) toku_free(maxkey);

    toku_fifo_free(&f);
    assert(f == 0);
}

extern "C" int test_fifo(void);
int test_fifo(void) {
	initialize_dummymsn();
	int rinit = toku_ft_layer_init();
	CKERR(rinit);
	test_fifo_create();
	test_fifo_enq(4);
	test_fifo_enq(512);
	toku_ft_layer_destroy();
	return 0;
}
