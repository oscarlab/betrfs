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

enum ftnode_verify_type {
    read_all=1,
    read_compressed,
    read_none
};

#ifndef MIN
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#endif

static int
string_key_cmp(DB *UU(e), const DBT *a, const DBT *b)
{
    char *CAST_FROM_VOIDP(s, a->data);
    char *CAST_FROM_VOIDP(t, b->data);
    return strcmp(s, t);
}

static void
le_add_to_bn(bn_data* bn, uint32_t idx, const  char *key, int keylen, const char *val, int vallen)
{
    LEAFENTRY r = NULL;
    uint32_t size_needed = LE_CLEAN_MEMSIZE(vallen);
    bn->get_space_for_insert(
        idx,
        key,
        keylen,
        size_needed,
        &r
        );
    resource_assert(r);
    r->type = LE_CLEAN;
    r->u.clean.vallen = vallen;
    memcpy(r->u.clean.val, val, vallen);
}


static void
le_malloc(bn_data* bn, uint32_t idx, const char *key, const char *val)
{
    int keylen = strlen(key) + 1;
    int vallen = strlen(val) + 1;
    le_add_to_bn(bn, idx, key, keylen, val, vallen);
}


static void
test1(int fd, FT brt_h, FTNODE *dn) {
    int r;
    struct ftnode_fetch_extra bfe_all;
    brt_h->key_ops.keycmp = string_key_cmp;
    fill_bfe_for_full_read(&bfe_all, brt_h);
    FTNODE_DISK_DATA ndd = NULL;
    r = toku_deserialize_ftnode_from(fd, make_blocknum(20), 0/*pass zero for hash*/, dn, &ndd, &bfe_all);
    bool is_leaf = ((*dn)->height == 0);
    assert(r==0);
    for (int i = 0; i < (*dn)->n_children; i++) {
        assert(BP_STATE(*dn,i) == PT_AVAIL);
    }
    // should sweep and NOT get rid of anything
    PAIR_ATTR attr;
    memset(&attr,0,sizeof(attr));
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    for (int i = 0; i < (*dn)->n_children; i++) {
        assert(BP_STATE(*dn,i) == PT_AVAIL);
    }
    // should sweep and get compress all
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    for (int i = 0; i < (*dn)->n_children; i++) {
        if (!is_leaf) {
            assert(BP_STATE(*dn,i) == PT_COMPRESSED);
        }
        else {
            assert(BP_STATE(*dn,i) == PT_ON_DISK);
        }
    }
    PAIR_ATTR size;
    bool req = toku_ftnode_pf_req_callback(*dn, &bfe_all);
    assert(req);
    toku_ftnode_pf_callback(*dn, ndd, &bfe_all, fd, &size);
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    for (int i = 0; i < (*dn)->n_children; i++) {
        assert(BP_STATE(*dn,i) == PT_AVAIL);
    }
    // should sweep and get compress all
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    for (int i = 0; i < (*dn)->n_children; i++) {
        if (!is_leaf) {
            assert(BP_STATE(*dn,i) == PT_COMPRESSED);
        }
        else {
            assert(BP_STATE(*dn,i) == PT_ON_DISK);
        }
    }

    req = toku_ftnode_pf_req_callback(*dn, &bfe_all);
    assert(req);
    toku_ftnode_pf_callback(*dn, ndd, &bfe_all, fd, &size);
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    for (int i = 0; i < (*dn)->n_children; i++) {
        assert(BP_STATE(*dn,i) == PT_AVAIL);
    }
    (*dn)->dirty = 1;
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    for (int i = 0; i < (*dn)->n_children; i++) {
        assert(BP_STATE(*dn,i) == PT_AVAIL);
    }
    toku_free(ndd);
    toku_ftnode_free(dn);
}


static int search_cmp(const struct ft_search& UU(so), const DBT* UU(key)) {
    return 0;
}

static void
test2(int fd, FT brt_h, FTNODE *dn) {
    struct ftnode_fetch_extra bfe_subset;
    DBT left, right;
    DB dummy_db;
    memset(&dummy_db, 0, sizeof(dummy_db));
    memset(&left, 0, sizeof(left));
    memset(&right, 0, sizeof(right));
    ft_search_t search_t;

    brt_h->key_ops.keycmp = string_key_cmp;
    fill_bfe_for_subset_read(
        &bfe_subset,
        brt_h,
        ft_search_init(
            &search_t,
            search_cmp,
            FT_SEARCH_LEFT,
            NULL,
            NULL
            ),
        &left,
        &right,
        true,
        true,
        false,
        false
        );
    FTNODE_DISK_DATA ndd = NULL;
    int r = toku_deserialize_ftnode_from(fd, make_blocknum(20), 0/*pass zero for hash*/, dn, &ndd, &bfe_subset);
    assert(r==0);
    bool is_leaf = ((*dn)->height == 0);
    // at this point, although both partitions are available, only the
    // second basement node should have had its clock
    // touched
    assert(BP_STATE(*dn, 0) == PT_AVAIL);
    assert(BP_STATE(*dn, 1) == PT_AVAIL);
    assert(BP_SHOULD_EVICT(*dn, 0));
    assert(!BP_SHOULD_EVICT(*dn, 1));
    PAIR_ATTR attr;
    memset(&attr,0,sizeof(attr));
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    assert(BP_STATE(*dn, 0) == (is_leaf) ? PT_ON_DISK : PT_COMPRESSED);
    assert(BP_STATE(*dn, 1) == PT_AVAIL);
    assert(BP_SHOULD_EVICT(*dn, 1));
    toku_ftnode_pe_callback(*dn, attr, &attr, brt_h);
    assert(BP_STATE(*dn, 1) == (is_leaf) ? PT_ON_DISK : PT_COMPRESSED);

    bool req = toku_ftnode_pf_req_callback(*dn, &bfe_subset);
    assert(req);
    toku_ftnode_pf_callback(*dn, ndd, &bfe_subset, fd, &attr);
    assert(BP_STATE(*dn, 0) == PT_AVAIL);
    assert(BP_STATE(*dn, 1) == PT_AVAIL);
    assert(BP_SHOULD_EVICT(*dn, 0));
    assert(!BP_SHOULD_EVICT(*dn, 1));

    toku_free(ndd);
    toku_ftnode_free(dn);
}

static void
test3_leaf(int fd, FT brt_h, FTNODE *dn) {
    struct ftnode_fetch_extra bfe_min;
    DBT left, right;
    DB dummy_db;
    memset(&dummy_db, 0, sizeof(dummy_db));
    memset(&left, 0, sizeof(left));
    memset(&right, 0, sizeof(right));

    brt_h->key_ops.keycmp = string_key_cmp;
    fill_bfe_for_min_read(
        &bfe_min,
        brt_h
        );
    FTNODE_DISK_DATA ndd = NULL;
    int r = toku_deserialize_ftnode_from(fd, make_blocknum(20), 0/*pass zero for hash*/, dn, &ndd, &bfe_min);
    assert(r==0);
    //
    // make sure we have a leaf
    //
    assert((*dn)->height == 0);
    for (int i = 0; i < (*dn)->n_children; i++) {
        assert(BP_STATE(*dn, i) == PT_ON_DISK);
    }
    toku_ftnode_free(dn);
    toku_free(ndd);
}

static void
test_serialize_nonleaf(void) {
    //    struct ft_handle source_ft;
    struct ftnode sn, *dn;
    memset(&sn, 0, sizeof(struct ftnode));

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);

    int fd = open(TOKU_TEST_FILENAME_DATA, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);

    //    source_ft.fd=fd;
    sn.max_msn_applied_to_node_on_disk.msn = 0;
    toku_init_dbt(&sn.bound_l);
    toku_init_dbt(&sn.bound_r);
    char *hello_string;
    sn.flags = 0x11223344;
    sn.thisnodename.b = 20;
    sn.layout_version = FT_LAYOUT_VERSION;
    sn.layout_version_original = FT_LAYOUT_VERSION;
    sn.height = 1;
    sn.n_children = 2;
    sn.dirty = 1;
    sn.oldest_referenced_xid_known = TXNID_NONE;
    sn.unbound_insert_count = 0;
    hello_string = toku_strdup("hello");
    MALLOC_N(2, sn.bp);
    MALLOC_N(1, sn.childkeys);
    toku_fill_dbt(&sn.childkeys[0], hello_string, 6);
    sn.totalchildkeylens = 6;
    BP_BLOCKNUM(&sn, 0).b = 30;
    BP_BLOCKNUM(&sn, 1).b = 35;
    BP_STATE(&sn,0) = PT_AVAIL;
    BP_STATE(&sn,1) = PT_AVAIL;
    set_BNC(&sn, 0, toku_create_empty_nl());
    set_BNC(&sn, 1, toku_create_empty_nl());
    toku_init_dbt(&BP_LIFT(&sn, 0));
    toku_init_dbt(&BP_LIFT(&sn, 1));
    toku_init_dbt(&BP_NOT_LIFTED(&sn, 0));
    toku_init_dbt(&BP_NOT_LIFTED(&sn, 1));
    //Create XIDS
    XIDS xids_0 = xids_get_root_xids();
    XIDS xids_123;
    XIDS xids_234;
    r = xids_create_child(xids_0, &xids_123, (TXNID)123);
    CKERR(r);
    r = xids_create_child(xids_123, &xids_234, (TXNID)234);
    CKERR(r);

    FT_MSG_S msg;
    DBT k,v;
    toku_fill_dbt(&k, "a",2);
    toku_fill_dbt(&v, "aval",5);
    ft_msg_init(&msg, FT_NONE, next_dummymsn(), xids_0, &k, &v);
    toku_bnc_insert_msg(BNC(&sn, 0), NULL,  &msg, true, NULL, string_key_cmp);

    FT_MSG_S msg2;
    toku_fill_dbt(&k, "b",2);
    toku_fill_dbt(&v, "bval",5);
    ft_msg_init(&msg2, FT_NONE, next_dummymsn(), xids_123, &k, &v);
    toku_bnc_insert_msg(BNC(&sn, 0), NULL, &msg2, false, NULL, string_key_cmp);

    FT_MSG_S msg3;
    toku_fill_dbt(&k, "x",2);
    toku_fill_dbt(&v, "xval",5);
    ft_msg_init(&msg3, FT_NONE, next_dummymsn(), xids_234, &k, &v);
    toku_bnc_insert_msg(BNC(&sn, 1), NULL, &msg3, true, NULL, string_key_cmp);
    //Cleanup:
    xids_destroy(&xids_0);
    xids_destroy(&xids_123);
    xids_destroy(&xids_234);

    FT_HANDLE XMALLOC(brt);
    FT XCALLOC(brt_h);
    toku_ft_init(brt_h,
                 make_blocknum(0),
                 ZERO_LSN,
                 TXNID_NONE,
                 4*1024*1024,
                 128*1024,
                 TOKU_DEFAULT_COMPRESSION_METHOD);
    brt->ft = brt_h;

    toku_blocktable_create_new(&brt_h->blocktable);
    //Want to use block #20
    BLOCKNUM b = make_blocknum(0);
    while (b.b < 20) {
        toku_allocate_blocknum(brt_h->blocktable, &b, brt_h);
    }
    assert(b.b == 20);

    DISKOFF offset;
    DISKOFF size;
    {
        toku_blocknum_realloc_on_disk(brt_h->blocktable, b, 100, &offset, brt_h, fd, false);
        assert(offset==BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);

        toku_translate_blocknum_to_offset_size(brt_h->blocktable, b, &offset, &size);
        assert(offset == BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);
        assert(size   == 100);
    }
    FTNODE_DISK_DATA ndd = NULL;
    r = toku_serialize_ftnode_to(fd, make_blocknum(20), &sn, &ndd, true, brt->ft, false, &offset, &size, true);
    assert(r==0);

    test1(fd, brt_h, &dn);
    test2(fd, brt_h, &dn);

    toku_free(hello_string);
    destroy_nonleaf_childinfo(BNC(&sn, 0));
    destroy_nonleaf_childinfo(BNC(&sn, 1));
    toku_free(sn.bp);
    toku_free(sn.childkeys);
    toku_free(ndd);

    toku_block_free(brt_h->blocktable, BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);
    toku_blocktable_destroy(&brt_h->blocktable);
    toku_free(brt_h->h);
    toku_free(brt_h);
    toku_free(brt);

    r = close(fd); assert(r != -1);
}

static void
test_serialize_leaf(void) {
    //    struct ft_handle source_ft;
    struct ftnode sn, *dn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);

    int fd = open(TOKU_TEST_FILENAME_DATA, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);


    sn.max_msn_applied_to_node_on_disk.msn = 0;
    toku_init_dbt(&sn.bound_l);
    toku_init_dbt(&sn.bound_r);
    sn.flags = 0x11223344;
    sn.thisnodename.b = 20;
    sn.layout_version = FT_LAYOUT_VERSION;
    sn.layout_version_original = FT_LAYOUT_VERSION;
    sn.height = 0;
    sn.n_children = 2;
    sn.dirty = 1;
    sn.oldest_referenced_xid_known = TXNID_NONE;
    sn.unbound_insert_count = 0;
    MALLOC_N(sn.n_children, sn.bp);
    MALLOC_N(1, sn.childkeys);
    toku_memdup_dbt(&sn.childkeys[0], "b", 2);
    sn.totalchildkeylens = 2;
    BP_STATE(&sn,0) = PT_AVAIL;
    BP_STATE(&sn,1) = PT_AVAIL;
    set_BLB(&sn, 0, toku_create_empty_bn());
    set_BLB(&sn, 1, toku_create_empty_bn());

    setup_leaf_prefetch_state(&sn);

    toku_init_dbt(&BP_LIFT(&sn, 0));
    toku_init_dbt(&BP_LIFT(&sn, 1));
    le_malloc(BLB_DATA(&sn, 0), 0, "a", "aval");
    le_malloc(BLB_DATA(&sn, 0), 1, "b", "bval");
    le_malloc(BLB_DATA(&sn, 1), 0, "x", "xval");

    FT_HANDLE XMALLOC(brt);
    FT XCALLOC(brt_h);
    toku_ft_init(brt_h,
                 make_blocknum(0),
                 ZERO_LSN,
                 TXNID_NONE,
                 4*1024*1024,
                 128*1024,
                 TOKU_DEFAULT_COMPRESSION_METHOD);
    brt->ft = brt_h;

    toku_blocktable_create_new(&brt_h->blocktable);
    //Want to use block #20
    BLOCKNUM b = make_blocknum(0);
    while (b.b < 20) {
        toku_allocate_blocknum(brt_h->blocktable, &b, brt_h);
    }
    assert(b.b == 20);

    DISKOFF offset;
    DISKOFF size;
    {
        toku_blocknum_realloc_on_disk(brt_h->blocktable, b, 100, &offset, brt_h, fd, false);
        assert(offset==BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);

        toku_translate_blocknum_to_offset_size(brt_h->blocktable, b, &offset, &size);
        assert(offset == BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);
        assert(size   == 100);
    }
    FTNODE_DISK_DATA ndd = NULL;
    r = toku_serialize_ftnode_to(fd, make_blocknum(20), &sn, &ndd, true, brt->ft, false, &offset, &size, true);
    assert(r==0);
    test1(fd, brt_h, &dn);
    test3_leaf(fd, brt_h,&dn);


    for (int i = 0; i < sn.n_children-1; ++i) {
        toku_free(sn.childkeys[i].data);
    }
    for (int i = 0; i < sn.n_children; i++) {
        destroy_basement_node(BLB(&sn, i));
    }
    toku_free(sn.bp);
    toku_free(sn.childkeys);

    toku_block_free(brt_h->blocktable, BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);
    toku_blocktable_destroy(&brt_h->blocktable);
    toku_free(brt_h->h);
    toku_free(brt_h);
    toku_free(brt);
    toku_free(ndd);
    r = close(fd); assert(r != -1);
}

extern "C" int test_ft_clock(void);
int
test_ft_clock (void) {
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);
    test_serialize_nonleaf();
    test_serialize_leaf();

    toku_ft_layer_destroy();
    return 0;
}
