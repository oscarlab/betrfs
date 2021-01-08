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

// it used to be the case that we copied the left and right keys of a
// range to be prelocked but never freed them, this test checks that they
// are freed (as of this time, this happens in destroy_bfe_for_prefetch)

#include "test_benchmark.h"


#include <ft-cachetable-wrappers.h>
#include <ft-flusher.h>

// Some constants to be used in calculations below
static const int nodesize = 1024; // Target max node size
static const int eltsize = 64;    // Element size (for most elements)
static const int bnsize = 256;    // Target basement node size
static const int eltsperbn = 256 / 64;  // bnsize / eltsize
static const int keylen = sizeof(long);
// vallen is eltsize - keylen and leafentry overhead
static const int vallen = 64 - sizeof(long) - (sizeof(((LEAFENTRY)NULL)->type)  // overhead from LE_CLEAN_MEMSIZE
                                               +sizeof(uint32_t)
                                               +sizeof(((LEAFENTRY)NULL)->u.clean.vallen));
#define dummy_msn_3884 ((MSN) { (uint64_t) 3884 * MIN_MSN.msn })

static TOKUTXN const null_txn = 0;
static DB * const null_db = 0;
static const char * fname;

static void
le_add_to_bn(bn_data* bn, uint32_t idx, const  char *key, int keysize, const char *val, int valsize)
{
    LEAFENTRY r = NULL;
    uint32_t size_needed = LE_CLEAN_MEMSIZE(valsize);
    bn->get_space_for_insert(
        idx,
        key,
        keysize,
        size_needed,
        &r
        );
    resource_assert(r);
    r->type = LE_CLEAN;
    r->u.clean.vallen = valsize;
    memcpy(r->u.clean.val, val, valsize);
}


static size_t
insert_dummy_value(FTNODE node, int bn, long k, uint32_t idx)
{
    char val[vallen];
    memset(val, k, sizeof val);
    le_add_to_bn(BLB_DATA(node, bn), idx,(char *) &k, keylen, val, vallen);
    return LE_CLEAN_MEMSIZE(vallen) + keylen + sizeof(uint32_t);
}

// TODO: this stuff should be in ft/ft-ops.cc, not in a test.
// it makes it incredibly hard to add things to an ftnode
// when tests hard code initializations...
static void
setup_ftnode_header(struct ftnode *node)
{
    node->flags = 0x11223344;
    node->thisnodename.b = 20;
    node->layout_version = FT_LAYOUT_VERSION;
    node->layout_version_original = FT_LAYOUT_VERSION;
    node->height = 0;
    node->dirty = 1;
    node->totalchildkeylens = 0;
    node->oldest_referenced_xid_known = TXNID_NONE;
    toku_init_dbt(&node->bound_l);
    toku_init_dbt(&node->bound_r);
}

static void
setup_ftnode_partitions(struct ftnode *node, int n_children, const MSN msn, size_t maxbnsize UU())
{
    node->n_children = n_children;
    node->max_msn_applied_to_node_on_disk = msn;
    MALLOC_N(node->n_children, node->bp);
    MALLOC_N(node->n_children - 1, node->childkeys);
    for (int bn = 0; bn < node->n_children; ++bn) {
        BP_STATE(node, bn) = PT_AVAIL;
        set_BLB(node, bn, toku_create_empty_bn());
        BLB_MAX_MSN_APPLIED(node, bn) = msn;
        toku_init_dbt(&BP_LIFT(node, bn));
        BP_PREFETCH_PFN(node,bn)    = NULL;
        BP_PREFETCH_PFN_CNT(node,bn) = 0;
        BP_PREFETCH_FLAG(node,bn) = false;
    }
}

static void
verify_basement_node_msns(FTNODE node, MSN expected)
{
    for(int i = 0; i < node->n_children; ++i) {
        assert(expected.msn == BLB_MAX_MSN_APPLIED(node, i).msn);
    }
}

//
// Maximum node size according to the BRT: 1024 (expected node size after split)
// Maximum basement node size: 256
// Actual node size before split: 2048
// Actual basement node size before split: 256
// Start by creating 8 basements, then split node, expected result of two nodes with 4 basements each.
static void
test_split_on_boundary(void)
{
    struct ftnode sn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    int fd = open(fname, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);


    setup_ftnode_header(&sn);
    const int nelts = 2 * nodesize / eltsize;
    setup_ftnode_partitions(&sn, nelts * eltsize / bnsize, dummy_msn_3884, bnsize);
    for (int bn = 0; bn < sn.n_children; ++bn) {
        long k;
        for (int i = 0; i < eltsperbn; ++i) {
            k = bn * eltsperbn + i;
            insert_dummy_value(&sn, bn, k, i);
        }
        if (bn < sn.n_children - 1) {
            toku_memdup_dbt(&sn.childkeys[bn], &k, sizeof k);
            sn.totalchildkeylens += (sizeof k);
        }
    }

    close(fd);
    CACHETABLE ct;
    FT_HANDLE brt;
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &brt, nodesize, bnsize, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun); assert(r==0);

    FTNODE nodea, nodeb;
    DBT splitk;
    // if we haven't done it right, we should hit the assert in the top of move_leafentries
    ft_split_leaf(brt->ft, &sn, &nodea, &nodeb, &splitk, true, SPLIT_EVENLY, 0, NULL);

    verify_basement_node_msns(nodea, dummy_msn_3884);
    verify_basement_node_msns(nodeb, dummy_msn_3884);

    toku_unpin_ftnode(brt->ft, nodeb);
    r = toku_close_ft_handle_nolsn(brt, NULL); assert(r == 0);
    toku_cachetable_close(&ct);

    if (splitk.data) {
        toku_free(splitk.data);
    }

    toku_destroy_ftnode_internals(&sn);
    // toku_destroy_ftnode_internals does not free bounds
    if (sn.bound_l.size != 0)
        toku_destroy_dbt(&sn.bound_l);
    if (sn.bound_r.size != 0)
        toku_destroy_dbt(&sn.bound_r);
}

//
// Maximum node size according to the BRT: 1024 (expected node size after split)
// Maximum basement node size: 256 (except the last)
// Actual node size before split: 4095
// Actual basement node size before split: 256 (except the last, of size 2K)
//
// Start by creating 9 basements, the first 8 being of 256 bytes each,
// and the last with one row of size 2047 bytes.  Then split node,
// expected result is two nodes, one with 8 basement nodes and one
// with 1 basement node.
static void
test_split_with_everything_on_the_left(void)
{
    struct ftnode sn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    int fd = open(fname, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);

    setup_ftnode_header(&sn);
    const int nelts = 2 * nodesize / eltsize;
    setup_ftnode_partitions(&sn, nelts * eltsize / bnsize + 1, dummy_msn_3884, 2 * nodesize);
    size_t big_val_size = 0;
    for (int bn = 0; bn < sn.n_children; ++bn) {
        long k;
        if (bn < sn.n_children - 1) {
            for (int i = 0; i < eltsperbn; ++i) {
                k = bn * eltsperbn + i;
                big_val_size += insert_dummy_value(&sn, bn, k, i);
            }
            toku_memdup_dbt(&sn.childkeys[bn], &k, sizeof k);
            sn.totalchildkeylens += (sizeof k);
        } else {
            k = bn * eltsperbn;
            // we want this to be as big as the rest of our data and a
            // little bigger, so the halfway mark will land inside this
            // value and it will be split to the left
            big_val_size += 100;
            char * XMALLOC_N(big_val_size, big_val);
            memset(big_val, k, big_val_size);
            le_add_to_bn(BLB_DATA(&sn, bn), 0, (char *) &k, keylen, big_val, big_val_size);
            toku_free(big_val);
        }
    }
    close(fd);
    CACHETABLE ct;
    FT_HANDLE brt;
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &brt, nodesize, bnsize, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun); assert(r==0);

    FTNODE nodea, nodeb;
    DBT splitk;
    // if we haven't done it right, we should hit the assert in the top of move_leafentries
    ft_split_leaf(brt->ft, &sn, &nodea, &nodeb, &splitk, true, SPLIT_EVENLY, 0, NULL);

    toku_unpin_ftnode(brt->ft, nodeb);
    r = toku_close_ft_handle_nolsn(brt, NULL); assert(r == 0);
    toku_cachetable_close(&ct);

    if (splitk.data) {
        toku_free(splitk.data);
    }

    toku_destroy_ftnode_internals(&sn);
    // toku_destroy_ftnode_internals does not free bounds
    if (sn.bound_l.size != 0)
        toku_destroy_dbt(&sn.bound_l);
    if (sn.bound_r.size != 0)
        toku_destroy_dbt(&sn.bound_r);

}

//
// Maximum node size according to the BRT: 1024 (expected node size after split)
// Maximum basement node size: 256 (except the last)
// Actual node size before split: 4095
// Actual basement node size before split: 256 (except the last, of size 2K)
//
// Start by creating 9 basements, the first 8 being of 256 bytes each,
// and the last with one row of size 2047 bytes.  Then split node,
// expected result is two nodes, one with 8 basement nodes and one
// with 1 basement node.
static void
test_split_on_boundary_of_last_node(void)
{
    struct ftnode sn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    int fd = open(fname, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);

    setup_ftnode_header(&sn);
    const int nelts = 2 * nodesize / eltsize;
    const size_t maxbnsize = 2 * nodesize;
    setup_ftnode_partitions(&sn, nelts * eltsize / bnsize + 1, dummy_msn_3884, maxbnsize);
    size_t big_val_size = 0;
    for (int bn = 0; bn < sn.n_children; ++bn) {
        long k;
        if (bn < sn.n_children - 1) {
            for (int i = 0; i < eltsperbn; ++i) {
                k = bn * eltsperbn + i;
                big_val_size += insert_dummy_value(&sn, bn, k, i);
            }
            toku_memdup_dbt(&sn.childkeys[bn], &k, sizeof k);
            sn.totalchildkeylens += (sizeof k);
        } else {
            k = bn * eltsperbn;
            // we want this to be slightly smaller than all the rest of
            // the data combined, so the halfway mark will be just to its
            // left and just this element will end up on the right of the split
            big_val_size -= 1 + (sizeof(((LEAFENTRY)NULL)->type)  // overhead from LE_CLEAN_MEMSIZE
                                 +sizeof(uint32_t) // sizeof(keylen)
                                 +sizeof(((LEAFENTRY)NULL)->u.clean.vallen));
            invariant(big_val_size <= maxbnsize);
            char * XMALLOC_N(big_val_size, big_val);
            memset(big_val, k, big_val_size);
            le_add_to_bn(BLB_DATA(&sn, bn), 0, (char *) &k, keylen, big_val, big_val_size);
            toku_free(big_val);
        }
    }
    close(fd);
    CACHETABLE ct;
    FT_HANDLE brt;
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &brt, nodesize, bnsize, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun); assert(r==0);

    FTNODE nodea, nodeb;
    DBT splitk;
    // if we haven't done it right, we should hit the assert in the top of move_leafentries
    ft_split_leaf(brt->ft, &sn, &nodea, &nodeb, &splitk, true, SPLIT_EVENLY, 0, NULL);

    toku_unpin_ftnode(brt->ft, nodeb);
    r = toku_close_ft_handle_nolsn(brt, NULL); assert(r == 0);
    toku_cachetable_close(&ct);

    if (splitk.data) {
        toku_free(splitk.data);
    }

    toku_destroy_ftnode_internals(&sn);
    // toku_destroy_ftnode_internals does not free bounds
    if (sn.bound_l.size != 0)
        toku_destroy_dbt(&sn.bound_l);
    if (sn.bound_r.size != 0)
        toku_destroy_dbt(&sn.bound_r);

}

static void
test_split_at_begin(void)
{
    struct ftnode sn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    int fd = open(fname, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);

    setup_ftnode_header(&sn);
    const int nelts = 2 * nodesize / eltsize;
    const size_t maxbnsize = 2 * nodesize;
    setup_ftnode_partitions(&sn, nelts * eltsize / bnsize, dummy_msn_3884, maxbnsize);
    size_t totalbytes = 0;
    for (int bn = 0; bn < sn.n_children; ++bn) {
        long k;
        for (int i = 0; i < eltsperbn; ++i) {
            k = bn * eltsperbn + i;
            if (bn == 0 && i == 0) {
                // we'll add the first element later when we know how big
                // to make it
                continue;
            }
            totalbytes += insert_dummy_value(&sn, bn, k, i-1);
        }
        if (bn < sn.n_children - 1) {
            toku_memdup_dbt(&sn.childkeys[bn], &k, sizeof k);
            sn.totalchildkeylens += (sizeof k);
        }
    }
    {  // now add the first element
        int bn = 0; long k = 0;
        // add a few bytes so the halfway mark is definitely inside this
        // val, which will make it go to the left and everything else to
        // the right
        char val[totalbytes + 3];
        invariant(totalbytes + 3 <= maxbnsize);
        memset(val, k, sizeof val);
        le_add_to_bn(BLB_DATA(&sn, bn), 0, (char *) &k, keylen, val, totalbytes + 3);
        totalbytes += LE_CLEAN_MEMSIZE(totalbytes + 3) + keylen + sizeof(uint32_t);
    }

    close(fd);
    CACHETABLE ct;
    FT_HANDLE brt;
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &brt, nodesize, bnsize, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun); assert(r==0);

    FTNODE nodea, nodeb;
    DBT splitk;
    // if we haven't done it right, we should hit the assert in the top of move_leafentries
    ft_split_leaf(brt->ft, &sn, &nodea, &nodeb, &splitk, true, SPLIT_EVENLY, 0, NULL);

    toku_unpin_ftnode(brt->ft, nodeb);
    r = toku_close_ft_handle_nolsn(brt, NULL); assert(r == 0);
    toku_cachetable_close(&ct);

    if (splitk.data) {
        toku_free(splitk.data);
    }

    toku_destroy_ftnode_internals(&sn);
    // toku_destroy_ftnode_internals does not free bounds
    if (sn.bound_l.size != 0)
        toku_destroy_dbt(&sn.bound_l);
    if (sn.bound_r.size != 0)
        toku_destroy_dbt(&sn.bound_r);

}

static void
test_split_at_end(void)
{
    struct ftnode sn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    int fd = open(fname, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO); assert(fd >= 0);

    setup_ftnode_header(&sn);
    const int nelts = 2 * nodesize / eltsize;
    const size_t maxbnsize = 2 * nodesize;
    setup_ftnode_partitions(&sn, nelts * eltsize / bnsize, dummy_msn_3884, maxbnsize);
    long totalbytes = 0;
    int bn, i;
    for (bn = 0; bn < sn.n_children; ++bn) {
        long k;
        for (i = 0; i < eltsperbn; ++i) {
            k = bn * eltsperbn + i;
            if (bn == sn.n_children - 1 && i == eltsperbn - 1) {
                // add a few bytes so the halfway mark is definitely inside this
                // val, which will make it go to the left and everything else to
                // the right, which is nothing, so we actually split at the very end
                char val[totalbytes + 3];
                invariant(totalbytes + 3 <= (long) maxbnsize);
                memset(val, k, sizeof val);
                le_add_to_bn(BLB_DATA(&sn, bn), i, (char *) &k, keylen, val, totalbytes + 3);
                totalbytes += LE_CLEAN_MEMSIZE(totalbytes + 3) + keylen + sizeof(uint32_t);
            } else {
                totalbytes += insert_dummy_value(&sn, bn, k, i);
            }
        }
        if (bn < sn.n_children - 1) {
            toku_memdup_dbt(&sn.childkeys[bn], &k, sizeof k);
            sn.totalchildkeylens += (sizeof k);
        }
    }

    close(fd);
    CACHETABLE ct;
    FT_HANDLE brt;
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &brt, nodesize, bnsize, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun); assert(r==0);

    FTNODE nodea, nodeb;
    DBT splitk;
    // if we haven't done it right, we should hit the assert in the top of move_leafentries
    ft_split_leaf(brt->ft, &sn, &nodea, &nodeb, &splitk, true, SPLIT_EVENLY, 0, NULL);

    toku_unpin_ftnode(brt->ft, nodeb);
    r = toku_close_ft_handle_nolsn(brt, NULL); assert(r == 0);
    toku_cachetable_close(&ct);

    if (splitk.data) {
        toku_free(splitk.data);
    }

    toku_destroy_ftnode_internals(&sn);
    // toku_destroy_ftnode_internals does not free bounds
    if (sn.bound_l.size != 0)
        toku_destroy_dbt(&sn.bound_l);
    if (sn.bound_r.size != 0)
        toku_destroy_dbt(&sn.bound_r);

}

// Maximum node size according to the BRT: 1024 (expected node size after split)
// Maximum basement node size: 256
// Actual node size before split: 2048
// Actual basement node size before split: 256
// Start by creating 9 basements, then split node.
// Expected result of two nodes with 5 basements each.
static void
test_split_odd_nodes(void)
{
    struct ftnode sn;

    int r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU); assert(r == 0);

    int fd = open(fname, O_RDWR|O_CREAT|O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO);
    assert(fd >= 0);

    setup_ftnode_header(&sn);
    // This will give us 9 children.
    const int nelts = 2 * (nodesize + 128) / eltsize;
    setup_ftnode_partitions(&sn, nelts * eltsize / bnsize, dummy_msn_3884, bnsize);
    for (int bn = 0; bn < sn.n_children; ++bn) {
        long k;
        for (int i = 0; i < eltsperbn; ++i) {
            k = bn * eltsperbn + i;
            insert_dummy_value(&sn, bn, k, i);
        }
        if (bn < sn.n_children - 1) {
            toku_memdup_dbt(&sn.childkeys[bn], &k, sizeof k);
            sn.totalchildkeylens += (sizeof k);
        }
    }

    close(fd);
    CACHETABLE ct;
    FT_HANDLE brt;
    toku_cachetable_create(&ct, 0, ZERO_LSN, NULL_LOGGER);
    r = toku_open_ft_handle(fname, 1, &brt, nodesize, bnsize, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun); assert(r==0);

    FTNODE nodea, nodeb;
    DBT splitk;
    // if we haven't done it right, we should hit the assert in the top of move_leafentries
    ft_split_leaf(brt->ft, &sn, &nodea, &nodeb, &splitk, true, SPLIT_EVENLY, 0, NULL);

    verify_basement_node_msns(nodea, dummy_msn_3884);
    verify_basement_node_msns(nodeb, dummy_msn_3884);

    toku_unpin_ftnode(brt->ft, nodeb);
    r = toku_close_ft_handle_nolsn(brt, NULL); assert(r == 0);
    toku_cachetable_close(&ct);

    if (splitk.data) {
        toku_free(splitk.data);
    }

    toku_destroy_ftnode_internals(&sn);
    // toku_destroy_ftnode_internals does not free bounds
    if (sn.bound_l.size != 0)
        toku_destroy_dbt(&sn.bound_l);
    if (sn.bound_r.size != 0)
        toku_destroy_dbt(&sn.bound_r);

}

extern "C" int test_3884(void);
int
test_3884 (void) {
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);
    fname = TOKU_TEST_FILENAME_DATA;

    test_split_on_boundary();
    test_split_with_everything_on_the_left();
    test_split_on_boundary_of_last_node();
    test_split_at_begin();
    test_split_at_end();
    test_split_odd_nodes();

    toku_ft_layer_destroy();
#ifdef __SUPPORT_DIRECT_IO
    printf("\n INFO: direct io is turned on \n");
#endif

    return 0;
}
