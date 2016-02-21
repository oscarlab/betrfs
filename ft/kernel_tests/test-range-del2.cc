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
#ident "$Id$"
/* The goal of this test.  Make sure that inserts stay behind deletes. */


#include "test.h"

#include <ft-cachetable-wrappers.h>
#include "ft-flusher.h"
#include "ft-flusher-internal.h"
#include "ft/checkpoint.h"

/* test-range-del has tested the simple case how the multicast is applied to leaf node for "fully covered" case . 
 * For test-range-del2 the matched kv are spanned on diff nodes, so which children to pick as the multicast bounds through pivots is exercised.  
 * tested three cases, as a complimentary to "fully covered" case.
 * (1) left overlap with [min_key, max_key]
 * (2) right overlap with [min_key, max_key]
 * (3) no overlap
 * and the corner case of using multicast to delete just a single element, at the start and end of buffer.
 * -JYM
 * */
static TOKUTXN const null_txn = 0;

enum { NODESIZE = 1024, KSIZE=NODESIZE-100, TOKU_PSIZE=20 };

static CACHETABLE ct;
static FT_HANDLE ft;
static const char *fname;

static int num_flushes_called;
static void update_status(FTNODE UU(child), int UU(dirtied), void* UU(extra)) {
    num_flushes_called++;
}
static int child_to_flush(FT UU(h), FTNODE parent, void* UU(extra)) {
    // internal node has 3 children
    if (parent->height == 1) {
        assert(parent->n_children == 3);
        return 2; //only flush child 2.
    }
    // root has 1 child
    else if (parent->height == 2) {
        assert(parent->n_children == 1);
        return 0;
    }
    else {
        assert(false);
    }
}

static bool always_flush(FTNODE UU(child), void* UU(extra)) {
    return true;
}

static bool
dont_destroy_bn(void* UU(extra))
{
    return false;
}


static void dont_merge(struct flusher_advice* UU(fa),
                              FT UU(h),
                              FTNODE UU(parent),
                              int UU(childnum),
                              FTNODE UU(child),
                              void* UU(extra))
{
    toku_unpin_ftnode(h,parent);
    toku_unpin_ftnode(h,child);
    return; // we do nothing but unlock the parent and the child
}

static void
doit (void) {
    BLOCKNUM node_leaf[3];
    BLOCKNUM node_internal, node_root;

    int r;
    
    toku_cachetable_create(&ct, 500*1024*1024, ZERO_LSN, nullptr);
    unlink(fname);
    r = toku_open_ft_handle(fname, 1, &ft, NODESIZE, NODESIZE/2, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert(r==0);

   
    toku_testsetup_initialize();  // must precede any other toku_testsetup calls

    r = toku_testsetup_leaf(ft, &node_leaf[0], 1, NULL, NULL);
    assert(r==0);
    r = toku_testsetup_leaf(ft, &node_leaf[1], 1, NULL, NULL);
    assert(r==0);
    r = toku_testsetup_leaf(ft, &node_leaf[2], 1, NULL, NULL);
    assert(r==0);

    char* pivots[2];
    pivots[0] = toku_strdup("/k/3");
    pivots[1] = toku_strdup("/m");
    int pivot_lens[2];
    pivot_lens[0] = 5;
    pivot_lens[1] = 3;

    r = toku_testsetup_nonleaf(ft, 1, &node_internal, 3, node_leaf, pivots, pivot_lens);
    assert(r==0);

    r = toku_testsetup_nonleaf(ft, 2, &node_root, 1, &node_internal, 0, 0);
    assert(r==0);

    r = toku_testsetup_root(ft, node_root);
    assert(r==0);

    //
    // at this point we have created a tree with a root, an internal node,
    // and three leaf nodes, the pivot being "/k/3" and "/m/1"

    // now we insert some rows into each leaf node
    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[0], 
        "/a/1", // key
        5, // keylen
        "a1", 
        3,
	FT_INSERT
        );
    assert(r==0);

    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[0], 
        "/a/2", // key
        5, // keylen
        "a2", 
        3,
	FT_INSERT
        );
    assert(r==0);

    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[0], 
        "/k/1", // key
        5, // keylen
        "k1", 
        3,
	FT_INSERT
        );
    assert(r==0);
    
    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[0], 
        "/k/2", // key
        5, // keylen
        "k2", 
        3,
	FT_INSERT
        );
    assert(r==0);
    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[1], 
        "/k/4", // key
        5, // keylen
        "k4", 
        3,
	FT_INSERT
        );
    assert(r==0);

    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[1], 
        "/k/5", // key
        5, // keylen
        "k5", 
        3,
	FT_INSERT
        );
    assert(r==0);

    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[1], 
        "/l/1", // key
        5, // keylen
        "l1", 
        3,
	FT_INSERT
        );
    assert(r==0);
    
    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[1], 
        "/l/2", // key
        5, // keylen
        "l2", 
        3,
	FT_INSERT
        );
    assert(r==0);
 
    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[2], 
        "/z/1", // key
        5, // keylen
        "z1", 
        3,
	FT_INSERT
        );
    assert(r==0);
    
    r = toku_testsetup_insert_to_leaf (
        ft, 
        node_leaf[2], 
        "/z/2", // key
        5, // keylen
        "z2", 
        3,
	FT_INSERT
        );
    assert(r==0);

    // now do lookups on some of the keys
    ft_lookup_and_check_nodup(ft, "/k/1", "k1");
    ft_lookup_and_check_nodup(ft, "/k/4", "k4");

    //do we have to run a cp here? It should not matter.
    CHECKPOINTER cp = toku_cachetable_get_checkpointer(ct);
    r = toku_checkpoint(cp, NULL, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT, false);
    assert_zero(r);
     
    r = toku_testsetup_insert_to_nonleaf (
        ft,
        node_root,
        FT_DELETE_MULTICAST,
        "/k",
        5,
        "/k/8",
        5,
        false,
	PM_UNCOMMITTED,
        NULL,
        0
        );
    assert(r==0);
 

    // check that lookups on the two keys and they should be gone.

    char key[5] = "/k/1";
    ft_lookup_and_fail_nodup(ft, key);

    key[3] = '2';
    ft_lookup_and_fail_nodup(ft, key);

    key[3] = '4';
    ft_lookup_and_fail_nodup(ft, key);

    key[3] = '5';
    ft_lookup_and_fail_nodup(ft, key);

    //probe the survivors
    ft_lookup_and_check_nodup(ft, "/a/1", "a1");
    ft_lookup_and_check_nodup(ft, "/l/2", "l2");
    ft_lookup_and_check_nodup(ft, "/z/2", "z2");
    
    //now instead of inserting msg, we will try to flush multicast msg top-down. I am a bit paranoid because ft_nonleaf_msg_multiple is never exercised. We only brought nodes up-to-date by looking up. -JYM

      r = toku_testsetup_insert_to_nonleaf (
        ft,
        node_root,
        FT_DELETE_MULTICAST,
        "/a/1",
        5,
        "/a/1",
        5,
        false,
	PM_UNCOMMITTED,
        NULL,
        0
        );
    assert(r==0);

    r = toku_testsetup_insert_to_nonleaf (
        ft,
        node_root,
        FT_DELETE_MULTICAST,
        "/z/2",
        5,
        "/z/2",
        5,
        false,
	PM_UNCOMMITTED,
        NULL,
        0
        );
    assert(r==0);

    // now run a checkpoint to get everything clean
    //
    cp = toku_cachetable_get_checkpointer(ct);
    r = toku_checkpoint(cp, NULL, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT, false);
    assert_zero(r);
 
    FTNODE node = NULL;
    toku_pin_node_with_min_bfe(&node, node_root, ft);
    toku_assert_entire_node_in_memory(node);
    assert(node->n_children == 1);
    assert(!node->dirty);
    assert(toku_bnc_n_entries(node->bp[0].ptr.u.nonleaf) > 0);
    
   struct flusher_advice fa;
    flusher_advice_init(
        &fa,
        child_to_flush,
        dont_destroy_bn,
        always_flush,
        dont_merge,  
        update_status,
	default_pick_child_after_split, // do not care, not going to happen.
        NULL
        );

       
    num_flushes_called = 0;
    toku_ft_flush_some_child(ft->ft, node, &fa);
    assert(num_flushes_called == 2);

    toku_pin_node_with_min_bfe(&node, node_root, ft);
    toku_assert_entire_node_in_memory(node);
    assert(node->dirty);
    assert(node->n_children == 1);
    assert(toku_bnc_n_entries(node->bp[0].ptr.u.nonleaf) == 0);
    toku_unpin_ftnode(ft->ft, node);

    toku_pin_node_with_min_bfe(&node, node_internal,ft);
    toku_assert_entire_node_in_memory(node);
    assert(node->dirty);
    assert(node->n_children == 3);
    // child 0 should have 2 msgs because no flush for it. 
    // child 1 should have 1 msg in buffer because only 1 msg applies
    // child 2 should have empty buffer because of flush.
        assert(toku_bnc_n_entries(node->bp[0].ptr.u.nonleaf) == 2); //not flushed
    assert(toku_bnc_n_entries(node->bp[1].ptr.u.nonleaf) == 1); // one msg
    assert(toku_bnc_n_entries(node->bp[2].ptr.u.nonleaf) == 0); //flushed, empty
    toku_unpin_ftnode(ft->ft, node);

    //exsitence check.
    char key2[5] = "/a/1";
    ft_lookup_and_fail_nodup(ft, key2);
    char key3[5] = "/z/2";
    ft_lookup_and_fail_nodup(ft, key3); 
    ft_lookup_and_check_nodup(ft, "/l/1", "l1");
    //cleaning up
    r = toku_close_ft_handle_nolsn(ft, 0);     assert(r==0);
    toku_cachetable_close(&ct);
    toku_free(pivots[0]);
    toku_free(pivots[1]);
}

extern "C" int test_range_del2(void);
int
test_range_del2 (void) {
    initialize_dummymsn();
    int rinit = toku_ft_layer_init();
    CKERR(rinit);
    fname = TOKU_TEST_FILENAME;
    num_flushes_called = 0;
    doit();
    toku_ft_layer_destroy();
    return 0;
}
