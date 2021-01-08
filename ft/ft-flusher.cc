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

#include "log-internal.h"
#include <ft-internal.h>
#include <ft-flusher.h>
#include <ft-flusher-internal.h>
#include <ft-cachetable-wrappers.h>
#include <ft.h>
#include <toku_assert.h>
#include <portability/toku_atomic.h>
#include <util/status.h>

/* Status is intended for display to humans to help understand system behavior.
 * It does not need to be perfectly thread-safe.
 */
static FT_FLUSHER_STATUS_S ft_flusher_status;

#define STATUS_INIT(k,c,t,l,inc) TOKUDB_STATUS_INIT(ft_flusher_status, k, c, t, "brt flusher: " l, inc)

#define STATUS_VALUE(x) ft_flusher_status.status[x].value.num
void toku_ft_flusher_status_init(void) {
    // Note,                                                                     this function initializes the keyname,  type, and legend fields.
    // Value fields are initialized to zero by compiler.
    STATUS_INIT(FT_FLUSHER_CLEANER_TOTAL_NODES,                nullptr, UINT64, "total nodes potentially flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_H1_NODES,                   nullptr, UINT64, "height-one nodes flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_HGT1_NODES,                 nullptr, UINT64, "height-greater-than-one nodes flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_EMPTY_NODES,                nullptr, UINT64, "nodes cleaned which had empty buffers", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_NODES_DIRTIED,              nullptr, UINT64, "nodes dirtied by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_MAX_BUFFER_SIZE,            nullptr, UINT64, "max bytes in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_MIN_BUFFER_SIZE,            nullptr, UINT64, "min bytes in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_TOTAL_BUFFER_SIZE,          nullptr, UINT64, "total bytes in buffers flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_MAX_BUFFER_WORKDONE,        nullptr, UINT64, "max workdone in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_MIN_BUFFER_WORKDONE,        nullptr, UINT64, "min workdone in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_TOTAL_BUFFER_WORKDONE,      nullptr, UINT64, "total workdone in buffers flushed by cleaner thread", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_STARTED,    nullptr, UINT64, "times cleaner thread tries to merge a leaf", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_RUNNING,    nullptr, UINT64, "cleaner thread leaf merges in progress", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_COMPLETED,  nullptr, UINT64, "cleaner thread leaf merges successful", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_CLEANER_NUM_DIRTIED_FOR_LEAF_MERGE, nullptr, UINT64, "nodes dirtied by cleaner thread leaf merges", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_TOTAL,                        nullptr, UINT64, "total number of flushes done by flusher threads or cleaner threads", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_IN_MEMORY,                    nullptr, UINT64, "number of in memory flushes", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_NEEDED_IO,                    nullptr, UINT64, "number of flushes that read something off disk", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES,                     nullptr, UINT64, "number of flushes that triggered another flush in child", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_1,                   nullptr, UINT64, "number of flushes that triggered 1 cascading flush", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_2,                   nullptr, UINT64, "number of flushes that triggered 2 cascading flushes", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_3,                   nullptr, UINT64, "number of flushes that triggered 3 cascading flushes", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_4,                   nullptr, UINT64, "number of flushes that triggered 4 cascading flushes", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_5,                   nullptr, UINT64, "number of flushes that triggered 5 cascading flushes", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_GT_5,                nullptr, UINT64, "number of flushes that triggered over 5 cascading flushes", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_SPLIT_LEAF,                         nullptr, UINT64, "leaf node splits", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_SPLIT_NONLEAF,                      nullptr, UINT64, "nonleaf node splits", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_MERGE_LEAF,                         nullptr, UINT64, "leaf node merges", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_MERGE_NONLEAF,                      nullptr, UINT64, "nonleaf node merges", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_FLUSHER_BALANCE_LEAF,                       nullptr, UINT64, "leaf node balances", TOKU_ENGINE_STATUS);

    STATUS_VALUE(FT_FLUSHER_CLEANER_MIN_BUFFER_SIZE) = UINT64_MAX;
    STATUS_VALUE(FT_FLUSHER_CLEANER_MIN_BUFFER_WORKDONE) = UINT64_MAX;

    ft_flusher_status.initialized = true;
}
#undef STATUS_INIT

void toku_ft_flusher_get_status(FT_FLUSHER_STATUS status) {
    if (!ft_flusher_status.initialized) {
        toku_ft_flusher_status_init();
    }
    *status = ft_flusher_status;
}

//
// For test purposes only.
// These callbacks are never used in production code, only as a way
//  to test the system (for example, by causing crashes at predictable times).
//
static void (*flusher_thread_callback)(int, void*) = NULL;
static void *flusher_thread_callback_extra = NULL;

void toku_flusher_thread_set_callback(void (*callback_f)(int, void*),
                                      void* extra) {
    flusher_thread_callback = callback_f;
    flusher_thread_callback_extra = extra;
}

static void call_flusher_thread_callback(int flt_state) {
    if (flusher_thread_callback) {
        flusher_thread_callback(flt_state, flusher_thread_callback_extra);
    }
}

static int
find_heaviest_child(FTNODE node)
{
    int max_child = 0;
    unsigned long max_weight = toku_bnc_nbytesinbuf(BNC(node, 0)) + BP_WORKDONE(node, 0);
    int i;

    assert(node->n_children > 0);
    for (i=1; i < node->n_children; i++) {
#ifdef TOKU_DEBUG_PARANOID
        if (BP_WORKDONE(node, i)) {
            assert(toku_bnc_nbytesinbuf(BNC(node, i)) > 0);
        }
#endif
        unsigned long bytes = toku_bnc_nbytesinbuf(BNC(node, i));
        unsigned long workdone = BP_WORKDONE(node, i);
        unsigned long this_weight = bytes + workdone;

        if (max_weight < this_weight) {
            max_child = i;
            max_weight = this_weight;
        }
    }

    return max_child;
}

static void
update_flush_status(FTNODE child, int cascades) {
    STATUS_VALUE(FT_FLUSHER_FLUSH_TOTAL)++;
    if (cascades > 0) {
        STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES)++;
        switch (cascades) {
        case 1:
            STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES_1)++; break;
        case 2:
            STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES_2)++; break;
        case 3:
            STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES_3)++; break;
        case 4:
            STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES_4)++; break;
        case 5:
            STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES_5)++; break;
        default:
            STATUS_VALUE(FT_FLUSHER_FLUSH_CASCADES_GT_5)++; break;
        }
    }
    bool flush_needs_io = false;
    for (int i = 0; !flush_needs_io && i < child->n_children; ++i) {
        if (BP_STATE(child, i) == PT_ON_DISK) {
            flush_needs_io = true;
        }
    }
    if (flush_needs_io) {
        STATUS_VALUE(FT_FLUSHER_FLUSH_NEEDED_IO)++;
    } else {
        STATUS_VALUE(FT_FLUSHER_FLUSH_IN_MEMORY)++;
    }
}

static void
maybe_destroy_child_blbs(FTNODE node, FTNODE child, FT h)
{
    // If the node is already fully in memory, as in upgrade, we don't
    // need to destroy the basement nodes because they are all equally
    // up to date.
    if (child->n_children > 1 &&
        child->height == 0 &&
        !child->dirty) {
        for (int i = 0; i < child->n_children; ++i) {
            if (BP_STATE(child, i) == PT_AVAIL &&
                node->max_msn_applied_to_node_on_disk.msn < BLB_MAX_MSN_APPLIED(child, i).msn)
            {
                toku_evict_bn_from_memory(child, i, h);
            }
        }
    }
}

static void
ft_merge_child(
    FT h,
    FTNODE node,
    int childnum_to_merge,
    bool *did_react,
    struct flusher_advice *fa);

static int
pick_heaviest_child(FT UU(h),
                    FTNODE parent,
                    void* UU(extra))
{
    int childnum = find_heaviest_child(parent);
    paranoid_invariant(toku_bnc_n_entries(BNC(parent, childnum))>0);
    return childnum;
}

bool
dont_destroy_basement_nodes(void* UU(extra))
{
    return false;
}

static bool
do_destroy_basement_nodes(void* UU(extra))
{
    return true;
}

bool
always_recursively_flush(FTNODE UU(child), void* UU(extra))
{
    return true;
}

bool
never_recursively_flush(FTNODE UU(child), void* UU(extra))
{
    return false;
}

/**
 * Flusher thread ("normal" flushing) implementation.
 */
struct flush_status_update_extra {
    int cascades;
    uint32_t nodesize;
};

static bool
recurse_if_child_is_gorged(FTNODE child, void* extra)
{
    struct flush_status_update_extra *fste = (flush_status_update_extra *)extra;
    return toku_ft_nonleaf_is_gorged(child, fste->nodesize);
}

int
default_pick_child_after_split(FT UU(h),
                               FTNODE UU(parent),
                               int UU(childnuma),
                               int UU(childnumb),
                               void* UU(extra))
{
    return -1;
}

void
default_merge_child(struct flusher_advice *fa,
                    FT h,
                    FTNODE parent,
                    int childnum,
                    FTNODE child,
                    void* UU(extra))
{
    //
    // There is probably a way to pass FTNODE child
    // into ft_merge_child, but for simplicity for now,
    // we are just going to unpin child and
    // let ft_merge_child pin it again
    //
    toku_unpin_ftnode_off_client_thread(h, child);
    //
    //
    // it is responsibility of ft_merge_child to unlock parent
    //
    bool did_react;
    ft_merge_child(h, parent, childnum, &did_react, fa);
}

void
flusher_advice_init(
    struct flusher_advice *fa,
    FA_PICK_CHILD pick_child,
    FA_SHOULD_DESTROY_BN should_destroy_basement_nodes,
    FA_SHOULD_RECURSIVELY_FLUSH should_recursively_flush,
    FA_MAYBE_MERGE_CHILD maybe_merge_child,
    FA_UPDATE_STATUS update_status,
    FA_PICK_CHILD_AFTER_SPLIT pick_child_after_split,
    void* extra
    )
{
    fa->pick_child = pick_child;
    fa->should_destroy_basement_nodes = should_destroy_basement_nodes;
    fa->should_recursively_flush = should_recursively_flush;
    fa->maybe_merge_child = maybe_merge_child;
    fa->update_status = update_status;
    fa->pick_child_after_split = pick_child_after_split;
    fa->extra = extra;
}

static void
flt_update_status(FTNODE child,
                 int UU(dirtied),
                 void* extra)
{
    struct flush_status_update_extra *fste = (struct flush_status_update_extra *) extra;
    update_flush_status(child, fste->cascades);
    // If `toku_ft_flush_some_child` decides to recurse after this, we'll need
    // cascades to increase.  If not it doesn't matter.
    fste->cascades++;
}

static void
flt_flusher_advice_init(struct flusher_advice *fa, struct flush_status_update_extra *fste, uint32_t nodesize)
{
    fste->cascades = 0;
    fste->nodesize = nodesize;
    flusher_advice_init(fa,
                        pick_heaviest_child,
                        dont_destroy_basement_nodes,
                        recurse_if_child_is_gorged,
                        default_merge_child,
                        flt_update_status,
                        default_pick_child_after_split,
                        fste);
}

struct ctm_extra {
    bool is_last_child;
    DBT target_key;
};

static int
ctm_pick_child(FT h,
               FTNODE parent,
               void* extra)
{
    struct ctm_extra* ctme = (struct ctm_extra *) extra;
    int childnum;
    if (parent->height == 1 && ctme->is_last_child) {
        childnum = parent->n_children - 1;
    }
    else {
        childnum = toku_ftnode_which_child(
            parent,
            &ctme->target_key,
            &h->cmp_descriptor,
            h->key_ops.keycmp);
    }
    return childnum;
}

static void
ctm_update_status(
    FTNODE UU(child),
    int dirtied,
    void* UU(extra)
    )
{
    STATUS_VALUE(FT_FLUSHER_CLEANER_NUM_DIRTIED_FOR_LEAF_MERGE) += dirtied;
}

static void
ctm_maybe_merge_child(struct flusher_advice *fa,
                      FT h,
                      FTNODE parent,
                      int childnum,
                      FTNODE child,
                      void *extra)
{
    if (child->height == 0) {
        (void) toku_sync_fetch_and_add(&STATUS_VALUE(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_COMPLETED), 1);
    }
    default_merge_child(fa, h, parent, childnum, child, extra);
}

static void
ct_maybe_merge_child(struct flusher_advice *fa,
                     FT h,
                     FTNODE parent,
                     int childnum,
                     FTNODE child,
                     void* extra)
{
    if (child->height > 0) {
        default_merge_child(fa, h, parent, childnum, child, extra);
    }
    else {
        struct ctm_extra ctme;
        paranoid_invariant(parent->n_children > 1);
        int pivot_to_save;
        //
        // we have two cases, one where the childnum
        // is the last child, and therefore the pivot we
        // save is not of the pivot which we wish to descend
        // and another where it is not the last child,
        // so the pivot is sufficient for identifying the leaf
        // to be merged
        //
        if (childnum == (parent->n_children - 1)) {
            ctme.is_last_child = true;
            pivot_to_save = childnum - 1;
        }
        else {
            ctme.is_last_child = false;
            pivot_to_save = childnum;
        }
        const DBT *pivot = &parent->childkeys[pivot_to_save];
        toku_clone_dbt(&ctme.target_key, *pivot);

        // at this point, ctme is properly setup, now we can do the merge
        struct flusher_advice new_fa;
        flusher_advice_init(
            &new_fa,
            ctm_pick_child,
            dont_destroy_basement_nodes,
            always_recursively_flush,
            ctm_maybe_merge_child,
            ctm_update_status,
            default_pick_child_after_split,
            &ctme);

        toku_unpin_ftnode_off_client_thread(h, parent);
        toku_unpin_ftnode_off_client_thread(h, child);

        FTNODE root_node = NULL;
        {
            uint32_t fullhash;
            CACHEKEY root;
            toku_calculate_root_offset_pointer(h, &root, &fullhash);
            struct ftnode_fetch_extra bfe;
            fill_bfe_for_full_read(&bfe, h);
            toku_pin_ftnode_off_client_thread(h, root, fullhash, &bfe, PL_WRITE_EXPENSIVE, 0, NULL, &root_node);
            toku_assert_entire_node_in_memory(root_node);
        }

        (void) toku_sync_fetch_and_add(&STATUS_VALUE(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_STARTED), 1);
        (void) toku_sync_fetch_and_add(&STATUS_VALUE(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_RUNNING), 1);

        toku_ft_flush_some_child(h, root_node, &new_fa);

        (void) toku_sync_fetch_and_sub(&STATUS_VALUE(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_RUNNING), 1);

        toku_destroy_dbt(&ctme.target_key);
    }
}

static void
ct_update_status(FTNODE child,
                 int dirtied,
                 void* extra)
{
    struct flush_status_update_extra* fste = (struct flush_status_update_extra *) extra;
    update_flush_status(child, fste->cascades);
    STATUS_VALUE(FT_FLUSHER_CLEANER_NODES_DIRTIED) += dirtied;
    // Incrementing this in case `toku_ft_flush_some_child` decides to recurse.
    fste->cascades++;
}

static void
ct_flusher_advice_init(struct flusher_advice *fa, struct flush_status_update_extra* fste, uint32_t nodesize)
{
    fste->cascades = 0;
    fste->nodesize = nodesize;
    flusher_advice_init(fa,
                        pick_heaviest_child,
                        do_destroy_basement_nodes,
                        recurse_if_child_is_gorged,
                        ct_maybe_merge_child,
                        ct_update_status,
                        default_pick_child_after_split,
                        fste);
}

//
// This returns true if the node MAY be reactive,
// false is we are absolutely sure that it is NOT reactive.
// The reason for inaccuracy is that the node may be
// a leaf node that is not entirely in memory. If so, then
// we cannot be sure if the node is reactive.
//
static bool may_node_be_reactive(FTNODE node)
{
    if (node->height == 0) {
        return true;
    } else {
        return (get_nonleaf_reactivity(node) != RE_STABLE);
    }
}

// relift a key when lifting changes
// the key is lifted by old_lifted, and now we want to set lifted part by
// new_lifted
int
toku_ft_relift_key(FT ft, DBT *key, DBT *old_lift, DBT *new_lift)
{
    int r = 0;
    DBT tmp;

    if (old_lift->size != 0) {
        r = toku_ft_unlift_key(ft, &tmp, key, old_lift);
        if (r)
            return r;
        toku_destroy_dbt(key);
    } else {
        toku_copy_dbt(&tmp, *key);
    }

    if (new_lift->size != 0) {
        r = toku_ft_lift_key(ft, key, &tmp, new_lift);
        toku_destroy_dbt(&tmp);
    } else {
        toku_copy_dbt(key, tmp);
    }

    return r;
}

static void
toku_ft_nonl_setup_lift(FT ft, FTNODE node)
{
    int r;
    if (node->n_children == 1) {
        toku_cleanup_dbt(&BP_LIFT(node, 0));
        r = toku_ft_lift(ft, &BP_LIFT(node, 0), &node->bound_l, &node->bound_r);
        assert_zero(r);
        return;
    }
    // n_children > 1
    toku_cleanup_dbt(&BP_LIFT(node, 0));
    r = toku_ft_lift(ft, &BP_LIFT(node, 0), &node->bound_l, &node->childkeys[0]);
    assert_zero(r);
    toku_cleanup_dbt(&BP_LIFT(node, node->n_children - 1));
    r = toku_ft_lift(ft, &BP_LIFT(node, node->n_children - 1),
                     &node->childkeys[node->n_children - 2], &node->bound_r);
    assert_zero(r);
    for (int i = 1; i < node->n_children - 1; i++) {
        toku_cleanup_dbt(&BP_LIFT(node, i));
        r = toku_ft_lift(ft, &BP_LIFT(node, i),
                         &node->childkeys[i - 1], &node->childkeys[i]);
        assert_zero(r);
    }
}

// Relift a nonleaf node:
// 1. Relift pivots (including bound_l and bound_r) and also BP_LIFT (because
//    pivots decides lifting)
// 2. We don't need to relift messages, because all messages are stored in BNCs
//    and have been lifted by BP_LIFT. Reliting pivots would not change the
//    logical contents of pivots (with all lifted prefixes), thus the total
//    lifted prefixes of those messages don't change
static int
toku_ft_nonleaf_relift(FT ft, FTNODE node, DBT *old_lift, DBT *new_lift)
{
    int r;

    node->totalchildkeylens = 0;
    if (node->bound_l.size != 0) {
        r = toku_ft_relift_key(ft, &node->bound_l, old_lift, new_lift);
        assert_zero(r);
        node->totalchildkeylens += node->bound_l.size;
    }
    if (node->bound_r.size != 0) {
        r = toku_ft_relift_key(ft, &node->bound_r, old_lift, new_lift);
        assert_zero(r);
        node->totalchildkeylens += node->bound_r.size;
    }
    for (int i = 0; i < node->n_children - 1; i++) {
        r = toku_ft_relift_key(ft, &node->childkeys[i], old_lift, new_lift);
        assert_zero(r);
        node->totalchildkeylens += node->childkeys[i].size;
    }
    // relift would not affect not_lifted in the BNC
    toku_ft_nonl_setup_lift(ft, node);
    return 0;
}

// Relift a leaf node:
// 1. Relift pivots (including bound_l and bound_r), BP_LIFTs are always empty
//    in leaves
// 2. We need to relift messages, because there is no lifting for BLBs (see 1)
static int
toku_ft_leaf_relift(FT ft, FTNODE node, DBT *old_lift, DBT *new_lift)
{
    int r, i;

    node->totalchildkeylens = 0;
    if (node->bound_l.size != 0) {
        r = toku_ft_relift_key(ft, &node->bound_l, old_lift, new_lift);
        assert_zero(r);
        node->totalchildkeylens += node->bound_l.size;
    }
    if (node->bound_r.size != 0) {
        r = toku_ft_relift_key(ft, &node->bound_r, old_lift, new_lift);
        assert_zero(r);
        node->totalchildkeylens += node->bound_r.size;
    }
    for (i = 0; i < node->n_children; i++) {
        if (i != node->n_children - 1) {
            r = toku_ft_relift_key(ft, &node->childkeys[i], old_lift, new_lift);
            assert_zero(r);
            node->totalchildkeylens += node->childkeys[i].size;
        }
        BASEMENTNODE bn = BLB(node, i);
        bn->data_buffer.relift_leafentries(ft, old_lift, new_lift, 0, bn->data_buffer.omt_size());
    }

    return 0;
}

int
toku_ft_node_relift(FT ft, FTNODE node, DBT *old_lift, DBT *new_lift)
{
    if (old_lift->size == 0 && new_lift->size == 0)
        return 0;
    if (old_lift->size == new_lift->size &&
        memcmp(old_lift->data, new_lift->data, old_lift->size) == 0)
    {
        return 0;
    }
    if (node->height == 0)
        return toku_ft_leaf_relift(ft, node, old_lift, new_lift);
    return toku_ft_nonleaf_relift(ft, node, old_lift, new_lift);
}

static inline void bring_node_fully_into_memory(FTNODE node, FT ft)
{
    if (!is_entire_node_in_memory(node)) {
        struct ftnode_fetch_extra bfe;
        fill_bfe_for_full_read(&bfe, ft);
        toku_cachetable_pf_pinned_pair(
            node,
            toku_ftnode_pf_callback,
            &bfe,
            ft->cf,
            node->thisnodename,
            toku_cachetable_hash(ft->cf, node->thisnodename)
            );
    }
}

static bool
toku_ftnode_maybe_break_cow(FT ft, FTNODE *node)
{
    bring_node_fully_into_memory(*node, ft);

    FTNODE new_n;
    new_n = (FTNODE)toku_cachetable_maybe_break_cow(ft, (*node)->ct_pair,
                                                    toku_node_save_ct_pair);
    if (new_n == NULL) {
        return false;
    }
    *node = new_n;
    return true;
}

static void
flush_this_child(FT ft, FTNODE node, FTNODE *child, int childnum)
// Effect: Push everything in the CHILDNUMth buffer of node down into the child.
{
    update_flush_status(*child, 0);
    toku_assert_entire_node_in_memory(node);

    if ((*child)->shadow_next) {
        assert_zero((*child)->height);
        toku_destroy_ftnode_shadows(*child);
    }
    bring_node_fully_into_memory(*child, ft);
    toku_assert_entire_node_in_memory(*child);
    paranoid_invariant(node->height>0);
    paranoid_invariant((*child)->thisnodename.b!=0);
    // VERIFY_NODE does not work off client thread as of now
    // VERIFY_NODE(t, child);
    if (toku_ftnode_maybe_break_cow(ft, child)) {
        BP_BLOCKNUM(node, childnum) = (*child)->thisnodename;
    }
    node->dirty = 1;
    // (*child)->dirty = 1;

    BP_WORKDONE(node, childnum) = 0;  // this buffer is drained, no work has been done by its contents
    NONLEAF_CHILDINFO bnc = BNC(node, childnum);
    set_BNC(node, childnum, toku_create_empty_nl());
    if(bnc->unbound_insert_count) {
        // we just detached a bnc with unbound inserts, so update parent bookkeeping
        paranoid_invariant(node->unbound_insert_count >= bnc->unbound_insert_count);
        node->unbound_insert_count -= bnc->unbound_insert_count;
    }
    // now we have a bnc to flush to the child. pass down the parent's
    // oldest known referenced xid as we flush down to the child.
    DBT pbound_l, pbound_r, not_lifted;
    {
        int r;
        if (childnum == 0) {
            r = toku_ft_lift_key_no_alloc(ft, &pbound_l, &node->bound_l, &BP_LIFT(node, childnum));
        } else {
            r = toku_ft_lift_key_no_alloc(ft, &pbound_l, &node->childkeys[childnum - 1], &BP_LIFT(node, childnum));
        }
        assert_zero(r);
        if (childnum == node->n_children - 1) {
            r = toku_ft_lift_key_no_alloc(ft, &pbound_r, &node->bound_r, &BP_LIFT(node, childnum));
        } else {
            r = toku_ft_lift_key_no_alloc(ft, &pbound_r, &node->childkeys[childnum], &BP_LIFT(node, childnum));
        }
        assert_zero(r);
    }
    toku_copy_dbt(&not_lifted, BP_NOT_LIFTED(node, childnum));
    toku_init_dbt(&BP_NOT_LIFTED(node, childnum));
    toku_bnc_flush_to_child_maybe_slice(ft, bnc, &not_lifted, &pbound_l, &pbound_r, (*child),
                                        node->oldest_referenced_xid_known);
    toku_ft_node_unbound_inserts_validation(node);
    toku_ft_node_unbound_inserts_validation(*child);
    toku_cleanup_dbt(&not_lifted);
    destroy_nonleaf_childinfo(bnc);
}

static inline void
merge_node_update_bound(FTNODE a, FTNODE b)
{
    if (a->bound_r.size) {
        a->totalchildkeylens -= a->bound_r.size;
        toku_cleanup_dbt(&a->bound_r);
    }
    if (b->bound_r.size) {
        a->totalchildkeylens += b->bound_r.size;
        toku_copy_dbt(&a->bound_r, b->bound_r);
        toku_init_dbt(&b->bound_r);
    }
    if (b->bound_l.size) {
        toku_cleanup_dbt(&b->bound_l);
    }
}

// handle_parent: whether we need to delete things in parent
/* NODE is a node with a child.
 * childnum was split into two nodes childa, and childb.  childa is the same as the original child.  childb is a new child.
 * We must slide things around, & move things from the old table to the new tables.
 * Requires: the CHILDNUMth buffer of node is empty.
 * We don't push anything down to children.  We split the node, and things land wherever they land.
 * We must delete the old buffer (but the old child is already deleted.)
 * On return, the new children and node STAY PINNED.
 */
// the data in the childsplitk is alloc'd and is consumed by this call.
static void
handle_split_of_child(FT ft, FTNODE node, int childnum,
                      FTNODE childa, FTNODE childb, DBT *splitk)
{
    assert(node->height > 0);
    assert(childnum >= 0);
    assert(childnum < node->n_children);

    NONLEAF_CHILDINFO old_bnc = BNC(node, childnum);
    assert(toku_bnc_nbytesinbuf(old_bnc) == 0);
    NONLEAF_CHILDINFO new_bnc = toku_create_empty_nl();

    WHEN_NOT_GCOV(
        if (toku_ft_debug_mode) {
            int i;
            printf("%s:%d Child %d splitting on %s\n", __FILE__, __LINE__, childnum, (char*)splitk->data);
            printf("%s:%d oldsplitkeys:", __FILE__, __LINE__);
            for(i=0; i<node->n_children-1; i++) printf(" %s", (char *) node->childkeys[i].data);
            printf("\n");
        }
    )

    // splitk is a key from the child, we need to unlift if necessary
    if (BP_LIFT(node, childnum).size) {
        DBT tmp;
        int r;
        r = toku_ft_unlift_key(ft, &tmp, splitk, &BP_LIFT(node, childnum));
        assert_zero(r);
        toku_destroy_dbt(splitk);
        toku_copy_dbt(splitk, tmp);
    }
    node->dirty = 1;
    assert(childa->dirty);
    assert(childb->dirty);

    // add new pivot and bnc
    XREALLOC_N(node->n_children, node->n_children + 1, node->bp);
    XREALLOC_N(node->n_children - 1, node->n_children, node->childkeys);
    // Slide the children over.
    // suppose n_children is 10 and childnum is 5, meaning node->childnum[5] just got split
    // this moves node->bp[6] through node->bp[9] over to
    // node->bp[7] through node->bp[10]
    for (int cnum = node->n_children; cnum > childnum + 1; cnum--) {
        node->bp[cnum] = node->bp[cnum - 1];
        toku_copy_dbt(&node->childkeys[cnum - 1], node->childkeys[cnum - 2]);
    }

    // set_BNC requires correct n_children
    node->n_children += 1;

    // use the same child
    assert(BP_BLOCKNUM(node, childnum).b == childa->thisnodename.b);
    // fill the empty slot
    memset(&node->bp[childnum + 1], 0, sizeof(node->bp[0]));
    BP_BLOCKNUM(node, childnum + 1) = childb->thisnodename;
    BP_WORKDONE(node, childnum + 1) = 0;
    BP_STATE(node, childnum + 1) = PT_AVAIL;
    set_BNC(node, childnum + 1, new_bnc);

    toku_copy_dbt(&node->childkeys[childnum], *splitk);
    node->totalchildkeylens += splitk->size;

    // adjust the lifting
    int r;
    DBT new_lift, *bound_l, *bound_r;
    // we should've handled not_lifted
    assert_zero(BP_NOT_LIFTED(node, childnum).size);
    // left and right pivots
    bound_l = (childnum == 0) ?
              &node->bound_l : &node->childkeys[childnum - 1];
    // slided
    bound_r = (childnum == node->n_children - 2) ?
              &node->bound_r : &node->childkeys[childnum + 1];
    // the right one: update lift and relift child
    toku_init_dbt(&new_lift);
    r = toku_ft_lift(ft, &new_lift, splitk, bound_r);
    assert_zero(r);
    r = toku_ft_node_relift(ft, childb, &BP_LIFT(node, childnum), &new_lift);
    assert_zero(r);
    toku_copy_dbt(&BP_LIFT(node, childnum + 1), new_lift);
    // the left (original) one: update lift and relift child
    toku_init_dbt(&new_lift);
    r = toku_ft_lift(ft, &new_lift, bound_l, splitk);
    assert_zero(r);
    r = toku_ft_node_relift(ft, childa, &BP_LIFT(node, childnum), &new_lift);
    assert_zero(r);
    toku_cleanup_dbt(&BP_LIFT(node, childnum));
    toku_copy_dbt(&BP_LIFT(node, childnum), new_lift);

    WHEN_NOT_GCOV(
        if (toku_ft_debug_mode) {
            int i;
            printf("%s:%d splitkeys:", __FILE__, __LINE__);
            for(i=0; i<node->n_children-2; i++) printf(" %s", (char*)node->childkeys[i].data);
            printf("\n");
        }
    )

    /* Keep pushing to the children, but not if the children would require a pushdown */
    VERIFY_NODE(t, node);
    VERIFY_NODE(t, childa);
    VERIFY_NODE(t, childb);
}

static uint64_t
ftleaf_disk_size(FTNODE node)
// Effect: get the disk size of a leafentry
{
    paranoid_invariant(node->height == 0);
    toku_assert_entire_node_in_memory(node);
    uint64_t retval = 0;
    for (int i = 0; i < node->n_children; i++) {
        retval += BLB_DATA(node, i)->get_disk_size();
    }
    return retval;
}

static void setkey_func(const DBT *new_key, void *extra)
{
    DBT *splitk = (DBT *)extra;
    toku_clone_dbt(splitk, *new_key);
}

static void
ftleaf_get_pf_split_loc(FT ft, FTNODE node, enum split_mode split_mode,
                        int *num_left_bns, int *num_left_les, DBT *splitk)
{
    DBT minpvt, maxpvt;
    int i, j, rr;
    BN_DATA bd;

    uint64_t sumlesizes = ftleaf_disk_size(node);
    uint32_t size_so_far = 0;
    uint32_t n_leafentries;
    size_t size_this_le;
    uint32_t keylen;
    void *key;

    switch (split_mode) {
    case SPLIT_LEFT_HEAVY:
    {
        for (i = node->n_children - 1; i >= 0; i--) {
            bd = BLB_DATA(node, i);
            n_leafentries = bd->omt_size();
            for (j = n_leafentries - 1; j >= 0; j--) {
                rr = bd->fetch_klpair_disksize(j, &size_this_le);
                invariant_zero(rr);
                if (size_so_far == 0) {
                    rr = bd->fetch_le_key_and_len(j, &keylen, &key);
                    invariant_zero(rr);
                    toku_fill_dbt(&maxpvt, key, keylen);
                    *num_left_bns = i + 1;
                    *num_left_les = j + 1;
                }
                size_so_far += size_this_le;
                if (size_so_far >= (sumlesizes >> 2)) {
                    rr = bd->fetch_le_key_and_len(j, &keylen, &key);
                    invariant_zero(rr);
                    toku_fill_dbt(&minpvt, key, keylen);

                    goto exit;
                }
            }
        }
        break;
    }
    case SPLIT_RIGHT_HEAVY:
    {
        for (i = 0; i < node->n_children; i++) {
            bd = BLB_DATA(node, i);
            n_leafentries = bd->omt_size();
            for (j = 0; j < n_leafentries; j++) {
                rr = bd->fetch_klpair_disksize(j, &size_this_le);
                invariant_zero(rr);
                if (size_so_far == 0) {
                    rr = bd->fetch_le_key_and_len(j, &keylen, &key);
                    invariant_zero(rr);
                    toku_fill_dbt(&minpvt, key, keylen);
                    *num_left_bns = i + 1;
                    *num_left_les = j + 1;
                }
                size_so_far += size_this_le;
                if (size_so_far >= (sumlesizes >> 2)) {
                    rr = bd->fetch_le_key_and_len(j, &keylen, &key);
                    invariant_zero(rr);
                    toku_fill_dbt(&maxpvt, key, keylen);

                    goto exit;
                }
            }
        }
        break;
    }
    case SPLIT_EVENLY:
    {
        *num_left_bns = -1;
        toku_init_dbt(&minpvt);
        for (i = 0; i < node->n_children; i++) {
            bd = BLB_DATA(node, i);
            n_leafentries = bd->omt_size();
            for (j = 0; j < n_leafentries; j++) {
                rr = bd->fetch_klpair_disksize(j, &size_this_le);
                invariant_zero(rr);
                size_so_far += size_this_le;
                if (size_so_far >= (sumlesizes >> 2) && minpvt.data == NULL) {
                    rr = bd->fetch_le_key_and_len(j, &keylen, &key);
                    invariant_zero(rr);
                    toku_fill_dbt(&minpvt, key, keylen);
                }
                if (size_so_far >= (sumlesizes >> 1) && *num_left_bns < 0) {
                    *num_left_bns = i + 1;
                    *num_left_les = j + 1;
                }
                if (size_so_far >= 3 * (sumlesizes >> 2)) {
                    rr = bd->fetch_le_key_and_len(j, &keylen, &key);
                    invariant_zero(rr);
                    toku_fill_dbt(&maxpvt, key, keylen);

                    goto exit;
                }
            }
        }
        break;
    }
    default:
        break;
    }
    abort();

exit:
    FAKE_DB(db, &ft->cmp_descriptor);
    ft->key_ops.keypfsplit(&db, &minpvt, &maxpvt, setkey_func, (void *)splitk);
    // if pf split chooses to intervene, update bns and les
    if (splitk->data != NULL) {
        int lo = 0;
        int hi = node->n_children - 1;
        int mid;
        while (lo < hi) {
            mid = (lo + hi) >> 1;
            rr = ft->key_ops.keycmp(&db, splitk, &node->childkeys[mid]);
            if (rr <= 0) {
                // splitk is smaller than or equal to pivotk
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        *num_left_bns = lo + 1;
        bd = BLB_DATA(node, lo);
        lo = 0;
        hi = bd->omt_size();
        while (lo < hi) {
            mid = (lo + hi) >> 1;
            rr = bd->fetch_le_key_and_len(mid, &keylen, &key);
            invariant_zero(rr);
            toku_fill_dbt(&minpvt, key, keylen);
            rr = ft->key_ops.keycmp(&db, &minpvt, splitk);
            if (rr <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (hi == 0 && *num_left_bns > 1) {
            *num_left_bns -= 1;
            *num_left_les = BLB_DATA(node, *num_left_bns - 1)->omt_size();
        } else {
            *num_left_les = hi;
        }
    }

    return;
}

static void
ftleaf_get_split_loc(
    FTNODE node,
    enum split_mode split_mode,
    int *num_left_bns,   // which basement within leaf
    int *num_left_les    // which key within basement
    )
// Effect: Find the location within a leaf node where we want to perform a split
// num_left_bns is how many basement nodes (which OMT) should be split to the left.
// num_left_les is how many leafentries in OMT of the last bn should be on the left side of the split.
{
    switch (split_mode) {
    case SPLIT_LEFT_HEAVY:
    {
        *num_left_bns = node->n_children;
        *num_left_les = BLB_DATA(node, *num_left_bns - 1)->omt_size();
        if (*num_left_les == 0) {
            *num_left_bns = node->n_children - 1;
            *num_left_les = BLB_DATA(node, *num_left_bns - 1)->omt_size();
        }
        goto exit;
    }
    case SPLIT_RIGHT_HEAVY:
    {
        *num_left_bns = 1;
        *num_left_les = BLB_DATA(node, 0)->omt_size() ? 1 : 0;

        goto exit;
    }
    case SPLIT_EVENLY:
    {
        paranoid_invariant(node->height == 0);
        // TODO: (Zardosht) see if we can/should make this faster, we iterate over the rows twice
        uint64_t sumlesizes = ftleaf_disk_size(node);
        uint32_t size_so_far = 0;
        for (int i = 0; i < node->n_children; i++) {
            BN_DATA bd = BLB_DATA(node, i);
            uint32_t n_leafentries = bd->omt_size();
            for (uint32_t j=0; j < n_leafentries; j++) {
                size_t size_this_le;
                int rr = bd->fetch_klpair_disksize(j, &size_this_le);
                invariant_zero(rr);
                size_so_far += size_this_le;
                if (size_so_far >= sumlesizes/2) {
                    *num_left_bns = i + 1;
                    *num_left_les = j + 1;
                    if (*num_left_bns == node->n_children &&
                        (unsigned int) *num_left_les == n_leafentries) {
                        // need to correct for when we're splitting after the
                        // last element, that makes no sense
                        if (*num_left_les > 1) {
                            (*num_left_les)--;
                        } else if (*num_left_bns > 1) {
                            (*num_left_bns)--;
                            *num_left_les = BLB_DATA(node, *num_left_bns - 1)->omt_size();
                        } else {
                            // we are trying to split a leaf with only one
                            // leafentry in it
                            abort();
                        }
                    }

                    // SOSP: Bill force splits on boundaries when unbound_inserts exist at the split
		    //SOSP: Jun -undo the force on-the-boundary
	            #if 0
                    if(BLB(node, *num_left_bns - 1)->unbound_insert_count) {
                        if ((*num_left_les != 0) && (*num_left_les != (int) BLB_DATA(node, *num_left_bns - 1)->omt_size())) {
                            if (*num_left_bns < node->n_children) {
                                *num_left_les = (int) BLB_DATA(node, *num_left_bns - 1)->omt_size();
                            } else {
                                /* num_left_bns == node->n_children */
                                //*num_left_bns -= 1;
                                //*num_left_les = BLB_DATA(node, *num_left_bns)->omt_size();
                                *num_left_les = 0;
                            }
                        }
                    }
		   #endif
                    goto exit;
                }
            }
        }
    }
    }
    abort();
exit:
    return;
}

static int
ft_compare_pivot(DESCRIPTOR desc, ft_compare_func cmp, const DBT *key, const DBT *pivot)
{
    int r;
    FAKE_DB(db, desc);
    r = cmp(&db, key, pivot);
    return r;
}

static inline void
split_node_update_bound(FTNODE a, FTNODE b, const void *key, uint32_t keylen)
{
    a->totalchildkeylens = a->totalchildkeylens + keylen - a->bound_r.size;
    b->totalchildkeylens = b->totalchildkeylens + keylen + a->bound_r.size;

    toku_copy_dbt(&b->bound_r, a->bound_r);
    toku_memdup_dbt(&b->bound_l, key, keylen);
    toku_memdup_dbt(&a->bound_r, key, keylen);
}

// TODO: (Zardosht) possibly get rid of this function and use toku_omt_split_at in
// ftleaf_split
static void
move_ubi_entries(FTNODE dst_node, FTNODE src_node, int dst_idx, int src_idx,
                     DBT *pivot, FT ft)
{
    BASEMENTNODE dst_bn = BLB(dst_node, dst_idx);
    BASEMENTNODE src_bn = BLB(src_node, src_idx);
    toku_list *src_unbound_inserts = &src_bn->unbound_inserts;
    toku_list *dst_unbound_inserts = &dst_bn->unbound_inserts;
    assert(toku_list_empty(dst_unbound_inserts));
    toku_list *list = toku_list_head(src_unbound_inserts);
    while(list != src_unbound_inserts) {
        struct ubi_entry *entry;
        entry = toku_list_struct(list, struct ubi_entry, node_list);
        if(ft_compare_pivot(&ft->cmp_descriptor, ft->key_ops.keycmp, &entry->key, pivot) > 0) {
            toku_list *to_be_removed = list;
            list = list->next;
            toku_list_remove(to_be_removed);
            toku_list_push(dst_unbound_inserts, to_be_removed);
            src_node->unbound_insert_count--;
            src_bn->unbound_insert_count--;
            dst_node->unbound_insert_count++;
            dst_bn->unbound_insert_count++;
        } else {
            list = list->next;
        }
    }
}

static void
move_leafentries(
    BASEMENTNODE dest_bn,
    BASEMENTNODE src_bn,
    uint32_t lbi, //lower bound inclusive
    uint32_t ube //upper bound exclusive
    )
//Effect: move leafentries in the range [lbi, upe) from src_omt to newly created dest_omt
{
    src_bn->data_buffer.move_leafentries_to(&dest_bn->data_buffer, lbi, ube);
}

// Effect: Split a leaf node.
// Argument "node" is node to be split.
// Upon return:
//   nodea and nodeb point to new nodes that result from split of "node"
//   nodea is the left node that results from the split
//   splitk is the right-most key of nodea
void
ft_split_leaf(FT ft, FTNODE node, FTNODE *nodea, FTNODE *nodeb, DBT *splitk,
              bool create_new_node, enum split_mode split_mode,
              uint32_t num_dependent_nodes, FTNODE* dependent_nodes)
{
    STATUS_VALUE(FT_FLUSHER_SPLIT_LEAF)++;

    assert_zero(node->height);
    if (node->n_children) {
        // First move all the accumulated stat64info deltas into the first basement.
        // After the split, either both nodes or neither node will be included in the next checkpoint.
        // The accumulated stats in the dictionary will be correct in either case.
        // By moving all the deltas into one (arbitrary) basement, we avoid the need to maintain
        // correct information for a basement that is divided between two leafnodes (i.e. when split is
        // not on a basement boundary).
        STAT64INFO_S delta_for_leafnode = toku_get_and_clear_basement_stats(node);
        BASEMENTNODE bn = BLB(node, 0);
        bn->stat64_delta = delta_for_leafnode;
    }


    FTNODE B = NULL;
    uint32_t fullhash;
    BLOCKNUM name;

    if (create_new_node) {
        // put value in cachetable and do checkpointing
        // of dependent nodes
        //
        // We do this here, before evaluating the last_bn_on_left
        // and last_le_on_left_within_bn because this operation
        // may write to disk the dependent nodes.
        // While doing so, we may rebalance the leaf node
        // we are splitting, thereby invalidating the
        // values of last_bn_on_left and last_le_on_left_within_bn.
        // So, we must call this before evaluating
        // those two values
        cachetable_put_empty_node_with_dep_nodes(
            ft, num_dependent_nodes, dependent_nodes, &name, &fullhash, &B);
        // GCC 4.8 seems to get confused and think B is maybe uninitialized at link time.
        // TODO(leif): figure out why it thinks this and actually fix it.
        assert(B != NULL);
    }

    // variables that say where we will do the split.
    // After the split, there will be num_left_bns basement nodes in the left node,
    // and the last basement node in the left node will have num_left_les leafentries.
    int num_left_bns;
    int num_left_les;
    assert(splitk != NULL);
    toku_init_dbt(splitk);
    if (ft->key_ops.keypfsplit != NULL) {
        ftleaf_get_pf_split_loc(ft, node, split_mode, &num_left_bns, &num_left_les, splitk);
    } else {
        ftleaf_get_split_loc(node, split_mode, &num_left_bns, &num_left_les);
    }

    {
        // calc how many BNs are going to be moved to B
        const bool split_on_boundary = (num_left_les == 0) ||
                                       (num_left_les == (int)BLB_DATA(node, num_left_bns - 1)->omt_size());
        int num_children_in_node = num_left_bns;
        int num_children_in_b = node->n_children - num_left_bns + (!split_on_boundary ? 1 : 0);
        if (num_children_in_b == 0) {
            assert(split_mode != SPLIT_RIGHT_HEAVY);
            num_children_in_b = 1;
        }
        assert(num_children_in_node > 0);
        // initialize B
        if (create_new_node) {
            toku_initialize_empty_ftnode(B, name, 0, num_children_in_b,
                                         ft->h->layout_version, ft->h->flags);
            B->fullhash = fullhash;
        } else {
            B = *nodeb;
            if (num_children_in_b > 1) {
                REALLOC_N(num_children_in_b - 1, B->n_children - 1, B->childkeys);
            } else {
                B->childkeys = NULL;
            }
            REALLOC_N(num_children_in_b, B->n_children, B->bp);
            B->n_children = num_children_in_b;
            for (int i = 0; i < num_children_in_b; i++) {
                BP_BLOCKNUM(B, i).b = 0;
                BP_STATE(B, i) = PT_AVAIL;
                BP_WORKDONE(B, i) = 0;
                BP_PREFETCH_FLAG(B, i)=false;
                set_BLB(B, i, toku_create_empty_bn());
            }
        }
        // start moving
        int curr_src_bn_idx = num_left_bns - 1;
        int curr_dst_bn_idx = 0;
        // handle boundary
        if (!split_on_boundary) {
            BP_STATE(B, curr_dst_bn_idx) = PT_AVAIL;
            destroy_basement_node(BLB(B, curr_dst_bn_idx));
            set_BNULL(B, curr_dst_bn_idx);
            set_BLB(B, curr_dst_bn_idx, toku_create_empty_bn_no_buffer());
            move_leafentries(BLB(B, curr_dst_bn_idx),
                             BLB(node, curr_src_bn_idx),
                             num_left_les, // first to move
                             BLB_DATA(node, curr_src_bn_idx)->omt_size());
            if (BLB(node, curr_src_bn_idx)->unbound_insert_count > 0) {
                uint32_t keylen;
                void *key;
                BN_DATA bd = BLB_DATA(node, num_left_bns - 1);
                int rr = bd->fetch_le_key_and_len(bd->omt_size() - 1, &keylen, &key);
                invariant_zero(rr);
                DBT future_pivot;
                toku_fill_dbt(&future_pivot, key, keylen);
                move_ubi_entries(B, node, curr_dst_bn_idx, curr_src_bn_idx,
                                 &future_pivot, ft);
            }
            BLB_MAX_MSN_APPLIED(B, curr_dst_bn_idx) = BLB_MAX_MSN_APPLIED(node, curr_src_bn_idx);
            curr_dst_bn_idx++;
        }
        curr_src_bn_idx++;
        // move BNs
        for (; curr_src_bn_idx < node->n_children; curr_src_bn_idx++, curr_dst_bn_idx++) {
            destroy_basement_node(BLB(B, curr_dst_bn_idx));
            set_BNULL(B, curr_dst_bn_idx);
            B->bp[curr_dst_bn_idx] = node->bp[curr_src_bn_idx];
            uint32_t ubi_count = BLB(node, curr_src_bn_idx)->unbound_insert_count;
            if (ubi_count) {
                paranoid_invariant(node->unbound_insert_count >= ubi_count);
                node->unbound_insert_count -= ubi_count;
                B->unbound_insert_count += ubi_count;
            }
        }
        if (curr_dst_bn_idx < B->n_children) {
            // we copied everything, but we may still leave some bp UNAVAIL
            BP_STATE(B, curr_dst_bn_idx) = PT_AVAIL;
        }
        // copy pvts
        int base_idx = num_left_bns - (split_on_boundary ? 0 : 1);
        for (int i = 0; i < num_children_in_b - 1; i++) {
            toku_copy_dbt(&B->childkeys[i], node->childkeys[i + base_idx]);
            B->totalchildkeylens += node->childkeys[i + base_idx].size;
            node->totalchildkeylens -= node->childkeys[i + base_idx].size;
            toku_init_dbt(&node->childkeys[i + base_idx]);
        }
        // get splitk (the pvt for the parent)
        if (split_on_boundary && num_left_bns < node->n_children) {
            if (splitk->data == NULL) {
                toku_copy_dbt(splitk, node->childkeys[num_left_bns - 1]);
                node->totalchildkeylens -= node->childkeys[num_left_bns - 1].size;
                toku_init_dbt(&node->childkeys[num_left_bns - 1]);
            } else {
                node->totalchildkeylens -= node->childkeys[num_left_bns - 1].size;
                toku_destroy_dbt(&node->childkeys[num_left_bns - 1]);
            }
        } else {
            if (splitk->data == NULL) {
                BN_DATA bd = BLB_DATA(node, num_left_bns - 1);
                uint32_t keylen;
                void *key;
                int rr = bd->fetch_le_key_and_len(bd->omt_size() - 1, &keylen, &key);
                invariant_zero(rr);
                toku_memdup_dbt(splitk, key, keylen);
            }
        }
        split_node_update_bound(node, B, splitk->data, splitk->size);
        // cleanup node
        REALLOC_N(num_children_in_node, node->n_children, node->bp);
        if (num_children_in_node > 1) {
            REALLOC_N(num_children_in_node - 1, node->n_children - 1, node->childkeys);
        } else {
            if (node->childkeys) {
                toku_free(node->childkeys);
                node->childkeys = NULL;
            }
        }
        node->n_children = num_children_in_node;
    }

    *nodea = node;
    *nodeb = B;

    toku_ft_node_unbound_inserts_validation(*nodea);
    toku_ft_node_unbound_inserts_validation(*nodeb);
}

static void
ft_split_nonl(FT ft, FTNODE node, FTNODE *nodea, FTNODE *nodeb, DBT *splitk,
              uint32_t num_dependent_nodes, FTNODE *dependent_nodes)
{
    STATUS_VALUE(FT_FLUSHER_SPLIT_NONLEAF)++;

    int old_n_children = node->n_children;
    int n_children_in_a;
    if (ft->key_ops.keypfsplit != NULL) {
        FAKE_DB(db, &ft->cmp_descriptor);
        n_children_in_a = old_n_children >> 2;
        int least_shared =
            ft->key_ops.keypfsplit(&db,
                                   &node->childkeys[n_children_in_a - 1],
                                   &node->childkeys[n_children_in_a],
                                   NULL, NULL);
        int dist_center = (old_n_children >> 1) - n_children_in_a;
        for (int i = n_children_in_a + 1; i < old_n_children - (old_n_children >> 2); i++) {
            int tmp =
                ft->key_ops.keypfsplit(&db,
                                       &node->childkeys[i - 1],
                                       &node->childkeys[i],
                                       NULL, NULL);
            if (tmp <= least_shared) {
                int tmp_dist_center;
                if (i > (old_n_children >> 1))
                    tmp_dist_center = i - (old_n_children >> 1);
                else
                    tmp_dist_center = (old_n_children >> 1) - i;
                if (tmp < least_shared || tmp_dist_center < dist_center) {
                    least_shared = tmp;
                    dist_center = tmp_dist_center;
                    n_children_in_a = i;
                }
            }
        }
    } else {
        n_children_in_a = old_n_children >> 1;
    }
    int n_children_in_b = old_n_children - n_children_in_a;
    assert(n_children_in_a > 0);
    assert(n_children_in_b > 0);

    FTNODE B;
    assert(node->height > 0);
    assert(node->n_children >= 2);
    create_new_ftnode_with_dep_nodes(ft, &B, node->height, n_children_in_b,
                                     num_dependent_nodes, dependent_nodes);
    for (int i = n_children_in_a; i < old_n_children; i++) {
        int targetchild = i - n_children_in_a;
        // move bp
        destroy_nonleaf_childinfo(BNC(B, targetchild));
        B->bp[targetchild] = node->bp[i];
        // update ubi
        uint32_t ubi_count = BNC(node, i)->unbound_insert_count;
        if (ubi_count) {
            assert(node->unbound_insert_count >= ubi_count);
            node->unbound_insert_count -= ubi_count;
            B->unbound_insert_count += ubi_count;
        }
        memset(&node->bp[i], 0, sizeof(node->bp[0]));
        // move pivot
        if (i > n_children_in_a) {
            toku_copy_dbt(&B->childkeys[targetchild - 1], node->childkeys[i - 1]);
            B->totalchildkeylens += node->childkeys[i - 1].size;
            node->totalchildkeylens -= node->childkeys[i - 1].size;
            toku_init_dbt(&node->childkeys[i - 1]);
        }
    }

    int old_n_children_in_a = node->n_children;
    node->n_children = n_children_in_a;

    toku_copy_dbt(splitk, node->childkeys[n_children_in_a - 1]);
    node->totalchildkeylens -= node->childkeys[n_children_in_a - 1].size;
    toku_init_dbt(&node->childkeys[n_children_in_a - 1]);
    split_node_update_bound(node, B, splitk->data, splitk->size);

    REALLOC_N(n_children_in_a, old_n_children_in_a, node->bp);
    if (n_children_in_a > 1) {
        REALLOC_N(n_children_in_a - 1, old_n_children_in_a - 1, node->childkeys);
    } else {
        if (node->childkeys) {
            toku_free(node->childkeys);
            node->childkeys = NULL;
        }
    }
    *nodea = node;
    *nodeb = B;

    toku_ft_node_unbound_inserts_validation(*nodea);
    toku_ft_node_unbound_inserts_validation(*nodeb);
}

//
// responsibility of ft_split_child is to take locked FTNODEs node and child
// and do the following:
//  - split child,
//  - fix node,
//  - release lock on node
//  - possibly flush either new children created from split, otherwise unlock children
//
static void
ft_split_child(FT ft, FTNODE node, int childnum, FTNODE child,
               enum split_mode split_mode, struct flusher_advice *fa)
{
    assert(node->height > 0);
    // require that the buffer for this child is empty
    assert(toku_bnc_nbytesinbuf(BNC(node, childnum))==0);
    FTNODE childa, childb;
    DBT splitk;

    toku_assert_entire_node_in_memory(node);
    toku_assert_entire_node_in_memory(child);

    // for test
    call_flusher_thread_callback(flt_flush_before_split);

    // use this break cow and also (maybe slice)
    flush_this_child(ft, node, &child, childnum);

    MSN max_msn = child->max_msn_applied_to_node_on_disk;
    FTNODE dep_nodes[2];
    dep_nodes[0] = node;
    dep_nodes[1] = child;
    if (child->height == 0) {
        ft_split_leaf(ft, child, &childa, &childb, &splitk, true, split_mode,
                      2, dep_nodes);
    } else {
        ft_split_nonl(ft, child, &childa, &childb, &splitk, 2, dep_nodes);
    }
    childa->dirty = 1;
    childb->dirty = 1;
    childa->max_msn_applied_to_node_on_disk = max_msn;
    childb->max_msn_applied_to_node_on_disk = max_msn;
    childb->oldest_referenced_xid_known = childa->oldest_referenced_xid_known;
    // after split, update parent, relift children
    handle_split_of_child(ft, node, childnum, childa, childb, &splitk);

    // paranoid
    toku_assert_entire_node_in_memory(node);
    toku_assert_entire_node_in_memory(childa);
    toku_assert_entire_node_in_memory(childb);

    // for test
    call_flusher_thread_callback(flt_flush_during_split);

    // at this point, the split is complete
    // now we need to unlock node,
    // and possibly continue flushing one of the children
    int picked_child = fa->pick_child_after_split(
                            ft, node, childnum, childnum + 1, fa->extra);
    toku_unpin_ftnode_off_client_thread(ft, node);
    if (picked_child == childnum ||
            (picked_child < 0 && childa->height > 0 &&
             fa->should_recursively_flush(childa, fa->extra))) {
        toku_unpin_ftnode_off_client_thread(ft, childb);
        toku_ft_flush_some_child(ft, childa, fa);
    } else if (picked_child == childnum + 1 ||
            (picked_child < 0 && childb->height > 0 &&
             fa->should_recursively_flush(childb, fa->extra))) {
        toku_unpin_ftnode_off_client_thread(ft, childa);
        toku_ft_flush_some_child(ft, childb, fa);
    } else {
        toku_unpin_ftnode_off_client_thread(ft, childa);
        toku_unpin_ftnode_off_client_thread(ft, childb);
    }
}

//SOSP TODO: BILL
//update unbound_insert_count's, lists
// handle_parent: whether we need to delete things in parent
// merge two leaf children, if handle_parent adjust parent bps and pivts
static void
merge_leaf_nodes(FT ft, FTNODE parent, int childnuma, FTNODE a, FTNODE b,
                 bool handle_parent)
{
    STATUS_VALUE(FT_FLUSHER_MERGE_LEAF)++;
    paranoid_invariant(a->height == 0);
    paranoid_invariant(b->height == 0);
    paranoid_invariant(a->n_children > 0);
    paranoid_invariant(b->n_children > 0);

    {
        // first, adjust lifting in two children
        int r;

        DBT new_lift, *bound_l, *bound_r;

        // we should've handled not_lifted (flushing before merging)
        assert_zero(BP_NOT_LIFTED(parent, childnuma).size);
        assert_zero(BP_NOT_LIFTED(parent, childnuma + 1).size);

        bound_l = (childnuma == 0) ?
                  &parent->bound_l : &parent->childkeys[childnuma - 1];
        bound_r = (childnuma == parent->n_children - 2) ?
                  &parent->bound_r : &parent->childkeys[childnuma + 1];
        toku_init_dbt(&new_lift);
        r = toku_ft_lift(ft, &new_lift, bound_l, bound_r);
        assert_zero(r);
        r = toku_ft_node_relift(ft, a, &BP_LIFT(parent, childnuma), &new_lift);
        assert_zero(r);
        r = toku_ft_node_relift(ft, b, &BP_LIFT(parent, childnuma + 1), &new_lift);
        assert_zero(r);
        toku_cleanup_dbt(&BP_LIFT(parent, childnuma));
        toku_cleanup_dbt(&BP_LIFT(parent, childnuma + 1));
        toku_copy_dbt(&BP_LIFT(parent, childnuma), new_lift);
    }

    // move each basement node from b to a
    // move the pivots, adding one of what used to be max(a)
    // move the estimates
    int n_children = a->n_children + b->n_children;

    //realloc pivots and basement nodes in a
    assert(n_children > 1);
    REALLOC_N(n_children, a->n_children, a->bp);
    REALLOC_N(n_children - 1, a->n_children - 1, a->childkeys);

    // bps
    memcpy(a->bp + a->n_children, b->bp, b->n_children * sizeof(b->bp[0]));
    // fill in pivot from bound_r
    toku_copy_dbt(&a->childkeys[a->n_children - 1], a->bound_r);
    toku_init_dbt(&a->bound_r);
    memcpy(a->childkeys + a->n_children, b->childkeys, (b->n_children - 1) * sizeof(b->childkeys[0]));
    a->totalchildkeylens += (b->totalchildkeylens - b->bound_l.size - b->bound_r.size);
    a->n_children = n_children;
    // uopdate bounds
    merge_node_update_bound(a, b);

    for (int i = 0; i < b->n_children ; ++i) {
        // Set both the state to invalid, and the tag to null
        BP_STATE(b,i) = PT_INVALID;
        set_BNULL(b, i);
    }

    a->unbound_insert_count += b->unbound_insert_count;
    b->unbound_insert_count = 0;
    // now that all the data has been moved from b to a
    if (b->n_children > 0) {
        if (b->n_children > 1) {
            toku_free(b->childkeys);
            b->childkeys = NULL;
        }
        toku_free(b->bp);
        b->bp = NULL;
        b->n_children = 0;
    }
    b->totalchildkeylens = 0;

    // remove the pivot from parent
    parent->totalchildkeylens -= parent->childkeys[childnuma].size;
    toku_cleanup_dbt(&parent->childkeys[childnuma]);

    if (handle_parent) {
        destroy_nonleaf_childinfo(BNC(parent, childnuma + 1));
        set_BNULL(parent, childnuma + 1);
        parent->n_children--;
        memmove(&parent->bp[childnuma + 1], &parent->bp[childnuma + 2],
                (parent->n_children - childnuma - 1) * sizeof(parent->bp[0]));
        REALLOC_N(parent->n_children, parent->n_children + 1, parent->bp);
        memmove(&parent->childkeys[childnuma], &parent->childkeys[childnuma + 1],
                (parent->n_children - childnuma - 1) * sizeof(parent->childkeys[0]));
        REALLOC_N(parent->n_children - 1, parent->n_children, parent->childkeys);
    }

    parent->dirty = 1;
    a->dirty = 1;
    b->dirty = 1;
}

// Effect:
//  If b is bigger then move stuff from b to a until b is the smaller.
//  If a is bigger then move stuff from a to b until a is the smaller.
static void
balance_leaf_nodes(FT ft, FTNODE parent, int childnuma, FTNODE a, FTNODE b)
{
    STATUS_VALUE(FT_FLUSHER_BALANCE_LEAF)++;

    DBT splitk;
    int r;
    // first merge all the data into a
    merge_leaf_nodes(ft, parent, childnuma, a, b, false);
    // now split them
    // because we are not creating a new node, we can pass in no dependent nodes
    ft_split_leaf(ft, a, &a, &b, &splitk, false, SPLIT_EVENLY, 0, NULL);
    // we merge the leaf and then split, need to adjust lift for the split
    // much the same as handle_split_of_child

    // first, add the new pivot to parent
    if (BP_LIFT(parent, childnuma).size) {
        DBT tmp;
        toku_init_dbt(&tmp);
        r = toku_ft_unlift_key(ft, &tmp, &splitk, &BP_LIFT(parent, childnuma));
        assert_zero(r);
        toku_cleanup_dbt(&splitk);
        toku_copy_dbt(&splitk, tmp);
    }
    toku_copy_dbt(&parent->childkeys[childnuma], splitk);
    parent->totalchildkeylens += parent->childkeys[childnuma].size;
    // relift children
    // we just merged two children, so they are lifted by LIFT(childnuma) now
    DBT new_lift, *bound_l, *bound_r;
    bound_l = (childnuma == 0) ?
              &parent->bound_l : &parent->childkeys[childnuma - 1];
    bound_r = (childnuma == parent->n_children - 2) ?
              &parent->bound_r : &parent->childkeys[childnuma + 1];
    // the right one
    toku_init_dbt(&new_lift);
    r = toku_ft_lift(ft, &new_lift, &splitk, bound_r);
    assert_zero(r);
    r = toku_ft_node_relift(ft, b, &BP_LIFT(parent, childnuma), &new_lift);
    assert_zero(r);
    assert_zero(BP_LIFT(parent, childnuma + 1).size);
    toku_copy_dbt(&BP_LIFT(parent, childnuma + 1), new_lift);
    // the left one
    toku_init_dbt(&new_lift);
    r = toku_ft_lift(ft, &new_lift, bound_l, &splitk);
    assert_zero(r);
    r = toku_ft_node_relift(ft, a, &BP_LIFT(parent, childnuma), &new_lift);
    assert_zero(r);
    toku_cleanup_dbt(&BP_LIFT(parent, childnuma));
    toku_copy_dbt(&BP_LIFT(parent, childnuma), new_lift);
}

// Effect: Either merge a and b into one one node (merge them into a) and set *did_merge = true.
//	   (We do this if the resulting node is not fissible)
//	   or distribute the leafentries evenly between a and b, and set *did_rebalance = true.
//	   (If a and be are already evenly distributed, we may do nothing.)
static void
ft_merge_pinned_leaf(FT ft, FTNODE parent, int childnuma, FTNODE a, FTNODE b,
                     bool *did_merge, bool *did_rebalance)
{
    unsigned int sizea = toku_serialize_ftnode_size(a);
    unsigned int sizeb = toku_serialize_ftnode_size(b);
    uint32_t num_leafentries = get_leaf_num_entries(a) + get_leaf_num_entries(b);
    uint32_t nodesize = ft->h->nodesize;
    if (num_leafentries > 1 && (sizea + sizeb) * 4 > (nodesize * 3)) {
        // the combined size is more than 3/4 of a node, so don't merge them.
        *did_merge = false;
        if (sizea * 4 > nodesize && sizeb * 4 > nodesize) {
            // no need to do anything if both are more than 1/4 of a node.
            *did_rebalance = false;
            return;
        }
        // one is less than 1/4 of a node, and together they are more than 3/4 of a node.
        // We don't need the parent_splitk any more. If we need a splitk (if we don't merge) we'll malloc a new one.
        *did_rebalance = true;
        balance_leaf_nodes(ft, parent, childnuma, a, b);
    } else {
        // we are merging them.
        *did_merge = true;
        *did_rebalance = false;
        merge_leaf_nodes(ft, parent, childnuma, a, b, true);
    }

    toku_ft_node_unbound_inserts_validation(a);
    toku_ft_node_unbound_inserts_validation(b);
}

// this function always merges two children (parent is a nonleaf node)
static void
ft_merge_pinned_nonl(FT ft, FTNODE parent, int childnuma, FTNODE a, FTNODE b,
                     bool *did_merge, bool *did_rebalance)
{
    int old_n_children = a->n_children;
    int new_n_children = old_n_children + b->n_children;

    // remove one pivot from parent
    parent->totalchildkeylens -= parent->childkeys[childnuma].size;
    {
        // we are going to merge a and b, the lifted part might change
        int r;
        DBT new_lift, *bound_l, *bound_r;

        bound_l = (childnuma == 0) ?
                  &parent->bound_l : &parent->childkeys[childnuma - 1];
        bound_r = (childnuma == parent->n_children - 2) ?
                  &parent->bound_r : &parent->childkeys[childnuma + 1];
        toku_init_dbt(&new_lift);
        r = toku_ft_lift(ft, &new_lift, bound_l, bound_r);
        assert_zero(r);
        // now new_lift has the future lift for the merged node
        r = toku_ft_node_relift(ft, a, &BP_LIFT(parent, childnuma), &new_lift);
        assert_zero(r);
        r = toku_ft_node_relift(ft, b, &BP_LIFT(parent, childnuma + 1), &new_lift);
        assert_zero(r);
        // cleanup old lift
        toku_cleanup_dbt(&BP_LIFT(parent, childnuma));
        toku_cleanup_dbt(&BP_LIFT(parent, childnuma + 1));
        toku_copy_dbt(&BP_LIFT(parent, childnuma), new_lift);
        // this pivot is going to be inserted into the child later in this
        //   func, lift it now so they are ready to be inserted
        if (new_lift.size) {
            DBT tmp;
            toku_init_dbt(&tmp);
            r = toku_ft_lift_key(ft, &tmp, &parent->childkeys[childnuma], &new_lift);
            assert_zero(r);
            toku_destroy_dbt(&parent->childkeys[childnuma]);
            toku_copy_dbt(&parent->childkeys[childnuma], tmp);
        }
    }

    assert(new_n_children > 1);
    // move bps
    XREALLOC_N(a->n_children, new_n_children, a->bp);
    memcpy(a->bp + old_n_children, b->bp, b->n_children * sizeof(b->bp[0]));
    // move pivots
    XREALLOC_N(a->n_children - 1, new_n_children - 1, a->childkeys);
    toku_copy_dbt(&a->childkeys[old_n_children - 1], parent->childkeys[childnuma]);
    a->totalchildkeylens += parent->childkeys[childnuma].size;
    toku_init_dbt(&parent->childkeys[childnuma]);
    memcpy(a->childkeys + old_n_children, b->childkeys, (b->n_children - 1) * sizeof(b->childkeys[0]));
    a->totalchildkeylens += (b->totalchildkeylens - b->bound_l.size - b->bound_r.size);
    // update bounds
    merge_node_update_bound(a, b);

    // b has been merge to a. Everything belonged to b
    // belongs to a now. Don't forget to cleanup/unreference
    // the prefixes (BP_LIFT) of each partition.
    // Otherwise BP_LIFT will be freed when b is
    // destroyed even though it belongs to a now.
    for (int i = 0; i < b->n_children ; ++i) {
        BP_STATE(b,i) = PT_INVALID;
        set_BNULL(b, i);
        toku_init_dbt(&BP_LIFT(b, i));
    }
    a->n_children = new_n_children;
    // ubi
    a->unbound_insert_count += b->unbound_insert_count;
    b->unbound_insert_count = 0;
    // now a is ready, cleanup b
    if (b->n_children > 0) {
        if (b->n_children > 1) {
            toku_free(b->childkeys);
            b->childkeys = NULL;
        }
        toku_free(b->bp);
        b->bp = NULL;
    }
    b->totalchildkeylens = 0;
    b->n_children = 0;
    // change parent
    destroy_nonleaf_childinfo(BNC(parent, childnuma + 1));
    set_BNULL(parent, childnuma + 1);
    parent->n_children--;
    memmove(&parent->bp[childnuma + 1], &parent->bp[childnuma + 2],
            (parent->n_children - childnuma - 1) * sizeof(parent->bp[0]));
    assert(parent->n_children > 0);
    REALLOC_N(parent->n_children, parent->n_children + 1, parent->bp);

    if (parent->n_children > 1) {
        memmove(&parent->childkeys[childnuma], &parent->childkeys[childnuma + 1],
                (parent->n_children - childnuma - 1) * sizeof(parent->childkeys[0]));
        REALLOC_N(parent->n_children - 1, parent->n_children, parent->childkeys);
    } else {
        toku_free(parent->childkeys);
        parent->childkeys = NULL;
    }
    assert(BP_BLOCKNUM(parent, childnuma).b == a->thisnodename.b);

    parent->dirty = 1;
    a->dirty = 1;
    b->dirty = 1;

    *did_merge = true;
    *did_rebalance = false;

    toku_ft_node_unbound_inserts_validation(a);
    toku_ft_node_unbound_inserts_validation(b);
    STATUS_VALUE(FT_FLUSHER_MERGE_NONLEAF)++;
}

// Effect: either merge a and b into one node (merge them into a) and set *did_merge = true.
//	   (We do this if the resulting node is not fissible)
//	   or distribute a and b evenly and set *did_merge = false and *did_rebalance = true
//	   (If a and be are already evenly distributed, we may do nothing.)
//  If we distribute:
//    For leaf nodes, we distribute the leafentries evenly.
//    For nonleaf nodes, we distribute the children evenly.  That may leave one or both of the nodes overfull, but that's OK.
//  If we distribute, we set *splitk to a malloced pivot key.
// Parameters:
//  t			The BRT.
//  parent		The parent of the two nodes to be split.
//  childnuma           childnum of a, the pivot[childnuma] is either freed()'d or returned in *splitk
//  a			The first node to merge.
//  b			The second node to merge.
//  logger		The logger.
//  did_merge		(OUT):	Did the two nodes actually get merged?
//  splitk		(OUT):	If the two nodes did not get merged, the new pivot key between the two nodes.
static void
ft_merge_pinned_nodes(FT ft, FTNODE parent, int childnuma, FTNODE a, FTNODE b,
                      bool *did_merge, bool *did_rebalance)
{
    MSN msn_max;
    assert(a->height == b->height);
    toku_assert_entire_node_in_memory(parent);
    toku_assert_entire_node_in_memory(a);
    toku_assert_entire_node_in_memory(b);

    {
        MSN msna = a->max_msn_applied_to_node_on_disk;
        MSN msnb = b->max_msn_applied_to_node_on_disk;
        msn_max = (msna.msn > msnb.msn) ? msna : msnb;
    }

    if (a->height == 0) {
        ft_merge_pinned_leaf(ft, parent, childnuma, a, b,
                             did_merge, did_rebalance);
    } else {
        ft_merge_pinned_nonl(ft, parent, childnuma, a, b,
                             did_merge, did_rebalance);
    }

    if (*did_merge || *did_rebalance) {
        // accurate for leaf nodes because all msgs above have been
        // applied, accurate for non-leaf nodes because buffer immediately
        // above each node has been flushed
        a->max_msn_applied_to_node_on_disk = msn_max;
        b->max_msn_applied_to_node_on_disk = msn_max;
    }
}

//
// Takes as input a locked node and a childnum_to_merge
// As output, two of node's children are merged or rebalanced, and node is unlocked
//
static void
ft_merge_child(FT ft, FTNODE node, int childnum_to_merge, bool *did_react,
               struct flusher_advice *fa)
{
    // this function should not be called
    // if the child is not mergable
    assert(node->height > 0);
    assert(node->n_children > 1);
    toku_assert_entire_node_in_memory(node);

    int childnuma, childnumb;
    if (childnum_to_merge > 0) {
        childnuma = childnum_to_merge - 1;
        childnumb = childnum_to_merge;
    } else {
        childnuma = childnum_to_merge;
        childnumb = childnum_to_merge + 1;
    }
    assert(childnuma >= 0);
    assert(childnuma + 1 == childnumb);
    assert(childnumb < node->n_children);

    // We suspect that at least one of the children is fusible, but they might not be.
    // for test
    call_flusher_thread_callback(flt_flush_before_merge);

    // pin children
    FTNODE childa, childb;
    {
        uint32_t childfullhash;
        struct ftnode_fetch_extra bfe;
        childfullhash = compute_child_fullhash(ft->cf, node, childnuma);
        fill_bfe_for_full_read(&bfe, ft);
        toku_pin_ftnode_off_client_thread(
            ft, BP_BLOCKNUM(node, childnuma), childfullhash, &bfe,
            PL_WRITE_EXPENSIVE, 1, &node, &childa);
    }
    // for test
    call_flusher_thread_callback(flt_flush_before_pin_second_node_for_merge);
    {
        FTNODE dep_nodes[2];
        dep_nodes[0] = node;
        dep_nodes[1] = childa;
        uint32_t childfullhash;
        struct ftnode_fetch_extra bfe;
        childfullhash = compute_child_fullhash(ft->cf, node, childnumb);
        fill_bfe_for_full_read(&bfe, ft);
        toku_pin_ftnode_off_client_thread(
            ft, BP_BLOCKNUM(node, childnumb), childfullhash, &bfe,
            PL_WRITE_EXPENSIVE, 2, dep_nodes, &childb);
    }

    flush_this_child(ft, node, &childa, childnuma);
    flush_this_child(ft, node, &childb, childnumb);

    // now we have both children pinned in main memory, and cachetable locked,
    // so no checkpoints will occur.
    bool did_merge, did_rebalance;
    {
    	toku_ft_node_unbound_inserts_validation(childa);
    	toku_ft_node_unbound_inserts_validation(childb);
        ft_merge_pinned_nodes(ft, node, childnuma, childa, childb,
                              &did_merge, &did_rebalance);
        *did_react = (bool)(did_merge || did_rebalance);
    }
    //
    // now we possibly flush the children
    //
    if (did_merge) {
        // for test
        call_flusher_thread_callback(flt_flush_before_unpin_remove);

        toku_cachetable_dec_refc_and_unpin(ft, childb);

        // for test
        call_flusher_thread_callback(ft_flush_aflter_merge);

        // unlock the parent
        assert(node->dirty);
        toku_unpin_ftnode_off_client_thread(ft, node);
    } else {
        // for test
        call_flusher_thread_callback(ft_flush_aflter_rebalance);

        // unlock the parent
        if (did_rebalance) {
            assert(node->dirty);
            assert(childb->dirty);
        }
        toku_unpin_ftnode_off_client_thread(ft, node);
        toku_unpin_ftnode_off_client_thread(ft, childb);
    }

    if (childa->height > 0 && fa->should_recursively_flush(childa, fa->extra)) {
        toku_ft_flush_some_child(ft, childa, fa);
    } else {
        toku_unpin_ftnode_off_client_thread(ft, childa);
    }
}

static void
dummy_update_status(FTNODE UU(child), int UU(dirtied), void *UU(extra))
{
}

static int
dummy_pick_heaviest_child(FT UU(h), FTNODE UU(parent), void* UU(extra))
{
    abort();
    return -1;
}

long count_d = 0;
long count_nd = 0 ;
extern "C" void printf_count_blindwrite(void) {
    printf("range delte at 1st %ld times, otherwise %ld times\n", count_d, count_nd);
}


static void
ft_flush_some_child(FT ft, FTNODE parent, struct flusher_advice *fa)
// Effect: This function does the following:
//   - Pick a child of parent (the heaviest child),
//   - flush from parent to child,
//   - possibly split/merge child.
//   - if child is gorged, recursively proceed with child
//  Note that parent is already locked
//  Upon exit of this function, parent is unlocked and no new
//  new nodes (such as a child) remain locked
{
    int dirtied = 0;
    NONLEAF_CHILDINFO bnc = NULL;
    DBT pbound_l, pbound_r, not_lifted;
    toku_init_dbt(&pbound_l);
    toku_init_dbt(&pbound_r);
    toku_init_dbt(&not_lifted);
    paranoid_invariant(parent->height>0);
    toku_assert_entire_node_in_memory(parent);
    TXNID oldest_referenced_xid = parent->oldest_referenced_xid_known;

    // pick the child we want to flush to
    int childnum = fa->pick_child(ft, parent, fa->extra);

    // for test
    call_flusher_thread_callback(flt_flush_before_child_pin);

    // get the child into memory
    BLOCKNUM targetchild = BP_BLOCKNUM(parent, childnum);
    toku_verify_blocknum_allocated(ft->blocktable, targetchild);
    uint32_t childfullhash = compute_child_fullhash(ft->cf, parent, childnum);
    FTNODE child;
    struct ftnode_fetch_extra bfe;
    // Note that we don't read the entire node into memory yet.
    // The idea is let's try to do the minimum work before releasing the parent lock
    fill_bfe_for_min_read(&bfe, ft);
    toku_pin_ftnode_off_client_thread(ft, targetchild, childfullhash, &bfe, PL_WRITE_EXPENSIVE, 1, &parent, &child);

    // for test
    call_flusher_thread_callback(ft_flush_aflter_child_pin);

    if (fa->should_destroy_basement_nodes(fa)) {
        maybe_destroy_child_blbs(parent, child, ft);
    }
    //Note that at this point, we don't have the entire child in.
    // Let's do a quick check to see if the child may be reactive
    // If the child cannot be reactive, then we can safely unlock
    // the parent before finishing reading in the entire child node.
    bool may_child_be_reactive = may_node_be_reactive(child);

    paranoid_invariant(child->thisnodename.b!=0);
    //VERIFY_NODE(brt, child);

    // only do the following work if there is a flush to perform
    if (toku_bnc_n_entries(BNC(parent, childnum)) > 0 || parent->height == 1) {
        if (!parent->dirty) {
            dirtied++;
            parent->dirty = 1;
        }
        // detach buffer
        BP_WORKDONE(parent, childnum) = 0;  // this buffer is drained, no work has been done by its contents
        {
            int r;
            if (childnum == 0) {
                r = toku_ft_lift_key(ft, &pbound_l, &parent->bound_l, &BP_LIFT(parent, childnum));
            } else {
                r = toku_ft_lift_key(ft, &pbound_l, &parent->childkeys[childnum - 1], &BP_LIFT(parent, childnum));
            }
            assert_zero(r);
            if (childnum == parent->n_children - 1) {
                r = toku_ft_lift_key(ft, &pbound_r, &parent->bound_r, &BP_LIFT(parent, childnum));
            } else {
                r = toku_ft_lift_key(ft, &pbound_r, &parent->childkeys[childnum], &BP_LIFT(parent, childnum));
            }
            assert_zero(r);
        }
        toku_clone_dbt(&not_lifted, BP_NOT_LIFTED(parent, childnum));
        toku_init_dbt(&BP_NOT_LIFTED(parent, childnum));

        bnc = BNC(parent, childnum);
        NONLEAF_CHILDINFO new_bnc = toku_create_empty_nl();
        set_BNC(parent, childnum, new_bnc);
        if(bnc->unbound_insert_count) {
            // we just detached a bnc with unbound inserts, so update parent bookkeeping
            paranoid_invariant(parent->unbound_insert_count >= bnc->unbound_insert_count);
            parent->unbound_insert_count -= bnc->unbound_insert_count;
        }
    }

    // if we are going to break cow, we need to update parent
    bring_node_fully_into_memory(child, ft);
    if (toku_ftnode_maybe_break_cow(ft, &child)) {
        BP_BLOCKNUM(parent, childnum) = child->thisnodename;
    }

    //
    // at this point, the buffer has been detached from the parent
    // and a new empty buffer has been placed in its stead
    // so, if we are absolutely sure that the child is not
    // reactive, we can unpin the parent
    //
    if (!may_child_be_reactive) {
        toku_unpin_ftnode_off_client_thread(ft, parent);
        parent = NULL;
    }

    //
    // now, if necessary, read/decompress the rest of child into memory,
    // so that we can proceed and apply the flush
    //
    /************blindwrite optimization starts *****************/
#if DEAD_LEAF
    struct pacman_opt_mgmt *pacman_manager = NULL;
    if(bnc != NULL) {
        //step 1 extract the fifo msgs and build the pacman opt msg list
        pacman_opt_mgmt_init(pacman_manager);
        toku_fifo_iterate_pacman(bnc->buffer, iterate_fn_bnc_build_pacman_opt, pacman_manager);
        //step 2 run the optimization
        default_run_optimization(pacman_manager, ft);
        //step 3 only for leaves -- skip reading the leaf if the 1st msg in the list is range delete
        if (child->height == 0 && bnc != NULL && toku_fifo_n_entries(bnc->buffer) > 0) {
            FT_MSG first_msg = pacman_get_first_msg(pacman_manager);
            const DBT *key = first_msg->key;
            const DBT *max_key = first_msg->max_key;
            if (FT_DELETE_MULTICAST == ft_msg_get_type(first_msg)) {
                count_d++;
                //test
                call_flusher_thread_callback(flt_flush_enter_blind_write);
                if (!is_on_fringe_of_range(ft, parent, childnum, key, max_key)) {
                    //the whole leaf might be deleted
                    call_flusher_thread_callback(flt_flush_completely_delete_leaf);
                    for(int i = 0; i < child->n_children; i++) {
                        //XIDS xids = ft_msg_get_xids(first_msg);
                        //if(!pass_liveness_check(child->min_ver_nums[i], xids_get_innermost_xid(xids)))
                        if(range_delete_is_granted(first_msg, TXNID_NONE, ft))
                            ft_flush_blind_delete_basement(child, i, first_msg);
                    }
                } else {
                    //partial delete -- some of basements may be gone
                    int start, end;
                    get_child_bounds_for_msg_put(ft->key_ops.keycmp, &ft->cmp_descriptor, child, first_msg, &start, &end);
                    for(int i = start+1; i< end; i++) {
                        //XIDS xids = ft_msg_get_xids(first_msg);
                        //if(!pass_liveness_check(child->min_ver_nums[i], xids_get_innermost_xid(xids)))
                        if(range_delete_is_granted(first_msg, TXNID_NONE, ft))
                            ft_flush_blind_delete_basement(child, i, first_msg);
                        //blindwrite
                    } // now bn fringes
                }

                MSN msg_msn = first_msg->msn;
                if (msg_msn.msn <= child->max_msn_applied_to_node_on_disk.msn) {
                    child->max_msn_applied_to_node_on_disk = msg_msn;
                }
                child->dirty = 1;
                //we applied the range-delete already, now what? should we eliminate it before we proceed? I do not think so as the msn should take care of it.
            } else {
                count_nd++;
            }
        }
    }
#endif
    /* blindwrite optimization ends */
    bring_node_fully_into_memory(child, ft);

    // It is possible after reading in the entire child,
    // that we now know that the child is not reactive
    // if so, we can unpin parent right now
    // we wont be splitting/merging child
    // and we have already replaced the bnc
    // for the root with a fresh one
    enum reactivity child_re = get_node_reactivity(child, ft->h->nodesize);
    if (parent && child_re == RE_STABLE) {
        toku_ft_node_unbound_inserts_validation(parent, 0 , __LINE__);
        toku_unpin_ftnode_off_client_thread(ft, parent);
        parent = NULL;
    }

    // from above, we know at this point that either the bnc
    // is detached from the parent (which may be unpinned),
    // and we have to apply the flush, or there was no data
    // in the buffer to flush, and as a result, flushing is not necessary
    // and bnc is NULL
    if (bnc != NULL) {
        if (!child->dirty) {
            dirtied++;
            child->dirty = 1;
        }
        toku_trace_printk("before flush %d\n", __LINE__);
        // do the actual flush
        toku_bnc_flush_to_child_maybe_slice(
            ft, bnc, &not_lifted, &pbound_l, &pbound_r, child, oldest_referenced_xid);
        destroy_nonleaf_childinfo(bnc);
        toku_cleanup_dbt(&not_lifted);
    }

    if (child) {
        toku_ft_node_unbound_inserts_validation(child, 0, __LINE__);
        //toku_trace_printk("after flush %d\n", __LINE__);
    }
    if (parent) {
        toku_ft_node_unbound_inserts_validation(parent,0, __LINE__);
    }
    fa->update_status(child, dirtied, fa->extra);
    // let's get the reactivity of the child again,
    // it is possible that the flush got rid of some values
    // and now the parent is no longer reactive
    child_re = get_node_reactivity(child, ft->h->nodesize);
    // if the parent has been unpinned above, then
    // this is our only option, even if the child is not stable
    // if the child is not stable, we'll handle it the next
    // time we need to flush to the child
    if (!parent ||
        child_re == RE_STABLE ||
        (child_re == RE_FUSIBLE && parent->n_children == 1)
        )
    {
        if (parent) {
            toku_unpin_ftnode_off_client_thread(ft, parent);
            parent = NULL;
        }
        //
        // it is the responsibility of ft_flush_some_child to unpin child
        //
        if (child->height > 0 && fa->should_recursively_flush(child, fa->extra)) {
            ft_flush_some_child(ft, child, fa);
        }
        else {
            toku_unpin_ftnode_off_client_thread(ft, child);
        }
    }
    else if (child_re == RE_FISSIBLE) {
        //
        // it is responsibility of `ft_split_child` to unlock nodes of
        // parent and child as it sees fit
        //
        paranoid_invariant(parent); // just make sure we have not accidentally unpinned parent
        ft_split_child(ft, parent, childnum, child, SPLIT_EVENLY, fa);
    }
    else if (child_re == RE_FUSIBLE) {
        //
        // it is responsibility of `maybe_merge_child to unlock nodes of
        // parent and child as it sees fit
        //
        paranoid_invariant(parent); // just make sure we have not accidentally unpinned parent
        fa->maybe_merge_child(fa, ft, parent, childnum, child, fa->extra);
    }
    else {
        abort();
    }
}

void toku_ft_flush_some_child(FT ft, FTNODE parent, struct flusher_advice *fa) {
    // Vanilla flush_some_child flushes from parent to child without
    // providing a meaningful oldest_referenced_xid. No simple garbage
    // collection is performed.
    return ft_flush_some_child(ft, parent, fa);
}

static void
update_cleaner_status(
    FTNODE node,
    int childnum)
{
    STATUS_VALUE(FT_FLUSHER_CLEANER_TOTAL_NODES)++;
    if (node->height == 1) {
        STATUS_VALUE(FT_FLUSHER_CLEANER_H1_NODES)++;
    } else {
        STATUS_VALUE(FT_FLUSHER_CLEANER_HGT1_NODES)++;
    }

    unsigned int nbytesinbuf = toku_bnc_nbytesinbuf(BNC(node, childnum));
    if (nbytesinbuf == 0) {
        STATUS_VALUE(FT_FLUSHER_CLEANER_EMPTY_NODES)++;
    } else {
        if (nbytesinbuf > STATUS_VALUE(FT_FLUSHER_CLEANER_MAX_BUFFER_SIZE)) {
            STATUS_VALUE(FT_FLUSHER_CLEANER_MAX_BUFFER_SIZE) = nbytesinbuf;
        }
        if (nbytesinbuf < STATUS_VALUE(FT_FLUSHER_CLEANER_MIN_BUFFER_SIZE)) {
            STATUS_VALUE(FT_FLUSHER_CLEANER_MIN_BUFFER_SIZE) = nbytesinbuf;
        }
        STATUS_VALUE(FT_FLUSHER_CLEANER_TOTAL_BUFFER_SIZE) += nbytesinbuf;

        uint64_t workdone = BP_WORKDONE(node, childnum);
        if (workdone > STATUS_VALUE(FT_FLUSHER_CLEANER_MAX_BUFFER_WORKDONE)) {
            STATUS_VALUE(FT_FLUSHER_CLEANER_MAX_BUFFER_WORKDONE) = workdone;
        }
        if (workdone < STATUS_VALUE(FT_FLUSHER_CLEANER_MIN_BUFFER_WORKDONE)) {
            STATUS_VALUE(FT_FLUSHER_CLEANER_MIN_BUFFER_WORKDONE) = workdone;
        }
        STATUS_VALUE(FT_FLUSHER_CLEANER_TOTAL_BUFFER_WORKDONE) += workdone;
    }
}

void toku_ft_split_child(
    FT ft,
    FTNODE node,
    int childnum,
    FTNODE child,
    enum split_mode split_mode
    )
{
    struct flusher_advice fa;
    flusher_advice_init(
        &fa,
        dummy_pick_heaviest_child,
        dont_destroy_basement_nodes,
        never_recursively_flush,
        default_merge_child,
        dummy_update_status,
        default_pick_child_after_split,
        NULL
        );
    ft_split_child(
        ft,
        node,
        childnum, // childnum to split
        child,
        split_mode,
        &fa
        );
}

void
ft_nonleaf_fetch_child_and_flush(FT ft, FTNODE node, int childnum, FTNODE *child)
{
    paranoid_invariant(node->height > 0);
    BLOCKNUM blocknum = BP_BLOCKNUM(node, childnum);
    uint32_t fullhash = compute_child_fullhash(ft->cf, node, childnum);
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, ft);
    toku_pin_ftnode_off_client_thread(ft, blocknum, fullhash,
                                      &bfe, PL_WRITE_EXPENSIVE, 1, &node,
                                      child);
    flush_this_child(ft, node, child, childnum);
}


void toku_ft_merge_child(
    FT ft,
    FTNODE node,
    int childnum
    )
{
    struct flusher_advice fa;
    flusher_advice_init(
        &fa,
        dummy_pick_heaviest_child,
        dont_destroy_basement_nodes,
        never_recursively_flush,
        default_merge_child,
        dummy_update_status,
        default_pick_child_after_split,
        NULL
        );
    bool did_react;
    ft_merge_child(
        ft,
        node,
        childnum, // childnum to merge
        &did_react,
        &fa
        );
}

int
toku_ftnode_cleaner_callback(
    void *ftnode_pv,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    void *extraargs)
{
    FTNODE node = (FTNODE) ftnode_pv;
    invariant(node->thisnodename.b == blocknum.b);
    invariant(node->fullhash == fullhash);
    invariant(node->height > 0);   // we should never pick a leaf node (for now at least)
    FT h = (FT) extraargs;
    bring_node_fully_into_memory(node, h);
    int childnum = find_heaviest_child(node);
    update_cleaner_status(node, childnum);

    // Either toku_ft_flush_some_child will unlock the node, or we do it here.
    if (toku_bnc_nbytesinbuf(BNC(node, childnum)) > 0) {
        struct flusher_advice fa;
        struct flush_status_update_extra fste;
        ct_flusher_advice_init(&fa, &fste, h->h->nodesize);
        toku_ft_flush_some_child(h, node, &fa);
    } else {
        toku_unpin_ftnode_off_client_thread(h, node);
    }
    return 0;
}

struct flusher_extra {
    FT h;
    FTNODE node;
    NONLEAF_CHILDINFO bnc;
    DBT pbound_l;
    DBT pbound_r;
    DBT not_lifted;
    TXNID oldest_referenced_xid;
};

//
// This is the function that gets called by a
// background thread. Its purpose is to complete
// a flush, and possibly do a split/merge.
//
static void flush_node_fun(void *fe_v)
{
    struct flusher_extra* fe = (struct flusher_extra *) fe_v;
    // The node that has been placed on the background
    // thread may not be fully in memory. Some message
    // buffers may be compressed. Before performing
    // any operations, we must first make sure
    // the node is fully in memory
    //
    // If we have a bnc, that means fe->node is a child, and we've already
    // destroyed its basement nodes if necessary, so we now need to either
    // read them back in, or just do the regular partial fetch.  If we
    // don't, that means fe->node is a parent, so we need to do this anyway.
    bring_node_fully_into_memory(fe->node,fe->h);
    fe->node->dirty = 1;

    struct flusher_advice fa;
    struct flush_status_update_extra fste;
    flt_flusher_advice_init(&fa, &fste, fe->h->h->nodesize);

    if (fe->bnc) {
        // In this case, we have a bnc to flush to a node

        // for test purposes
        call_flusher_thread_callback(flt_flush_before_applying_inbox);

        toku_bnc_flush_to_child_maybe_slice(
            fe->h, fe->bnc, &fe->not_lifted, &fe->pbound_l, &fe->pbound_r, fe->node,
            fe->oldest_referenced_xid);
        toku_ft_node_unbound_inserts_validation(fe->node,0, __LINE__);
        destroy_nonleaf_childinfo(fe->bnc);

        // after the flush has completed, now check to see if the node needs flushing
        // If so, call ft_flush_some_child on the node (because this flush intends to
        // pass a meaningful oldest referenced xid for simple garbage collection), and it is the
        // responsibility of the flush to unlock the node. otherwise, we unlock it here.
        if (fe->node->height > 0 && toku_ft_nonleaf_is_gorged(fe->node, fe->h->h->nodesize)) {
            ft_flush_some_child(fe->h, fe->node, &fa);
        } else {
            toku_unpin_ftnode_off_client_thread(fe->h,fe->node);
        }

        toku_cleanup_dbt(&fe->not_lifted);
        toku_cleanup_dbt(&fe->pbound_l);
        toku_cleanup_dbt(&fe->pbound_r);
    } else {
        // In this case, we were just passed a node with no
        // bnc, which means we are tasked with flushing some
        // buffer in the node.
        // It is the responsibility of flush some child to unlock the node
        ft_flush_some_child(fe->h, fe->node, &fa);
    }
    remove_background_job_from_cf(fe->h->cf);
    toku_free(fe);
}

static void
place_node_and_bnc_on_background_thread(
    FT ft,
    FTNODE parent,
    TXNID oldest_referenced_xid,
    // if childnum < 0, we didn't lock child ignore all following args
    // otherwise, we locked the child
    int childnum,
    NONLEAF_CHILDINFO bnc,
    FTNODE child)
{
    struct flusher_extra *XMALLOC(fe);
    fe->h = ft;
    fe->oldest_referenced_xid = oldest_referenced_xid;
    if (childnum >= 0) {
        int r;
        fe->node = child;
        fe->bnc = bnc;
        if (childnum == 0) {
            r = toku_ft_lift_key(ft, &fe->pbound_l, &parent->bound_l, &BP_LIFT(parent, childnum));
        } else {
            r = toku_ft_lift_key(ft, &fe->pbound_l, &parent->childkeys[childnum - 1], &BP_LIFT(parent, childnum));
        }
        assert_zero(r);
        if (childnum == parent->n_children - 1) {
            r = toku_ft_lift_key(ft, &fe->pbound_r, &parent->bound_r, &BP_LIFT(parent, childnum));
        } else {
            r = toku_ft_lift_key(ft, &fe->pbound_r, &parent->childkeys[childnum], &BP_LIFT(parent, childnum));
        }
        assert_zero(r);
        toku_copy_dbt(&fe->not_lifted, BP_NOT_LIFTED(parent, childnum));
        toku_init_dbt(&BP_NOT_LIFTED(parent, childnum));
    } else {
        fe->node = parent;
        fe->bnc = NULL;
        toku_init_dbt(&fe->pbound_l);
        toku_init_dbt(&fe->pbound_r);
        toku_init_dbt(&fe->not_lifted);
    }

    cachefile_kibbutz_enq(ft->cf, flush_node_fun, fe);
}

//
// This takes as input a gorged, locked,  non-leaf node named parent
// and sets up a flush to be done in the background.
// The flush is setup like this:
//  - We call maybe_get_and_pin_clean on the child we want to flush to in order to try to lock the child
//  - if we successfully pin the child, and the child does not need to be split or merged
//     then we detach the buffer, place the child and buffer onto a background thread, and
//     have the flush complete in the background, and unlock the parent. The child will be
//     unlocked on the background thread
//  - if any of the above does not happen (child cannot be locked,
//     child needs to be split/merged), then we place the parent on the background thread.
//     The parent will be unlocked on the background thread
//
void toku_ft_flush_node_on_background_thread(FT h, FTNODE parent)
{
    TXNID oldest_referenced_xid_known = parent->oldest_referenced_xid_known;
    //
    // first let's see if we can detach buffer on client thread
    // and pick the child we want to flush to
    //
    int childnum = find_heaviest_child(parent);
    paranoid_invariant(toku_bnc_n_entries(BNC(parent, childnum))>0);
    //
    // see if we can pin the child
    //
    FTNODE child;
    uint32_t childfullhash = compute_child_fullhash(h->cf, parent, childnum);
    int r = toku_maybe_pin_ftnode_clean(h, BP_BLOCKNUM(parent, childnum), childfullhash, PL_WRITE_EXPENSIVE, &child);
    if (r != 0) {
        // In this case, we could not lock the child, so just place the parent on the background thread
        // In the callback, we will use toku_ft_flush_some_child, which checks to
        // see if we should blow away the old basement nodes.
        place_node_and_bnc_on_background_thread(
            h, parent, oldest_referenced_xid_known, -1, NULL, NULL);
    } else {
        //
        // successfully locked child
        //
        bool may_child_be_reactive = may_node_be_reactive(child);
        if (!may_child_be_reactive && !toku_blocknum_multi_copy(h->blocktable, child->thisnodename)) {
            // We're going to unpin the parent, so before we do, we must
            // check to see if we need to blow away the basement nodes to
            // keep the MSN invariants intact.
            if (child->shadow_next) {
                assert_zero(child->height);
                toku_destroy_ftnode_shadows(child);
            }

            //
            // can detach buffer and unpin root here
            //
            parent->dirty = 1;
            BP_WORKDONE(parent, childnum) = 0;  // this buffer is drained, no work has been done by its contents
            NONLEAF_CHILDINFO bnc = BNC(parent, childnum);
            NONLEAF_CHILDINFO new_bnc = toku_create_empty_nl();
            set_BNC(parent, childnum, new_bnc);

            if(bnc->unbound_insert_count) {
                // we just detached a bnc with unbound inserts, so update parent bookkeeping
                paranoid_invariant(parent->unbound_insert_count >= bnc->unbound_insert_count);
                parent->unbound_insert_count -= bnc->unbound_insert_count;
            }
            //
            // at this point, the buffer has been detached from the parent
            // and a new empty buffer has been placed in its stead
            // so, because we know for sure the child is not
            // reactive, we can unpin the parent
            place_node_and_bnc_on_background_thread(
                h, parent, oldest_referenced_xid_known, childnum, bnc, child);
            toku_unpin_ftnode(h, parent);
        } else {
            // because the child may be reactive, we need to
            // put parent on background thread.
            // As a result, we unlock the child here.
            toku_unpin_ftnode(h, child);
            // Again, we'll have the parent on the background thread, so
            // we don't need to destroy the basement nodes yet.
            place_node_and_bnc_on_background_thread(
                h, parent, oldest_referenced_xid_known, -1, NULL, NULL);
        }
    }
}

#include <toku_race_tools.h>
void __attribute__((__constructor__)) toku_ft_flusher_helgrind_ignore(void);
void
toku_ft_flusher_helgrind_ignore(void) {
    TOKU_VALGRIND_HG_DISABLE_CHECKING(&ft_flusher_status, sizeof ft_flusher_status);
}

#undef STATUS_VALUE
