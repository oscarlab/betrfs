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
#include <log-internal.h>
#include <ft-internal.h>
#include <ft-flusher.h>
#include <ft-flusher-internal.h>
#include <ft-cachetable-wrappers.h>
#include <ft.h>
#include <toku_assert.h>
#include <portability/toku_atomic.h>
#include <util/status.h>

static inline int compare_k_x(FT ft, const DBT *k, const DBT *x) {
    FAKE_DB(db, &ft->cmp_descriptor);
    return ft->key_ops.keycmp(&db, k, x);
}

static int default_right_compare(const ft_search_t &search, const DBT *x) {
    FT CAST_FROM_VOIDP(ft, search.context);
    return compare_k_x(ft, search.k, x) >= 0; /* return max xy: kv >= xy */
}

static void
verify_all_in_mempool(FTNODE UU() node)
{
#ifdef TOKU_DEBUG_PARANOID
    if (node->height==0) {
        for (int i = 0; i < node->n_children; i++) {
            invariant(BP_STATE(node,i) == PT_AVAIL);
            BLB_DATA(node, i)->verify_mempool();
        }
    }
#endif
}

static int
heaviside_from_search_t(const DBT &kdbt, ft_search_t &search) {
    int cmp = search.compare(search,
                              search.k ? &kdbt : 0);
    // The search->compare function returns only 0 or 1
    switch (search.direction) {
    case FT_SEARCH_LEFT:   return cmp==0 ? -1 : +1;
    case FT_SEARCH_RIGHT:  return cmp==0 ? +1 : -1; // Because the comparison runs backwards for right searches.
    }
    abort(); return 0;
}

static inline int ft_slice_compare(FT ft, const DBT *a, const DBT *b)
{
    FAKE_DB(db, &ft->cmp_descriptor);
    return ft->key_ops.keycmp(&db, a, b);
}

static void
move_unbound_entries(
    FTNODE B,
    FTNODE node,
    BASEMENTNODE dest_bn,
    BASEMENTNODE src_bn,
    DBT * pivot,
    FT ft)
{
    toku_list *src_unbound_inserts = &src_bn->unbound_inserts;
    toku_list *dest_unbound_inserts = &dest_bn->unbound_inserts;
    paranoid_invariant(toku_list_empty(dest_unbound_inserts));
    toku_list *list = toku_list_head(src_unbound_inserts);
    while(list != src_unbound_inserts) {
        struct unbound_insert_entry *entry = toku_list_struct(list, struct unbound_insert_entry, node_list);
        if(ft_slice_compare(ft, entry->key, pivot) > 0) {
            toku_list *to_be_removed = list;
            list = list->next;
            toku_list_remove(to_be_removed);
            toku_list_push(dest_unbound_inserts, to_be_removed);
            src_bn->unbound_insert_count--;
            node->unbound_insert_count--;
            dest_bn->unbound_insert_count++;
            B->unbound_insert_count++;
        } else {
            list = list->next;
        }
    }
}
static inline void
move_leafentries(BASEMENTNODE dest_bn, BASEMENTNODE src_bn,
                 uint32_t lbi, //lower bound inclusive
                 uint32_t ube //upper bound exclusive
)
//Effect: move leafentries in the range [lbi, upe) from src_omt to newly created dest_omt
{
    src_bn->data_buffer.move_leafentries_to(&dest_bn->data_buffer, lbi, ube);
}

static void
ft_init_new_root_only(FT ft, FTNODE oldroot, FTNODE *newrootp)
// Effect:  Create a new root node only, no splitting!.
//  oldroot is unpinned by us.
//  Leave the new root pinned.
{
    FTNODE newroot;

    BLOCKNUM old_blocknum = oldroot->thisnodename;
    uint32_t old_fullhash = oldroot->fullhash;
    PAIR old_pair = oldroot->ct_pair;

    int new_height = oldroot->height+1;
    uint32_t new_fullhash;
    BLOCKNUM new_blocknum;
    PAIR new_pair = NULL;

    cachetable_put_empty_node_with_dep_nodes(
        ft,
        1,
        &oldroot,
        &new_blocknum,
        &new_fullhash,
        &newroot
        );
    new_pair = newroot->ct_pair;

    assert(newroot);
    assert(new_height > 0);
    toku_initialize_empty_ftnode (
        newroot,
        new_blocknum,
        new_height,
        1,
        ft->h->layout_version,
        ft->h->flags
        );
    MSN msna = oldroot->max_msn_applied_to_node_on_disk;
    newroot->max_msn_applied_to_node_on_disk = msna;
    BP_STATE(newroot,0) = PT_AVAIL;
    newroot->dirty = 1;

    // now do the "switcheroo"
    BP_BLOCKNUM(newroot,0) = new_blocknum;
    newroot->thisnodename = old_blocknum;
    newroot->fullhash = old_fullhash;
    newroot->ct_pair = old_pair;

    oldroot->thisnodename = new_blocknum;
    oldroot->fullhash = new_fullhash;
    oldroot->ct_pair = new_pair;

    toku_cachetable_swap_pair_values(old_pair, new_pair);

    toku_unpin_ftnode_off_client_thread(ft, oldroot);
    *newrootp = newroot;
    //toku_unpin_ftnode_off_client_thread(ft, newroot);
}

static void (*slicer_thread_callback)(int, void*, void*) = NULL;
static void *slicer_thread_callback_extra = NULL;

void toku_slicer_thread_set_callback(void (*callback_f)(int, void*, void *),
                                      void* extra) {
    slicer_thread_callback = callback_f;
    slicer_thread_callback_extra = extra;
}

void call_slicer_thread_callback(int slicer_state, void * others) {
    if (slicer_thread_callback) {
        slicer_thread_callback(slicer_state, slicer_thread_callback_extra, others);
    }
}

static FTNODE
load_next_node(FT ft, FTNODE parent, int child_to_read)
{
    BLOCKNUM childblocknum = BP_BLOCKNUM(parent, child_to_read);
    toku_verify_blocknum_allocated(ft->blocktable, childblocknum);
    uint32_t childfullhash = compute_child_fullhash(ft->cf, parent,
                                                    child_to_read);
    FTNODE child = nullptr;
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, ft);
    toku_trace_printk("%s:%d:about to load node blocknum=%ld,height=%d\n", __func__, __LINE__, childblocknum, parent->height-1);
    toku_pin_ftnode_off_client_thread(ft, childblocknum, childfullhash, &bfe,
                                      PL_WRITE_EXPENSIVE, 1, &parent, &child);
    toku_trace_printk("loading finished\n");

    if (toku_bnc_n_entries(BNC(parent, child_to_read)) > 0)
        toku_ft_flush_this_child(ft, parent, child, child_to_read);
    return child;
}

#define ft_slice_which_child_l(ft, node, key) ft_slice_which_child(ft, node, key, 0)
#define ft_slice_which_child_r(ft, node, key) ft_slice_which_child(ft, node, key, 1)
// return the childnum according to the key, better not call directly
static int
ft_slice_which_child(FT ft, FTNODE node, const DBT *key, int right)
{
    if (node->n_children <= 1)
        return 0;
    int lo = 0;
    int hi = node->n_children - 1;
    int mi;
    while (lo < hi) {
        mi = (lo + hi) / 2;
        int r = ft_slice_compare(ft, key, &node->childkeys[mi]);
        if (r < 0) {
            hi = mi;
        } else if (r == 0) {
            return (right) ? mi : mi + 1;
        } else {
            lo = mi + 1;
        }
    }
    return lo;
}

// which child(ren) are left and right key going to
// if this node is LCA, return 1
static inline int
ft_slice_childnum(FT ft, FTNODE node, ft_slice_t *slice,
                  int *l_childnum, int *r_childnum)
{
    *l_childnum = ft_slice_which_child_l(ft, node, slice->l_key);
    *r_childnum = ft_slice_which_child_r(ft, node, slice->r_key);
    return (*l_childnum != *r_childnum);
}

static inline void
ft_slice_update_bound(FTNODE l, FTNODE r, const DBT *bound)
{
    l->totalchildkeylens = l->totalchildkeylens + bound->size - l->bound_r.size;
    r->totalchildkeylens = r->totalchildkeylens + bound->size + l->bound_r.size;
    toku_copy_dbt(&r->bound_r, l->bound_r);
    toku_memdup_dbt(&l->bound_r, bound->data, bound->size);
    toku_memdup_dbt(&r->bound_l, bound->data, bound->size);
}

static void
ft_slice_update_parent(FT ft,
                       FTNODE parent, int nodenum,
                       FTNODE node, FTNODE sib, const DBT *pivot,
                       DBT **affected_keys, int n_affected_keys)
{
    NONLEAF_CHILDINFO old_bnc = BNC(parent, nodenum);
    NONLEAF_CHILDINFO new_bnc = toku_create_empty_nl();
    paranoid_invariant(toku_bnc_nbytesinbuf(old_bnc) == 0);
    DBT unlifted_pivot, lift_l, lift_r;
    int r;
    toku_init_dbt(&lift_l);
    toku_init_dbt(&lift_r);
    if (ft->key_ops.keylift != NULL) {
        DBT *bound;
        int relifted[n_affected_keys];
        memset(relifted, 0, sizeof(relifted));
        if (BP_LIFT(parent, nodenum).size) {
            toku_init_dbt(&unlifted_pivot);
            r = toku_ft_unlift_key(ft, &unlifted_pivot, pivot, &BP_LIFT(parent, nodenum));
            assert_zero(r);
        } else {
            toku_memdup_dbt(&unlifted_pivot, pivot->data, pivot->size);
        }
        bound = (nodenum == parent->n_children - 1) ?
                &parent->bound_r : &parent->childkeys[nodenum];
        r = toku_ft_lift(ft, &lift_r, &unlifted_pivot, bound);
        assert_zero(r);
        r = toku_ft_node_relift(ft, sib, &BP_LIFT(parent, nodenum), &lift_r);
        assert_zero(r);
        if (BP_LIFT(parent, nodenum).size || lift_r.size) {
            for (int i = 0; i < n_affected_keys; i++) {
                if (ft_slice_compare(ft, affected_keys[i], pivot) > 0) {
                    r = toku_ft_relift_key(ft, affected_keys[i],
                                           &BP_LIFT(parent, nodenum), &lift_r);
                    assert_zero(r);
                    relifted[i] = 1;
                }
            }
        }
        bound = (nodenum == 0) ?
                &parent->bound_l : &parent->childkeys[nodenum - 1];
        r = toku_ft_lift(ft, &lift_l, bound, &unlifted_pivot);
        assert_zero(r);
        r = toku_ft_node_relift(ft, node, &BP_LIFT(parent, nodenum), &lift_l);
        assert_zero(r);
        if (BP_LIFT(parent, nodenum).size || lift_l.size) {
            for (int i = 0; i < n_affected_keys; i++) {
                if (!relifted[i] && ft_slice_compare(ft, affected_keys[i], pivot) < 0) {
                    r = toku_ft_relift_key(ft, affected_keys[i],
                                           &BP_LIFT(parent, nodenum), &lift_l);
                    assert_zero(r);
                }
            }
        }
        toku_cleanup_dbt(&BP_LIFT(parent, nodenum));
        toku_copy_dbt(&BP_LIFT(parent, nodenum), lift_l);
    } else {
        toku_memdup_dbt(&unlifted_pivot, pivot->data, pivot->size);
    }

    paranoid_invariant(parent->n_children > 0);
    parent->n_children += 1;
    XREALLOC_N(parent->n_children, parent->bp);
    XREALLOC_N(parent->n_children - 1, parent->childkeys);

    for (int i = parent->n_children - 1; i > nodenum + 1; i--) {
        parent->bp[i] = parent->bp[i - 1];
        toku_copy_dbt(&parent->childkeys[i - 1], parent->childkeys[i - 2]);
    }
    memset(&parent->bp[nodenum + 1], 0, sizeof(parent->bp[0]));
    BP_BLOCKNUM(parent, nodenum + 1) = sib->thisnodename;
    BP_WORKDONE(parent, nodenum + 1) = 0;
    BP_STATE(parent, nodenum + 1) = PT_AVAIL;
    toku_copy_dbt(&BP_LIFT(parent, nodenum + 1), lift_r);
    set_BNC(parent, nodenum + 1, new_bnc);

    toku_copy_dbt(&parent->childkeys[nodenum], unlifted_pivot);
    parent->totalchildkeylens += unlifted_pivot.size;
}

enum ft_slice_mode {
    FT_SLICE_SRC_L,
    FT_SLICE_SRC_R,
    FT_SLICE_DST_L,
    FT_SLICE_DST_R,
    FT_SLICE_SRC_R_MAYBE_EMPTY
};

// return the new node
// if we don't need to slice return NULL
// when mode is FT_SLICE_SRC_R_MAYBE_EMPTY, return (void *)-ENOENT if we find
//   slice will make node empty
static FTNODE
ft_slice_leaf(FT ft, FTNODE parent, FTNODE node, int nodenum,
              FTNODE *deps, int depth, const DBT *key,
              DBT **affected_keys, int n_affected_keys, enum ft_slice_mode mode)
{
    paranoid_invariant(parent->height == 1);
    paranoid_invariant(node->height == 0);
    deps[depth] = node;

    if (mode == FT_SLICE_SRC_R_MAYBE_EMPTY) {
        if (node->n_children == 1 && BLB_DATA(node, 0)->omt_size() == 0)
            return (FTNODE)(-ENOENT);
    }
    if (ft_slice_compare(ft, &node->bound_l, key) == 0) {
        assert(mode == FT_SLICE_SRC_L || mode == FT_SLICE_DST_L);
        return (FTNODE)NULL;
    }
    if (ft_slice_compare(ft, &node->bound_r, key) == 0) {
        assert(mode == FT_SLICE_SRC_R || mode == FT_SLICE_DST_R || mode == FT_SLICE_SRC_R_MAYBE_EMPTY);
        return (FTNODE)NULL;
    }

    toku_assert_entire_node_in_memory(node);
    verify_all_in_mempool(node);
    int childnum = ft_slice_which_child_r(ft, node, key);
    int num_left_les, n_children_in_node, n_children_in_sib;
    bool slice_on_boundary;
    {
        ft_search_t search;
        ft_search_init(&search, default_right_compare, FT_SEARCH_RIGHT, key, ft);
        int direction = -1;
        uint32_t idx = 0;
        LEAFENTRY le;
        uint32_t le_key_len;
        void *le_key;
        BASEMENTNODE bn = BLB(node, childnum);
        int r = bn->data_buffer.find<ft_search_t &, heaviside_from_search_t>(
                    search, direction, &le, &le_key, &le_key_len, &idx);

        ft_search_finish(&search);
        if (r != 0) {
            assert(r == DB_NOTFOUND);
            n_children_in_node = (childnum == 0) ? 1 : childnum;
            n_children_in_sib = node->n_children - childnum;
            num_left_les = 0;
            slice_on_boundary = true;
        } else {
            n_children_in_node = childnum + 1;
            if (idx + 1 == (int)BLB_DATA(node, childnum)->omt_size()) {
                n_children_in_sib = node->n_children - n_children_in_node;
                if (n_children_in_sib == 0)
                    n_children_in_sib = 1;
                num_left_les = 0;
                slice_on_boundary = true;
                // start transfering from next child
                childnum += 1;
            } else {
                n_children_in_sib = node->n_children - n_children_in_node + 1;
                assert(n_children_in_sib != 0);
                num_left_les = idx + 1;
                slice_on_boundary = false;
            }
        }
    }
    if (mode == FT_SLICE_SRC_R_MAYBE_EMPTY) {
        if (childnum == 0 && num_left_les == 0)
            return (FTNODE)(-ENOENT);
    }

    // start slicing
    if (node->n_children)
        BLB(node, 0)->stat64_delta = toku_get_and_clear_basement_stats(node);

    FTNODE sib = NULL;
    {
        uint32_t fullhash;
        BLOCKNUM name;
        cachetable_put_empty_node_with_dep_nodes(
            ft, depth + 1, deps, &name, &fullhash, &sib);
        invariant_notnull(sib);
        toku_initialize_empty_ftnode(sib, name, 0, n_children_in_sib,
                                     ft->h->layout_version,
                                     ft->h->flags);
        sib->fullhash = fullhash;
    }
    int dst_childnum = 0;
    if (!slice_on_boundary) {
        BP_STATE(sib, dst_childnum) = PT_AVAIL;
        destroy_basement_node(BLB(sib, dst_childnum));
        set_BNULL(sib, dst_childnum);
        set_BLB(sib, dst_childnum, toku_create_empty_bn_no_buffer());
        move_leafentries(BLB(sib, dst_childnum), BLB(node, childnum),
                         num_left_les, BLB_DATA(node, childnum)->omt_size());
        if (BLB(node, childnum)->unbound_insert_count > 0) {
            uint32_t le_key_len;
            void *le_key;
            BN_DATA bd = BLB_DATA(node, childnum);
            int rr = bd->fetch_le_key_and_len(bd->omt_size() - 1,
                                              &le_key_len, &le_key);
            invariant_zero(rr);
            DBT future_pivot;
            toku_init_dbt(&future_pivot);
            toku_memdup_dbt(&future_pivot, le_key, le_key_len);
            move_unbound_entries(sib, node,
                                 BLB(sib, dst_childnum), BLB(node, childnum),
                                 &future_pivot, ft);
            toku_destroy_dbt(&future_pivot);
        }
        BLB_MAX_MSN_APPLIED(sib, dst_childnum) = BLB_MAX_MSN_APPLIED(node, childnum);
        if (dst_childnum < sib->n_children - 1) {
            toku_copy_dbt(&sib->childkeys[dst_childnum], node->childkeys[childnum]);
            sib->totalchildkeylens += node->childkeys[childnum].size;
            node->totalchildkeylens -= node->childkeys[childnum].size;
            toku_init_dbt(&node->childkeys[childnum]);
        }
        childnum += 1;
        dst_childnum += 1;
    } else {
        // we will use our key as pivot, so destroy the pivot if we are not
        // going to move everything to sib or keep everything in node
        if (childnum > 0 && childnum < node->n_children) {
            node->totalchildkeylens -= node->childkeys[childnum - 1].size;
            toku_destroy_dbt(&node->childkeys[childnum - 1]);
        }
    }
    for (; childnum < node->n_children; childnum += 1, dst_childnum += 1) {
        destroy_basement_node(BLB(sib, dst_childnum));
        set_BNULL(sib, dst_childnum);
        sib->bp[dst_childnum] = node->bp[childnum];
        if (childnum == 0) {
            // surely this is the case that leaves node only 1 empty childnum
            BP_WORKDONE(node, 0) = 0;
            BP_INIT_TOUCHED_CLOCK(node, 0);
            set_BNULL(node, 0);
            set_BLB(node, 0, toku_create_empty_bn());
        }
        if (childnum < node->n_children - 1) {
            toku_copy_dbt(&sib->childkeys[dst_childnum], node->childkeys[childnum]);
            sib->totalchildkeylens += node->childkeys[childnum].size;
            node->totalchildkeylens -= node->childkeys[childnum].size;
            toku_init_dbt(&node->childkeys[childnum]);
        }
        uint32_t ubi_count = BLB(node, childnum)->unbound_insert_count;
        if (ubi_count) {
            paranoid_invariant(node->unbound_insert_count >= ubi_count);
            node->unbound_insert_count -= ubi_count;
            sib->unbound_insert_count += ubi_count;
        }
    }
    if (dst_childnum < sib->n_children)
        BP_STATE(sib, dst_childnum) = PT_AVAIL;
    node->n_children = n_children_in_node;
    REALLOC_N(n_children_in_node, node->bp);
    if (n_children_in_node > 1)
        REALLOC_N(n_children_in_node - 1, node->childkeys);
    else {
        toku_free(node->childkeys);
        node->childkeys = NULL;
    }

    ft_slice_update_bound(node, sib, key);

    sib->max_msn_applied_to_node_on_disk = node->max_msn_applied_to_node_on_disk;
    sib->oldest_referenced_xid_known = node->oldest_referenced_xid_known;

    toku_ft_node_unbound_inserts_validation(node);
    toku_ft_node_unbound_inserts_validation(sib);

    ft_slice_update_parent(ft, parent, nodenum, node, sib, key,
                           affected_keys, n_affected_keys);

    parent->dirty = 1;
    node->dirty = 1;
    sib->dirty = 1;

    return sib;
}

static FTNODE
ft_slice_nonl(FT ft, FTNODE parent, FTNODE node, int nodenum,
              FTNODE *deps, int depth, const DBT *key,
              DBT **affected_keys, int n_affected_keys, enum ft_slice_mode mode)
{
    paranoid_invariant(parent->height == node->height + 1);
    paranoid_invariant(node->height > 0);
    deps[depth] = node;

    assert(mode != FT_SLICE_SRC_R_MAYBE_EMPTY);
    if (ft_slice_compare(ft, &node->bound_l, key) == 0) {
        assert(mode == FT_SLICE_SRC_L || mode == FT_SLICE_DST_L);
        return (FTNODE)NULL;
    }
    if (ft_slice_compare(ft, &node->bound_r, key) == 0) {
        assert(mode == FT_SLICE_SRC_R || mode == FT_SLICE_DST_R);
        return (FTNODE)NULL;
    }

    toku_assert_entire_node_in_memory(node);
    int childnum;
    childnum = ft_slice_which_child_r(ft, node, key);
    assert(childnum < node->n_children - 1);
    assert(ft_slice_compare(ft, key, &node->childkeys[childnum]) == 0);
    int n_children_in_node = childnum + 1;
    int n_children_in_sib = node->n_children - n_children_in_node;
    FTNODE sib;
    create_new_ftnode_with_dep_nodes(
        ft, &sib, node->height, n_children_in_sib, depth + 1, deps);
    for (int i = 0; i < n_children_in_sib; i++) {
        destroy_nonleaf_childinfo(BNC(sib, i));
        sib->bp[i] = node->bp[i + n_children_in_node];
        // we pushed everything
        assert(BNC(node, i + n_children_in_node)->unbound_insert_count == 0);
        memset(&node->bp[i + n_children_in_node], 0, sizeof(node->bp[0]));
        if (i < n_children_in_sib - 1) {
            toku_copy_dbt(&sib->childkeys[i], node->childkeys[i + n_children_in_node]);
            sib->totalchildkeylens += node->childkeys[i + n_children_in_node].size;
            node->totalchildkeylens -= node->childkeys[i + n_children_in_node].size;
            toku_init_dbt(&node->childkeys[i + n_children_in_node]);
        }
    }
    node->totalchildkeylens -= node->childkeys[n_children_in_node - 1].size;
    toku_destroy_dbt(&node->childkeys[n_children_in_node - 1]);
    node->n_children = n_children_in_node;
    REALLOC_N(n_children_in_node, node->bp);
    if (n_children_in_node > 1)
        REALLOC_N(n_children_in_node - 1, node->childkeys);
    else {
        toku_free(node->childkeys);
        node->childkeys = NULL;
    }

    ft_slice_update_bound(node, sib, key);

    sib->max_msn_applied_to_node_on_disk = node->max_msn_applied_to_node_on_disk;
    sib->oldest_referenced_xid_known = node->oldest_referenced_xid_known;

    toku_ft_node_unbound_inserts_validation(node);
    toku_ft_node_unbound_inserts_validation(sib);

    ft_slice_update_parent(ft, parent, nodenum, node, sib, key,
                           affected_keys, n_affected_keys);

    parent->dirty = 1;
    node->dirty = 1;
    sib->dirty = 1;

    return sib;
}

typedef FTNODE(*ft_slice_func)(FT, FTNODE, FTNODE, int, FTNODE *, int,
                               const DBT *, DBT **, int, enum ft_slice_mode);

#define ft_slice_src_l(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, func) \
    func(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, FT_SLICE_SRC_L)
#define ft_slice_src_r(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, func) \
    func(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, FT_SLICE_SRC_R)
#define ft_slice_dst_l(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, func) \
    func(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, FT_SLICE_DST_L)
#define ft_slice_dst_r(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, func) \
    func(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, FT_SLICE_DST_R)
#define ft_slice_src_r_ME(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, func) \
    func(ft, parent, node, nodenum, deps, depth, key, ak, n_ak, FT_SLICE_SRC_R_MAYBE_EMPTY)

enum ft_slice_lock_order {
    LOCK_SRC_FIRST,
    LOCK_DST_FIRST
};

// dont want to pass so many things through args, make it inline
static inline int
ft_slice_node_quadruple(FT ft, enum ft_slice_lock_order lock_order,
                        FTNODE *src_l_deps, FTNODE *src_r_deps,
                        FTNODE *dst_l_deps, FTNODE *dst_r_deps, int depth,
                        ft_slice_t *src_slice,
                        FTNODE src_l_parent, FTNODE src_l_node, int src_l_nodenum,
                        FTNODE src_r_parent, FTNODE src_r_node, int src_r_nodenum,
                        ft_slice_t *dst_slice,
                        FTNODE dst_l_parent, FTNODE dst_l_node, int dst_l_nodenum,
                        FTNODE dst_r_parent, FTNODE dst_r_node, int dst_r_nodenum,
                        int *src_LCA_childnum, int *dst_LCA_childnum)
{
    FTNODE sib;
    bool leaf = (src_l_node->height == 0);
    ft_slice_func func = (leaf) ? ft_slice_leaf : ft_slice_nonl;
    DBT sr_key, dl_key, dr_key;
    toku_memdup_dbt(&sr_key, src_slice->r_key->data, src_slice->r_key->size);
    toku_memdup_dbt(&dl_key, dst_slice->l_key->data, dst_slice->l_key->size);
    toku_memdup_dbt(&dr_key, dst_slice->r_key->data, dst_slice->r_key->size);
    DBT *affected_keys[3];
    int n_affected_keys;

    n_affected_keys = 0;
    if (src_r_node == src_l_node) {
        affected_keys[n_affected_keys] = &sr_key;
        n_affected_keys += 1;
    }
    if (dst_l_node == src_l_node) {
        affected_keys[n_affected_keys] = &dl_key;
        n_affected_keys += 1;
    }
    if (dst_r_node == src_l_node) {
        affected_keys[n_affected_keys] = &dr_key;
        n_affected_keys += 1;
    }

    sib = ft_slice_src_l(ft, src_l_parent, src_l_node, src_l_nodenum,
                         src_l_deps, depth + 1, src_slice->l_key,
                         affected_keys, n_affected_keys, func);
    bool maybe_src_empty = leaf && (src_l_node == src_r_node);
    if (sib != NULL) {
        bool unlock_sib = true;
        if (src_r_parent == src_l_parent) {
            if (src_r_node == src_l_node) {
                src_r_node = sib;
                unlock_sib = false;
            }
            src_r_nodenum += 1;
        }
        if (lock_order == LOCK_SRC_FIRST) {
            if (dst_l_parent == src_l_parent) {
                if (dst_l_node == src_l_node) {
                    dst_l_node = sib;
                    unlock_sib = false;
                }
                dst_l_nodenum += 1;
            }
            if (dst_r_parent == src_l_parent) {
                if (dst_r_node == src_l_node) {
                    dst_r_node = sib;
                    unlock_sib = false;
                }
                dst_r_nodenum += 1;
            }
        }
        if (unlock_sib)
            toku_unpin_ftnode(ft, sib);
    }
    if (src_r_node != src_l_node && dst_l_node != src_l_node && dst_r_node != src_l_node)
        toku_unpin_ftnode(ft, src_l_node);

    n_affected_keys = 0;
    if (dst_l_node == src_r_node) {
        affected_keys[n_affected_keys] = &dl_key;
        n_affected_keys += 1;
    }
    if (dst_r_node == src_r_node) {
        affected_keys[n_affected_keys] = &dr_key;
        n_affected_keys += 1;
    }

    if (maybe_src_empty) {
        sib = ft_slice_src_r_ME(ft, src_r_parent, src_r_node, src_r_nodenum,
                                src_r_deps, depth + 1, &sr_key,
                                affected_keys, n_affected_keys, func);
    } else {
        sib = ft_slice_src_r(ft, src_r_parent, src_r_node, src_r_nodenum,
                             src_r_deps, depth + 1, &sr_key,
                             affected_keys, n_affected_keys, func);
    }
    if (sib == (FTNODE)(-ENOENT)) {
        paranoid_invariant(leaf);
        toku_unpin_ftnode(ft, src_r_node);
        if (dst_l_node != src_r_node)
            toku_unpin_ftnode(ft, dst_l_node);
        if (dst_r_node != dst_l_node && dst_r_node != src_r_node)
            toku_unpin_ftnode(ft, dst_r_node);
        *src_LCA_childnum = -1;
        *dst_LCA_childnum = -1;

        toku_destroy_dbt(&sr_key);
        toku_destroy_dbt(&dl_key);
        toku_destroy_dbt(&dr_key);

        return 1;
    }

    if (sib != NULL) {
        bool unlock_sib = true;
        if (lock_order == LOCK_SRC_FIRST) {
            if (dst_l_parent == src_r_parent) {
                if (dst_l_node == src_r_node) {
                    dst_l_node = sib;
                    unlock_sib = false;
                }
                dst_l_nodenum += 1;
            }
            if (dst_r_parent == src_r_parent) {
                if (dst_r_node == src_r_node) {
                    dst_r_node = sib;
                    unlock_sib = false;
                }
                dst_r_nodenum += 1;
            }
        }
        if (unlock_sib)
            toku_unpin_ftnode(ft, sib);
    }
    if (dst_l_node != src_r_node && dst_r_node != src_r_node)
        toku_unpin_ftnode(ft, src_r_node);

    if (dst_r_node == dst_l_node) {
        affected_keys[0] = &dr_key;
        n_affected_keys = 1;
    } else {
        n_affected_keys = 0;
    }
    sib = ft_slice_dst_l(ft, dst_l_parent, dst_l_node, dst_l_nodenum,
                         dst_l_deps, depth + 1, &dl_key,
                         affected_keys, n_affected_keys, func);
    if (sib != NULL) {
        bool unlock_sib = true;
        if (dst_r_parent == dst_l_parent) {
            if (dst_r_node == dst_l_node) {
                dst_r_node = sib;
                unlock_sib = false;
            }
            dst_r_nodenum += 1;
        }
        if (lock_order == LOCK_DST_FIRST && src_r_parent == dst_l_parent) {
            // we need src_r_nodenum as src_LCA_childnum, so we keep track
            src_r_nodenum += 1;
        }
        if (unlock_sib)
            toku_unpin_ftnode(ft, sib);
    }
    if (dst_r_node != dst_l_node)
        toku_unpin_ftnode(ft, dst_l_node);

    n_affected_keys = 0;
    sib = ft_slice_dst_r(ft, dst_r_parent, dst_r_node, dst_r_nodenum,
                         dst_r_deps, depth + 1, &dr_key,
                         affected_keys, n_affected_keys, func);
    if (sib != NULL) {
        if (lock_order == LOCK_DST_FIRST && src_r_parent == dst_r_parent) {
            // we need src_r_nodenum as src_LCA_childnum, so we keep track
            src_r_nodenum += 1;
        }
        toku_unpin_ftnode(ft, sib);
    }
    toku_unpin_ftnode(ft, dst_r_node);

    toku_destroy_dbt(&sr_key);
    toku_destroy_dbt(&dl_key);
    toku_destroy_dbt(&dr_key);

    *dst_LCA_childnum = dst_r_nodenum;
    *src_LCA_childnum = src_r_nodenum;

    return 0;
}

// slice four times, all those parents, nodes might be the same
// enter with everything write_locked,
// return with node unlocked
// unpleasantly long
static int
ft_slice_above_LCA_quadruple(FT ft, enum ft_slice_lock_order lock_order,
                             FTNODE *src_l_deps, FTNODE *src_r_deps,
                             FTNODE *dst_l_deps, FTNODE *dst_r_deps, int depth,
                             ft_slice_t *src_slice,
                             FTNODE src_l_parent, FTNODE src_l_node, int src_l_nodenum,
                             FTNODE src_r_parent, FTNODE src_r_node, int src_r_nodenum,
                             ft_slice_t *dst_slice,
                             FTNODE dst_l_parent, FTNODE dst_l_node, int dst_l_nodenum,
                             FTNODE dst_r_parent, FTNODE dst_r_node, int dst_r_nodenum,
                             int *src_LCA_childnum, int *dst_LCA_childnum)
{
    src_l_deps[depth] = src_l_parent;
    src_r_deps[depth] = src_r_parent;
    dst_l_deps[depth] = dst_l_parent;
    dst_r_deps[depth] = dst_r_parent;

    int r;
    if (src_l_node->height == 0) {
        return ft_slice_node_quadruple(ft, lock_order,
                                       src_l_deps, src_r_deps,
                                       dst_l_deps, dst_r_deps, depth,
                                       src_slice,
                                       src_l_parent, src_l_node, src_l_nodenum,
                                       src_r_parent, src_r_node, src_r_nodenum,
                                       dst_slice,
                                       dst_l_parent, dst_l_node, dst_l_nodenum,
                                       dst_r_parent, dst_r_node, dst_r_nodenum,
                                       src_LCA_childnum, dst_LCA_childnum);
    }

    // load all children in correct order
    int src_l_childnum = ft_slice_which_child_l(ft, src_l_node, src_slice->l_key);
    int src_r_childnum = ft_slice_which_child_r(ft, src_r_node, src_slice->r_key);
    int dst_l_childnum = ft_slice_which_child_l(ft, dst_l_node, dst_slice->l_key);
    int dst_r_childnum = ft_slice_which_child_r(ft, dst_r_node, dst_slice->r_key);
    FTNODE src_l_child, src_r_child, dst_l_child, dst_r_child;
    if (lock_order == LOCK_SRC_FIRST) {
        src_l_child = load_next_node(ft, src_l_node, src_l_childnum);
        if (src_r_node == src_l_node && src_r_childnum == src_l_childnum) {
            src_r_child = src_l_child;
        } else {
            src_r_child = load_next_node(ft, src_r_node, src_r_childnum);
        }
        if (dst_l_node == src_r_node && dst_l_childnum == src_r_childnum) {
            dst_l_child = src_r_child;
        } else {
            dst_l_child = load_next_node(ft, dst_l_node, dst_l_childnum);
        }
        if (dst_r_node == dst_l_node && dst_r_childnum == dst_l_childnum) {
            dst_r_child = dst_l_child;
        } else {
            dst_r_child = load_next_node(ft, dst_r_node, dst_r_childnum);
        }
    } else {
        dst_l_child = load_next_node(ft, dst_l_node, dst_l_childnum);
        if (dst_r_node == dst_l_node && dst_r_childnum == dst_l_childnum) {
            dst_r_child = dst_l_child;
        } else {
            dst_r_child = load_next_node(ft, dst_r_node, dst_r_childnum);
        }
        if (src_l_node == dst_r_node && src_l_childnum == dst_r_childnum) {
            src_l_child = dst_r_child;
        } else {
            src_l_child = load_next_node(ft, src_l_node, src_l_childnum);
        }
        if (src_r_node == src_l_node && src_r_childnum == src_l_childnum) {
            src_r_child = src_l_child;
        } else {
            src_r_child = load_next_node(ft, src_r_node, src_r_childnum);
        }
    }

    // lift everything
    DBT *old_sl, *old_sr, *old_dl, *old_dr;
    DBT new_sl, new_sr, new_dl, new_dr;
    old_sl = src_slice->l_key;
    old_sr = src_slice->r_key;
    old_dl = dst_slice->l_key;
    old_dr = dst_slice->r_key;
    if (BP_LIFT(src_l_node, src_l_childnum).size) {
        r = toku_ft_lift_key_no_alloc(ft, &new_sl, src_slice->l_key, &BP_LIFT(src_l_node, src_l_childnum));
        assert_zero(r);
        src_slice->l_key = &new_sl;
    }
    if (BP_LIFT(src_r_node, src_r_childnum).size) {
        r = toku_ft_lift_key_no_alloc(ft, &new_sr, src_slice->r_key, &BP_LIFT(src_r_node, src_r_childnum));
        assert_zero(r);
        src_slice->r_key = &new_sr;
    }
    if (BP_LIFT(dst_l_node, dst_l_childnum).size) {
        r = toku_ft_lift_key_no_alloc(ft, &new_dl, dst_slice->l_key, &BP_LIFT(dst_l_node, dst_l_childnum));
        assert_zero(r);
        dst_slice->l_key = &new_dl;
    }
    if (BP_LIFT(dst_r_node, dst_r_childnum).size) {
        r = toku_ft_lift_key_no_alloc(ft, &new_dr, dst_slice->r_key, &BP_LIFT(dst_r_node, dst_r_childnum));
        assert_zero(r);
        dst_slice->r_key = &new_dr;
    }

    // recursive call
    r = ft_slice_above_LCA_quadruple(
            ft, lock_order,
            src_l_deps, src_r_deps, dst_l_deps, dst_r_deps, depth + 1,
            src_slice,
            src_l_node, src_l_child, src_l_childnum,
            src_r_node, src_r_child, src_r_childnum,
            dst_slice,
            dst_l_node, dst_l_child, dst_l_childnum,
            dst_r_node, dst_r_child, dst_r_childnum,
            src_LCA_childnum, dst_LCA_childnum);

    // cleanup vestige of lifting
    src_slice->l_key = old_sl;
    src_slice->r_key = old_sr;
    dst_slice->l_key = old_dl;
    dst_slice->r_key = old_dr;

    // src empty we dont slice
    if (r) {
        toku_unpin_ftnode(ft, src_l_node);
        if (src_r_node != src_l_node)
            toku_unpin_ftnode(ft, src_r_node);
        if (dst_l_node != src_r_node && dst_l_node != src_l_node)
            toku_unpin_ftnode(ft, dst_l_node);
        if (dst_r_node != dst_l_node && dst_r_node != src_r_node && dst_r_node != src_l_node)
            toku_unpin_ftnode(ft, dst_r_node);
        return 1;
    }

    return ft_slice_node_quadruple(ft, lock_order,
                                   src_l_deps, src_r_deps,
                                   dst_l_deps, dst_r_deps, depth,
                                   src_slice,
                                   src_l_parent, src_l_node, src_l_nodenum,
                                   src_r_parent, src_r_node, src_r_nodenum,
                                   dst_slice,
                                   dst_l_parent, dst_l_node, dst_l_nodenum,
                                   dst_r_parent, dst_r_node, dst_r_nodenum,
                                   src_LCA_childnum, dst_LCA_childnum);
}

// neither src or dst reaches LCA, so we check chidren
static int
ft_slice_locate_LCA_quadruple(FT ft, enum ft_slice_lock_order lock_order,
                              ft_slice_t *src_slice, FTNODE src_node, int src_childnum,
                              ft_slice_t *dst_slice, FTNODE dst_node, int dst_childnum,
                              FTNODE *src_above_LCA, int *src_LCA_childnum,
                              FTNODE *dst_above_LCA, int *dst_LCA_childnum)
{
    // load child
    FTNODE src_child, dst_child;
    if (lock_order == LOCK_SRC_FIRST) {
        src_child = load_next_node(ft, src_node, src_childnum);
        if (src_node == dst_node && src_childnum == dst_childnum) {
            dst_child = src_child;
        } else {
            dst_child = load_next_node(ft, dst_node, dst_childnum);
        }
    } else {
        dst_child = load_next_node(ft, dst_node, dst_childnum);
        if (src_node == dst_node && src_childnum == dst_childnum) {
            src_child = dst_child;
        } else {
            src_child = load_next_node(ft, src_node, src_childnum);
        }
    }

    // lift with src_node, dst_node
    DBT *old_sl, *old_sr, *old_dl, *old_dr;
    DBT new_sl, new_sr, new_dl, new_dr;
    int r;
    old_sl = src_slice->l_key;
    old_sr = src_slice->r_key;
    old_dl = dst_slice->l_key;
    old_dr = dst_slice->r_key;
    if (BP_LIFT(src_node, src_childnum).size) {
        r = toku_ft_lift_key_no_alloc(ft, &new_sl, src_slice->l_key, &BP_LIFT(src_node, src_childnum));
        assert_zero(r);
        r = toku_ft_lift_key_no_alloc(ft, &new_sr, src_slice->r_key, &BP_LIFT(src_node, src_childnum));
        assert_zero(r);
        src_slice->l_key = &new_sl;
        src_slice->r_key = &new_sr;
    }
    if (BP_LIFT(dst_node, dst_childnum).size) {
        r = toku_ft_lift_key_no_alloc(ft, &new_dl, dst_slice->l_key, &BP_LIFT(dst_node, dst_childnum));
        assert_zero(r);
        r = toku_ft_lift_key_no_alloc(ft, &new_dr, dst_slice->r_key, &BP_LIFT(dst_node, dst_childnum));
        assert_zero(r);
        dst_slice->l_key = &new_dl;
        dst_slice->r_key = &new_dr;
    }

    // figure out childnum and whether we reach LCA
    int src_l_next, src_r_next, dst_l_next, dst_r_next;
    int src_reach_LCA = ft_slice_childnum(ft, src_child, src_slice,
                                          &src_l_next, &src_r_next);
    int dst_reach_LCA = ft_slice_childnum(ft, dst_child, dst_slice,
                                          &dst_l_next, &dst_r_next);
    paranoid_invariant(src_child->height == dst_child->height);

    if (src_child->height != 0 && !src_reach_LCA && !dst_reach_LCA) {
        toku_unpin_ftnode(ft, src_node);
        if (src_node != dst_node)
            toku_unpin_ftnode(ft, dst_node);
        // src_child and dst_child are not LCAs, recursively call
        r = ft_slice_locate_LCA_quadruple(
                ft, lock_order,
                src_slice, src_child, src_l_next,
                dst_slice, dst_child, dst_l_next,
                src_above_LCA, src_LCA_childnum,
                dst_above_LCA, dst_LCA_childnum);
    } else {
        // slice from here
        *src_above_LCA = src_node;
        *dst_above_LCA = dst_node;
        // call to slice, src_child and dst_child are unlocked by them
        FTNODE src_l_deps[src_node->height + 1], src_r_deps[src_node->height + 1],
               dst_l_deps[dst_node->height + 1], dst_r_deps[dst_node->height + 1];
        r = ft_slice_above_LCA_quadruple(
                ft, lock_order,
                src_l_deps, src_r_deps, dst_l_deps, dst_r_deps, 0,
                src_slice, src_node, src_child, src_childnum, src_node, src_child, src_childnum,
                dst_slice, dst_node, dst_child, dst_childnum, dst_node, dst_child, dst_childnum,
                src_LCA_childnum, dst_LCA_childnum);
    }

    // cleanup vestige of lifting
    src_slice->l_key = old_sl;
    src_slice->r_key = old_sr;
    dst_slice->l_key = old_dl;
    dst_slice->r_key = old_dr;

    return r;
}

// go down the tree with src_slice and dst_slice, when either
//   src_slice or dst_slice reaches LCA, it starts slicing
// return 0: there is something to renamie, i.e. src subtree is not empty
// return 1: nothing need to rename
// when return, src_above_LCA, dst_above_LCA (might be the same), src_LCA,
//   dst_LCA (might be the same if nothing to rename) are locked
int toku_ft_slice_quadruple(FT ft,
                            ft_slice_t *src_slice, ft_slice_t *dst_slice,
                            FTNODE *src_above_LCA, int *src_LCA_childnum,
                            FTNODE *dst_above_LCA, int *dst_LCA_childnum
#if HIDE_LATENCY
                            , BACKGROUND_JOB_MANAGER bjm
#endif
                            )
{
    struct ftnode_fetch_extra bfe;
    FTNODE root = NULL;
    fill_bfe_for_full_read(&bfe, ft);
    uint32_t fullhash;
    CACHEKEY root_key;
    toku_calculate_root_offset_pointer(ft, &root_key, &fullhash);
    toku_pin_ftnode_off_client_thread_batched(
        ft, root_key, fullhash, &bfe, PL_WRITE_EXPENSIVE,
        0, NULL, &root);
#if HIDE_LATENCY
    if (bjm) {
        bjm_remove_background_job(bjm);
    }
#endif

    int src_l_childnum, src_r_childnum, dst_l_childnum, dst_r_childnum;
    int src_reach_LCA = ft_slice_childnum(ft, root, src_slice,
                                          &src_l_childnum, &src_r_childnum);
    int dst_reach_LCA = ft_slice_childnum(ft, root, dst_slice,
                                          &dst_l_childnum, &dst_r_childnum);

    int r;
    enum ft_slice_lock_order lock_order;
    if (ft_slice_compare(ft, src_slice->r_key, dst_slice->l_key) < 0)
        lock_order = LOCK_SRC_FIRST;
    else
        lock_order = LOCK_DST_FIRST;
    if (root->height != 0 && !src_reach_LCA && !dst_reach_LCA) {
        r = ft_slice_locate_LCA_quadruple(
                ft, lock_order,
                src_slice, root, src_l_childnum,
                dst_slice, root, dst_l_childnum,
                src_above_LCA, src_LCA_childnum,
                dst_above_LCA, dst_LCA_childnum);
    } else {
        ft_init_new_root_only(ft, root, &root);
        r = ft_slice_locate_LCA_quadruple(
                ft, lock_order,
                src_slice, root, 0,
                dst_slice, root, 0,
                src_above_LCA, src_LCA_childnum,
                dst_above_LCA, dst_LCA_childnum);
    }

    return r;
}
