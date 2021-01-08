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

#include "ft-cachetable-wrappers.h"
#include "ft-flusher.h"
#include "ft-internal.h"
#include "ft.h"
#include "fttypes.h"
#include "ule.h"

// dummymsn needed to simulate msn because messages are injected at a lower level than toku_ft_root_put_cmd()
#define MIN_DUMMYMSN ((MSN) {(uint64_t)1 << 62})
static MSN dummymsn;      
static LSN dummylsn;
static int testsetup_initialized = 0;
static TOKULOGGER test_logger;

// Must be called before any other test_setup_xxx() functions are called.
void
toku_testsetup_initialize(void) {
    if (testsetup_initialized == 0) {
        testsetup_initialized = 1;
        dummymsn = MIN_DUMMYMSN;
	dummylsn = {0};
    }
}

void toku_testsetup_set_logger(TOKULOGGER * logger) {
	test_logger = * logger;
}
static MSN
next_dummymsn(void) {
    ++(dummymsn.msn);
    return dummymsn;
}
#if 1
MSN get_last_dummymsn(void) {
    return dummymsn;
}
#endif
static LSN
next_dummylsn(void) {
    ++(dummylsn.lsn);
    return dummylsn;
}

bool ignore_if_was_already_open;
int toku_testsetup_leaf(FT_HANDLE brt, BLOCKNUM *blocknum, int n_children, char **keys, int *keylens) {
    FTNODE node;
    assert(testsetup_initialized);
    toku_create_new_ftnode(brt, &node, 0, n_children);
    int i;
    for (i=0; i<n_children; i++) {
        BP_STATE(node,i) = PT_AVAIL;
    }

    for (i=0; i+1<n_children; i++) {
        toku_memdup_dbt(&node->childkeys[i], keys[i], keylens[i]);
        node->totalchildkeylens += keylens[i];
    }

    *blocknum = node->thisnodename;
    toku_unpin_ftnode(brt->ft, node);
    return 0;
}

int toku_testsetup_leaf_lifted(FT_HANDLE brt, BLOCKNUM *blocknum, int n_children, char **keys, int *keylens, char * bound_l, int bound_l_length, char * bound_r, int bound_r_length) {
    FTNODE node;
    assert(testsetup_initialized);
    toku_create_new_ftnode(brt, &node, 0, n_children);
    int i;

    for (i=0; i<n_children; i++) {
        BP_STATE(node,i) = PT_AVAIL;
    }

    for (i=0; i+1<n_children; i++) {
        toku_memdup_dbt(&node->childkeys[i], keys[i], keylens[i]);
        node->totalchildkeylens += keylens[i];
    }

    if(bound_l_length > 0) {
        toku_memdup_dbt(&node->bound_l, bound_l, bound_l_length);
        node->totalchildkeylens += bound_l_length;
    } else {
        toku_init_dbt(&node->bound_l);
    }
    if(bound_r_length > 0) {
        toku_memdup_dbt(&node->bound_r, bound_r, bound_r_length);
        node->totalchildkeylens += bound_r_length;
    } else {
        toku_init_dbt(&node->bound_r);
    }
    *blocknum = node->thisnodename;
    toku_unpin_ftnode(brt->ft, node);
    return 0;
}


// Don't bother to clean up carefully if something goes wrong.  (E.g., it's OK to have malloced stuff that hasn't been freed.)
int toku_testsetup_nonleaf (FT_HANDLE brt, int height, BLOCKNUM *blocknum, int n_children, BLOCKNUM *children, char **keys, int *keylens) {
    FTNODE node;
    assert(testsetup_initialized);
    assert(n_children<=FT_FANOUT);
    toku_create_new_ftnode(brt, &node, height, n_children);
    int i;
    for (i=0; i<n_children; i++) {
        BP_BLOCKNUM(node, i) = children[i];
        BP_STATE(node,i) = PT_AVAIL;
    }
    for (i=0; i+1<n_children; i++) {
        toku_memdup_dbt(&node->childkeys[i], keys[i], keylens[i]);
        node->totalchildkeylens += keylens[i];
    }
    *blocknum = node->thisnodename;
    toku_unpin_ftnode(brt->ft, node);
    return 0;
}

int toku_testsetup_nonleaf_lifted (FT_HANDLE brt, int height, BLOCKNUM *blocknum, int n_children, BLOCKNUM *children, char **keys, int *keylens, char ** lifted_data, int* lifted_lens, char *bound_l, int bound_l_len, char * bound_r, int bound_r_len) {
    FTNODE node;
    assert(testsetup_initialized);
    assert(n_children<=FT_FANOUT);
    toku_create_new_ftnode(brt, &node, height, n_children);
    int i;
    for (i=0; i<n_children; i++) {
        BP_BLOCKNUM(node, i) = children[i];
        BP_STATE(node,i) = PT_AVAIL;
    }
    for (i=0; i+1<n_children; i++) {
        toku_memdup_dbt(&node->childkeys[i], keys[i], keylens[i]);
        node->totalchildkeylens += keylens[i];
    }
    for (i=0; i<n_children; i++) {
        toku_memdup_dbt(&BP_LIFT(node, i), lifted_data[i], lifted_lens[i]);
    }
    if(bound_l_len > 0) {
        toku_memdup_dbt(&node->bound_l, bound_l, bound_l_len);
        node->totalchildkeylens += bound_l_len;
    } else {
        toku_init_dbt(&node->bound_l);
    }
    if(bound_r_len > 0) {
        toku_memdup_dbt(&node->bound_r, bound_r, bound_r_len);
        node->totalchildkeylens += bound_r_len;
    } else {
        toku_init_dbt(&node->bound_r);
    }
    *blocknum = node->thisnodename;
    toku_unpin_ftnode(brt->ft, node);
    return 0;
}
int toku_testsetup_root(FT_HANDLE brt, BLOCKNUM blocknum) {
    assert(testsetup_initialized);
    brt->ft->h->root_blocknum = blocknum;
    return 0;
}

int toku_testsetup_get_sersize(FT_HANDLE brt, BLOCKNUM diskoff) // Return the size on disk
{
    assert(testsetup_initialized);
    void *node_v;
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, brt->ft);
    int r  = toku_cachetable_get_and_pin(
        brt->ft->cf, diskoff,
        toku_cachetable_hash(brt->ft->cf, diskoff),
        &node_v,
        NULL,
        get_write_callbacks_for_node(brt->ft),
        toku_ftnode_fetch_callback,
        toku_ftnode_pf_req_callback,
        toku_ftnode_pf_callback,
        true,
        &bfe
        );
    assert(r==0);
    FTNODE CAST_FROM_VOIDP(node, node_v);
    int size = toku_serialize_ftnode_size(node);
    toku_unpin_ftnode(brt->ft, node);
    return size;
}

int toku_testsetup_insert_to_leaf (FT_HANDLE brt, BLOCKNUM blocknum, const char *key, int keylen, const char *val, int vallen, enum ft_msg_type type) {
    void *node_v;
    int r;

    assert(testsetup_initialized);

    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, brt->ft);
    r = toku_cachetable_get_and_pin(
        brt->ft->cf,
        blocknum,
        toku_cachetable_hash(brt->ft->cf, blocknum),
        &node_v,
        NULL,
        get_write_callbacks_for_node(brt->ft),
	toku_ftnode_fetch_callback,
        toku_ftnode_pf_req_callback,
        toku_ftnode_pf_callback,
        true,
	&bfe
	);
    if (r!=0) return r;
    FTNODE CAST_FROM_VOIDP(node, node_v);
    toku_verify_or_set_counts(node);
    assert(node->height==0);

    DBT keydbt,valdbt;
    MSN msn = next_dummymsn();
    FT_MSG_S cmd;
    ft_msg_init(&cmd, type, msn, xids_get_root_xids(),
                     toku_fill_dbt(&keydbt, key, keylen),
                    toku_fill_dbt(&valdbt, val, vallen) );

    struct ubi_entry *entry = NULL;
    if (FT_UNBOUND_INSERT == type) {
        entry = toku_alloc_ubi_entry(UBI_UNBOUND, next_dummylsn(), cmd.msn, &keydbt);
        toku_logger_append_ubi_entry(test_logger, entry);
    }
    toku_ft_node_put_cmd (
        brt->ft,
        &brt->ft->cmp_descriptor,
        entry,
        node,
        -1,
        &cmd,
        true,
        make_gc_info(true),
        NULL
        );

    toku_verify_or_set_counts(node);

    toku_unpin_ftnode(brt->ft, node);
    return 0;
}

static int
testhelper_string_key_cmp(DB *UU(e), const DBT *a, const DBT *b)
{
    char *CAST_FROM_VOIDP(s, a->data), *CAST_FROM_VOIDP(t, b->data);
    return strcmp(s, t);
}


void
toku_pin_node_with_min_bfe(FTNODE* node, BLOCKNUM b, FT_HANDLE t)
{
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_min_read(&bfe, t->ft);
    toku_pin_ftnode_off_client_thread(
        t->ft, 
        b,
        toku_cachetable_hash(t->ft->cf, b),
        &bfe,
        PL_WRITE_EXPENSIVE,
        0,
        NULL,
        node
        );
}

#if 0
int toku_testsetup_insert_to_nonleaf_with_txn (FT_HANDLE brt, BLOCKNUM blocknum, enum ft_msg_type cmdtype, const char *key, int keylen, const char *val, int vallen, TOKUTXN txn) {


    void *node_v;
    int r;

    XIDS xids = toku_txn_get_xids(txn);   
    assert(testsetup_initialized);

    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, brt->ft);
    r = toku_cachetable_get_and_pin(
        brt->ft->cf,
        blocknum,
        toku_cachetable_hash(brt->ft->cf, blocknum),
        &node_v,
        NULL,
        get_write_callbacks_for_node(brt->ft),
	toku_ftnode_fetch_callback,
        toku_ftnode_pf_req_callback,
        toku_ftnode_pf_callback,
        true,
	&bfe
        );
    if (r!=0) return r;
    FTNODE CAST_FROM_VOIDP(node, node_v);
    assert(node->height>0);

    DBT k;
    int childnum = toku_ftnode_which_child(node,
                                            toku_fill_dbt(&k, key, keylen),
                                            &brt->ft->cmp_descriptor, brt->ft->compare_fun);


    MSN msn = next_dummymsn();
    
    FT_MSG_S msg;
    DBT kdbt, vdbt;
    toku_fill_dbt(&kdbt, key, keylen);
    toku_fill_dbt(&vdbt, val, vallen); 
    ft_msg_init(&msg, cmdtype, msn, xids, &kdbt, &vdbt);
    toku_bnc_insert_msg(BNC(node, childnum), &msg, true, NULL, testhelper_string_key_cmp);
    // Hack to get the test working. The problem is that this test
    // is directly queueing something in a FIFO instead of 
    // using brt APIs.
    node->max_msn_applied_to_node_on_disk = msn;
    node->dirty = 1;
    // Also hack max_msn_in_ft
    brt->ft->h->max_msn_in_ft = msn;

    toku_unpin_ftnode(brt->ft, node);
    return 0;
}
#endif
int toku_testsetup_insert_to_nonleaf (FT_HANDLE brt, BLOCKNUM blocknum, enum ft_msg_type cmdtype, const char *key, int keylen, const char *val, int vallen) {
    void *node_v;
    int r;

    assert(testsetup_initialized);

    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, brt->ft);
    r = toku_cachetable_get_and_pin(
        brt->ft->cf,
        blocknum,
        toku_cachetable_hash(brt->ft->cf, blocknum),
        &node_v,
        NULL,
        get_write_callbacks_for_node(brt->ft),
	toku_ftnode_fetch_callback,
        toku_ftnode_pf_req_callback,
        toku_ftnode_pf_callback,
        true,
	&bfe
        );
    if (r!=0) return r;
    FTNODE CAST_FROM_VOIDP(node, node_v);
    assert(node->height>0);

    DBT k;
    int childnum = toku_ftnode_which_child(node,
                                            toku_fill_dbt(&k, key, keylen),
                                            &brt->ft->cmp_descriptor, brt->ft->key_ops.keycmp);

    XIDS xids_0 = xids_get_root_xids();
    MSN msn = next_dummymsn();
    
    FT_MSG_S msg;
    DBT kdbt, vdbt;
    toku_fill_dbt(&kdbt, key, keylen);
    toku_fill_dbt(&vdbt, val, vallen);
    ft_msg_init(&msg, cmdtype, msn, xids_0, &kdbt, &vdbt);
    struct ubi_entry *entry = NULL;
    if (FT_UNBOUND_INSERT == cmdtype) {
        entry = toku_alloc_ubi_entry(UBI_UNBOUND, next_dummylsn(), msg.msn, &kdbt);
        toku_logger_append_ubi_entry(test_logger, entry);
    }
    toku_bnc_insert_msg(BNC(node, childnum), entry, &msg, true, NULL, testhelper_string_key_cmp);
    // Hack to get the test working. The problem is that this test
    // is directly queueing something in a FIFO instead of 
    // using brt APIs.
    node->max_msn_applied_to_node_on_disk = msn;
    node->dirty = 1;
    if(FT_UNBOUND_INSERT == cmdtype) {
	node->unbound_insert_count++;
}
    // Also hack max_msn_in_ft
    brt->ft->h->max_msn_in_ft = msn;

    toku_unpin_ftnode(brt->ft, node);
    return 0;
}

// adding this function to support multi-cast delete messages
// We can also change the signature of the above function to support all kinds of messages 
// but that will require a lot of change in existing test cases
// for multicast messages, we need a key range (e.g. key and key_max)
int toku_testsetup_insert_to_nonleaf (FT_HANDLE ft_handle, BLOCKNUM blocknum, enum ft_msg_type cmdtype, const char *key, int keylen, const char *max_key, int max_keylen, bool is_right_excl, enum pacman_status pm_status, const char *val, int vallen) {
    void *node_v;
    int r;

    assert(testsetup_initialized);

    ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, ft_handle->ft);
    r = toku_cachetable_get_and_pin(
        ft_handle->ft->cf,
        blocknum,
        toku_cachetable_hash(ft_handle->ft->cf, blocknum),
        &node_v,
        NULL,
        get_write_callbacks_for_node(ft_handle->ft),
	toku_ftnode_fetch_callback,
        toku_ftnode_pf_req_callback,
        toku_ftnode_pf_callback,
        true,
	&bfe
        );
    if (r!=0) return r;
    FTNODE CAST_FROM_VOIDP(node, node_v);
    assert(node->height>0);

    //ok,we are just literally repeating the logic of get_child_bounds_for_msg_put -JYM
    DBT k;
    int start_idx = toku_ftnode_which_child(node, toku_fill_dbt(&k, key, keylen), &ft_handle->ft->cmp_descriptor, ft_handle->ft->key_ops.keycmp);
    int end_idx = toku_ftnode_which_child(node, toku_fill_dbt(&k, max_key, max_keylen), &ft_handle->ft->cmp_descriptor, ft_handle->ft->key_ops.keycmp);
     
    XIDS xids_0 = xids_get_root_xids();
    MSN msn = next_dummymsn();
   
    for (int childnum = start_idx; childnum <= end_idx; childnum++)
   {  
      
        FT_MSG_S msg;
        DBT kdbt, vdbt,mdbt;
        toku_fill_dbt(&kdbt, key, keylen);
        toku_fill_dbt(&vdbt, val, vallen); 
        toku_fill_dbt(&mdbt, max_key, max_keylen);
        ft_msg_multicast_init(&msg, cmdtype, msn, xids_0, &kdbt, &mdbt, &vdbt, is_right_excl, pm_status);
        toku_bnc_insert_msg(BNC(node, childnum), nullptr, &msg, true, NULL, testhelper_string_key_cmp);
         }
    // Hack to get the test working. The problem is that this test
    // is directly queueing something in a FIFO instead of 
    // using ft APIs.
    node->max_msn_applied_to_node_on_disk = msn;
    node->dirty = 1;
    // Also hack max_msn_in_ft
    ft_handle->ft->h->max_msn_in_ft = msn;

    toku_unpin_ftnode(ft_handle->ft, node);
    return 0;
}
