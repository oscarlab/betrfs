/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ifndef FT_CACHETABLE_WRAPPERS_H
#define FT_CACHETABLE_WRAPPERS_H

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

#include <fttypes.h>
#include <ft-internal.h>
#include "cachetable.h"
/**
 * Put an empty node (that is, no fields filled) into the cachetable. 
 * In the process, write dependent nodes out for checkpoint if 
 * necessary.
 */
void
cachetable_put_empty_node_with_dep_nodes(
    FT h,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes,
    BLOCKNUM* name, //output
    uint32_t* fullhash, //output
    FTNODE* result
    );

/**
 * Create a new ftnode with specified height and number of children.
 * In the process, write dependent nodes out for checkpoint if 
 * necessary.
 */
void
create_new_ftnode_with_dep_nodes(
    FT h,
    FTNODE *result,
    int height,
    int n_children,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes
    );

/**
 * Create a new ftnode with specified height
 * and children. 
 * Used for test functions only.
 */
void
toku_create_new_ftnode (
    FT_HANDLE t,
    FTNODE *result,
    int height,
    int n_children
    );

int
toku_pin_ftnode_for_search(
    FT_HANDLE brt,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    UNLOCKERS unlockers,
    bool multipath,
    ANCESTORS ancestors,
    FTNODE_FETCH_EXTRA bfe,
    struct pivot_bounds *bounds,
    FTNODE *node_p,
    bool *node_changed);

/**
 * Unfortunately, this function is poorly named
 * as over time, client threads have also started
 * calling this function.
 * This function returns a pinned ftnode to the caller.
 * Unlike toku_pin_ftnode, this function blocks until the node is pinned.
 */
void
toku_pin_ftnode_off_client_thread(
    FT h,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    FTNODE_FETCH_EXTRA bfe,
    pair_lock_type lock_type,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes,
    FTNODE *node_p
    );

void
toku_pin_ftnode_off_client_thread_and_maybe_move_messages(
    FT h,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    FTNODE_FETCH_EXTRA bfe,
    pair_lock_type lock_type,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes,
    FTNODE *node_p,
    bool move_messages
    );

/**
 * This function may return a pinned ftnode to the caller, if pinning is cheap.
 * If the node is already locked, or is pending a checkpoint, the node is not pinned and -1 is returned.
 */
int toku_maybe_pin_ftnode_clean(FT ft, BLOCKNUM blocknum, uint32_t fullhash, pair_lock_type lock_type, FTNODE *nodep);

/**
 * Batched version of toku_pin_ftnode_off_client_thread, see cachetable
 * batched API for more details.
 */
void
toku_pin_ftnode_off_client_thread_batched_and_maybe_move_messages(
    FT h,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    FTNODE_FETCH_EXTRA bfe,
    pair_lock_type lock_type,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes,
    FTNODE *node_p,
    bool move_messages
    );

/**
 * Batched version of toku_pin_ftnode_off_client_thread, see cachetable
 * batched API for more details.
 */
void
toku_pin_ftnode_off_client_thread_batched(
    FT h,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    FTNODE_FETCH_EXTRA bfe,
    pair_lock_type lock_type,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes,
    FTNODE *node_p
    );

/**
 * Effect: Unpin a brt node. Used for
 * nodes that were pinned off client thread.
 */
void
toku_unpin_ftnode_off_client_thread(FT h, FTNODE node);

/**
 * Effect: Unpin a brt node.
 * Used for nodes pinned on a client thread
 */
void
toku_unpin_ftnode(FT h, FTNODE node);

void
toku_unpin_ftnode_read_only(FT ft, FTNODE node);

#endif
