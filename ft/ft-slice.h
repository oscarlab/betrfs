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
#define TOKU_DEBUG_SLICE_CHILDKEYLENS 0
#if TOKU_DEBUG_SLICE_CHILDKEYLENS
#define paranoid_debug_childkeylens(a) childkeylens_debug(a)
#else
#define paranoid_debug_childkeylens(a) ((void) 1)
#endif

#ifndef FT_SLICE_H
#define FT_SLICE_H
#include "ft-ops.h"
#include "background_job_manager.h"
#include "ft-search.h"
#define slice_leaf_end_debug 1
#define slice_nonleaf_debug 2
#define slice_left_edge_begin_debug 3
#define slice_right_edge_begin_debug 4
#define slice_both_edges_end_debug 5
#define slice_convergence_found_debug 6
#define slice_end_debug 7
#define relocate_start_sliced_triple 8
#define relocate_finish 9
struct reloc_debug_args {
    FTNODE src_node;
    FTNODE dest_node;
    int src_childnum;
    int dest_childnum;
};

typedef struct ft_slice {
    DBT *l_key;
    DBT *r_key;
} ft_slice_t;

typedef struct toku_slicer_debug_args {
    int depth;
    int childnum;
    FTNODE parent;
    FTNODE child;
} slicer_d_t;


void toku_slicer_thread_set_callback(void (*callback_f)(int, void*, void*), void * );

#if 0
void ft_slice_init(ft_slice_t *, void *, size_t, void *, size_t);
void ft_slice_destroy(ft_slice_t *);
#endif

static inline void
ft_slice_init(ft_slice_t *slice, DBT *l_key, DBT *r_key)
{
    slice->l_key = l_key;
    slice->r_key = r_key;
}

static inline void
ft_slice_destroy(ft_slice_t *UU(slice))
{
    return;
}

void call_slicer_thread_callback(int slicer_state, void * others=nullptr) ;

int toku_ft_slice_quadruple(FT ft,
                            ft_slice_t *src_slice, ft_slice_t *dst_slice,
                            FTNODE *src_above_LCA, int *src_LCA_childnum,
                            FTNODE *dst_above_LCA, int *dst_LCA_childnum
#if HIDE_LATENCY
                            , BACKGROUND_JOB_MANAGER bjm
#endif
                            );
#endif
