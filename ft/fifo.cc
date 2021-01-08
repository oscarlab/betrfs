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

#include "fifo.h"
#include "xids.h"
#include "ybt.h"
#include <memory.h>
#include <toku_assert.h>

struct fifo {
    int n_items_in_fifo;
    char *memory;       // An array of bytes into which fifo_entries are embedded.
    int   memory_size;  // How big is fifo_memory
    int   memory_used;  // How many bytes are in use?
};

const int fifo_initial_size = 4096;

static void fifo_init(struct fifo *fifo)
{
    fifo->n_items_in_fifo = 0;
    fifo->memory          = 0;
    fifo->memory_size     = 0;
    fifo->memory_used     = 0;
}

__attribute__((const,nonnull))
static int fifo_entry_size(struct fifo_entry *entry)
{
    DBT key, val, max_key;
    FT_MSG_S msg;
    fifo_entry_get_msg(&msg, entry, &key, &val, &max_key);
    return toku_ft_msg_memsize_in_fifo(&msg);
}

/*
__attribute__((const,nonnull))
size_t toku_ft_msg_memsize_in_fifo(FT_MSG cmd) {
    // This must stay in sync with fifo_entry_size because that's what we
    // really trust.  But sometimes we only have an in-memory FT_MSG, not
    // a serialized fifo_entry so we have to fake it.
    return sizeof (struct fifo_entry) + cmd->key->size + cmd->val->size
        + xids_get_size(cmd->xids)
        - sizeof(XIDS_S);
}
*/

int toku_fifo_create(FIFO *ptr)
{
    struct fifo *XMALLOC(fifo);
    if (fifo == 0)
        return ENOMEM;
    fifo_init(fifo);
    *ptr = fifo;
    return 0;
}

void toku_fifo_free(FIFO *ptr)
{
    FIFO fifo = *ptr;
    if (fifo->memory)
        toku_free(fifo->memory);
    fifo->memory = 0;
    toku_free(fifo);
    *ptr = 0;
}

int toku_fifo_n_entries(FIFO fifo)
{
    return fifo->n_items_in_fifo;
}

static int next_power_of_two(int n)
{
    int r = 4096;
    while (r < n) {
        r *= 2;
        assert(r > 0);
    }
    return r;
}

__attribute__((const, nonnull))
size_t toku_ft_msg_memsize_in_fifo(FT_MSG msg) {
    const uint32_t  keylen = ft_msg_get_keylen(msg);
    const uint32_t  datalen = ft_msg_get_vallen(msg);
    const size_t xidslen = xids_get_size(ft_msg_get_xids(msg));
    size_t ret = sizeof(struct fifo_entry) + keylen + datalen + xidslen - sizeof(XIDS_S);
    if(ft_msg_type_is_multicast((enum ft_msg_type)(unsigned char)ft_msg_get_type(msg)))
    {
        uint32_t max_keylen = ft_msg_get_max_keylen(msg);
        ret += sizeof(max_keylen) + max_keylen;

        if (FT_DELETE_MULTICAST == (enum ft_msg_type)(unsigned char)ft_msg_get_type(msg)) {
            ret += sizeof(bool) + sizeof(enum pacman_status);
        }
    } else if (FT_GOTO == (enum ft_msg_type)(unsigned char)ft_msg_get_type(msg)) {

        uint32_t max_keylen = ft_msg_get_max_keylen(msg);
        ret += sizeof(max_keylen) + max_keylen + sizeof(BLOCKNUM) + sizeof(int);
    }
    return ret;
}

int toku_fifo_enq(FIFO fifo, FT_MSG msg, bool is_fresh, int32_t *dest)
{
    int need_space_here = toku_ft_msg_memsize_in_fifo(msg);
    int need_space_total = fifo->memory_used+need_space_here;
    if (fifo->memory == NULL) {
        fifo->memory_size = next_power_of_two(need_space_total);
        fifo->memory = (char *) sb_malloc_sized(fifo->memory_size, true);
    }
    if (need_space_total > fifo->memory_size) {
        // Out of memory at the end.
        int next_2 = next_power_of_two(need_space_total);
        // resize the fifo
        XREALLOC_N(fifo->memory_size, next_2, fifo->memory);
        fifo->memory_size = next_2;
    }
    void *key = ft_msg_get_key(msg);
    void *data = ft_msg_get_val(msg);
    uint32_t keylen = ft_msg_get_keylen(msg);
    uint32_t datalen = ft_msg_get_vallen(msg);
    enum ft_msg_type type = ft_msg_get_type(msg);
    MSN msn = ft_msg_get_msn(msg);
    XIDS xids = ft_msg_get_xids(msg);
    struct fifo_entry *entry = (struct fifo_entry *)(fifo->memory + fifo->memory_used);
    fifo_entry_set_msg_type(entry, type);
    entry->msn = msn;
    xids_cpy(&entry->xids_s, xids);
    entry->is_fresh = is_fresh;
    entry->keylen = keylen;
    unsigned char *e_key = xids_get_end_of_array(&entry->xids_s);
    memcpy(e_key, key, keylen);
    entry->vallen = datalen;
    memcpy(e_key + keylen, data, datalen);
    char *pos = (char *)(e_key + keylen) + datalen;
    if (ft_msg_type_is_multicast((enum ft_msg_type)entry->type)) {
        uint32_t max_keylen = ft_msg_get_max_keylen(msg);
        paranoid_invariant(max_keylen > 0);
        memcpy(pos, &max_keylen, sizeof(max_keylen));
        pos += sizeof(max_keylen);
        memcpy(pos, ft_msg_get_max_key(msg), max_keylen);
        pos += max_keylen;

        if (FT_DELETE_MULTICAST == (enum ft_msg_type) entry->type) {
            bool is_right_excl = ft_msg_is_multicast_rightexcl(msg);
            memcpy(pos, &is_right_excl, sizeof(is_right_excl));
            pos += sizeof(is_right_excl);

            enum pacman_status pm_status = ft_msg_multicast_pm_status(msg);
            memcpy(pos, &pm_status, sizeof(pm_status));
            pos += sizeof(pm_status);
        }
    } else if (FT_GOTO == (enum ft_msg_type)(char) entry->type) {
        uint32_t max_keylen = ft_msg_get_max_keylen(msg);
        paranoid_invariant(max_keylen > 0);
        memcpy(pos, &max_keylen, sizeof(max_keylen));
        pos += sizeof(max_keylen);
        memcpy(pos, ft_msg_get_max_key(msg), max_keylen);
        pos += max_keylen;

        struct goto_extra *gt = ft_msg_goto_extra(msg);
        memcpy(pos, &gt->blocknum, sizeof(gt->blocknum));
        pos += sizeof(gt->blocknum);
        memcpy(pos, &gt->height, sizeof(gt->height));
        pos += sizeof(gt->height);
    }

    if (dest) {
        *dest = fifo->memory_used;
    }
    paranoid_invariant(fifo->memory_used + need_space_here == (int)(pos - fifo->memory));
    fifo->n_items_in_fifo++;
    fifo->memory_used += need_space_here;
    return 0;
}

#pragma GCC push_options
#pragma GCC optimize ("O0")
static int toku_fifo_iterate_internal_start(FIFO UU(fifo)) { return 0; }
static int toku_fifo_iterate_internal_has_more(FIFO fifo, int off) { return off < fifo->memory_used; }
static int toku_fifo_iterate_internal_next(FT_MSG msg, int off) {
    return off + toku_ft_msg_memsize_in_fifo(msg);
}
#pragma GCC pop_options
struct fifo_entry * toku_fifo_iterate_internal_get_entry(FIFO fifo, int off) {
    return (struct fifo_entry *)(fifo->memory + off);
}
size_t toku_fifo_internal_entry_memsize(struct fifo_entry *e) {
    return fifo_entry_size(e);
}

void
fifo_entry_get_msg(FT_MSG ft_msg, struct fifo_entry *entry,
                   DBT *k, DBT *v, DBT *m)
{
    uint32_t keylen = entry->keylen;
    uint32_t vallen = entry->vallen;
    enum ft_msg_type type = (enum ft_msg_type)entry->type;
    MSN msn = entry->msn;
    const XIDS xids = (XIDS) &entry->xids_s;
    const void *key = xids_get_end_of_array(xids);
    const void *val = (uint8_t *)key + entry->keylen;
    if (ft_msg_type_is_multicast(type)) {
        char *pos = (char *)val + vallen;
        uint32_t max_keylen = *(uint32_t *)pos;
        pos += sizeof(max_keylen);
        const void *max_key = pos;
        pos = (char *)max_key + max_keylen;
        bool is_right_excl = false;
        enum pacman_status pm_status = PM_UNCOMMITTED;

        if (FT_DELETE_MULTICAST == type) {
            is_right_excl = *(bool *)pos;
            pos += sizeof(bool);
            pm_status = *(enum pacman_status *)pos;
            pos += sizeof(enum pacman_status);
        }
        ft_msg_multicast_init(ft_msg, type, msn, xids,
                              toku_fill_dbt(k, key, keylen),
                              toku_fill_dbt(m, max_key, max_keylen),
                              toku_fill_dbt(v, val, vallen),
                              is_right_excl, pm_status);
    } else if (FT_GOTO == type) {
        char *pos = (char *)val + vallen;
        uint32_t max_keylen = *(uint32_t *)pos;
        pos += sizeof(max_keylen);
        const void *max_key = pos;
        pos += max_keylen;
        BLOCKNUM blocknum = *(BLOCKNUM *)pos;
        pos += sizeof(BLOCKNUM);
        int height = *(int *)pos;
        pos += sizeof(int);
        ft_msg_goto_init(ft_msg, msn, xids,
                         toku_fill_dbt(k, key, keylen),
                         toku_fill_dbt(m, max_key, max_keylen),
                         toku_fill_dbt(v, val, vallen),
                         blocknum, height);
    } else {
        memset(m, 0, sizeof(*m));
        ft_msg_init(ft_msg, type, msn, xids,
                    toku_fill_dbt(k, key, keylen),
                    toku_fill_dbt(v, val, vallen));
    }
}
//this iterate exposes the fifo entry to the caller
int toku_fifo_iterate_pacman(FIFO fifo,
                             int (*f)(struct fifo_entry *e, void *, FT_MSG &),
                             void *arg)
{
    int fifo_iterate_off;
    int r = 0;
    for (fifo_iterate_off = toku_fifo_iterate_internal_start(fifo);
         toku_fifo_iterate_internal_has_more(fifo, fifo_iterate_off);)
    {
        struct fifo_entry *e = toku_fifo_iterate_internal_get_entry(fifo, fifo_iterate_off);
        FT_MSG msg;
        r = f(e, arg, msg);
        if (r) break;
        fifo_iterate_off = toku_fifo_iterate_internal_next(msg, fifo_iterate_off);
    }
    return r;
}


int toku_fifo_iterate(FIFO fifo,
                      int (*f)(FT_MSG msg, bool is_fresh, void *),
                      void *arg)
{
    int fifo_iterate_off;
    int r = 0;
    for (fifo_iterate_off = toku_fifo_iterate_internal_start(fifo);
         toku_fifo_iterate_internal_has_more(fifo, fifo_iterate_off);)
    {
        struct fifo_entry *e = toku_fifo_iterate_internal_get_entry(fifo, fifo_iterate_off);
        DBT key, val, max_key;
        FT_MSG_S msg;
        fifo_entry_get_msg(&msg, e, &key, &val, &max_key);
        fifo_iterate_off = toku_fifo_iterate_internal_next(&msg, fifo_iterate_off);
        bool is_fresh = e->is_fresh;
        r = f(&msg, is_fresh, arg);
        if (r) break;
    }
    return r;
}

void toku_fifo_iterate_flip_msg_type (FIFO fifo, enum ft_msg_type from_type, enum ft_msg_type to_type)
{
    int fifo_iterate_off;
    for (fifo_iterate_off = toku_fifo_iterate_internal_start(fifo);
          toku_fifo_iterate_internal_has_more(fifo, fifo_iterate_off);)
    {
        struct fifo_entry *e = toku_fifo_iterate_internal_get_entry(fifo, fifo_iterate_off);
        DBT key,  val,  max_key;
        FT_MSG_S msg;
        fifo_entry_get_msg(&msg, e, &key, &val, &max_key);
        if(from_type == e->type)
      	    e->type = to_type;
        fifo_iterate_off = toku_fifo_iterate_internal_next(&msg, fifo_iterate_off);
    }
}
unsigned int toku_fifo_buffer_size_in_use (FIFO fifo)
{
    return fifo->memory_used;
}

unsigned long toku_fifo_memory_size_in_use(FIFO fifo)
{
    return sizeof(*fifo) + fifo->memory_used;
}

unsigned long toku_fifo_memory_footprint(FIFO fifo)
{
    // DEP 11/12/19: I see no reason to query the kernel for how big the allocation
    //               is, given that we already memoize it.  And a lot of expensive reasons
    //               to do the query.  Just return our memoized size.
    size_t size_used = fifo->memory_size;
    long rval = sizeof(*fifo) + size_used;
    return rval;
}

DBT *fill_dbt_for_fifo_entry(DBT *dbt, const struct fifo_entry *entry)
{
    return toku_fill_dbt(dbt, xids_get_end_of_array((XIDS) &entry->xids_s), entry->keylen);
}

struct fifo_entry *toku_fifo_get_entry(FIFO fifo, int off)
{
    return toku_fifo_iterate_internal_get_entry(fifo, off);
}

void toku_fifo_clone(FIFO orig_fifo, FIFO* cloned_fifo)
{
    struct fifo *XMALLOC(new_fifo);
    assert(new_fifo);
    new_fifo->n_items_in_fifo = orig_fifo->n_items_in_fifo;
    new_fifo->memory_used = orig_fifo->memory_used;
    new_fifo->memory_size = new_fifo->memory_used;
    new_fifo->memory = (char *) sb_malloc_sized(new_fifo->memory_size, true);
    memcpy(
        new_fifo->memory,
        orig_fifo->memory,
        new_fifo->memory_size
        );
    *cloned_fifo = new_fifo;
}

bool toku_are_fifos_same(FIFO fifo1, FIFO fifo2)
{
    return (fifo1->memory_used == fifo2->memory_used &&
            memcmp(fifo1->memory, fifo2->memory, fifo1->memory_used) == 0);
}
