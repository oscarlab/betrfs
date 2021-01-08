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


#include <toku_portability.h>
#include "fttypes.h"
#include "xids.h"
#include "ft_msg.h"


uint32_t 
ft_msg_get_keylen(FT_MSG ft_msg) {
    uint32_t rval = ft_msg->key->size;
    return rval;
}

uint32_t 
ft_msg_get_vallen(FT_MSG ft_msg) {
    uint32_t rval = ft_msg->val->size;
    return rval;
}

XIDS
ft_msg_get_xids(FT_MSG ft_msg) {
    XIDS rval = ft_msg->xids;
    return rval;
}

void *
ft_msg_get_key(FT_MSG ft_msg) {
    void * rval = ft_msg->key->data;
    return rval;
}

void *
ft_msg_get_val(FT_MSG ft_msg) {
    void * rval = ft_msg->val->data;
    return rval;
}

enum ft_msg_type
ft_msg_get_type(FT_MSG ft_msg) {
    enum ft_msg_type rval = ft_msg->type;
    return rval;
}

void *
ft_msg_get_max_key(FT_MSG ft_msg) {
    void * rval = ft_msg->max_key->data;
    return rval;
}

uint32_t
ft_msg_get_max_keylen(FT_MSG ft_msg) {
    uint32_t rval = ft_msg->max_key->size;
    return rval;
}

enum pacman_status
ft_msg_get_pm_status(FT_MSG ft_msg) {
    assert(FT_DELETE_MULTICAST == ft_msg->type);
    return ft_msg -> u.rd_extra.pm_status;
}

struct goto_extra *ft_msg_goto_extra(FT_MSG ft_msg)
{
    assert(FT_GOTO == ft_msg->type);
    return &ft_msg->u.gt_extra;
}

MSN
ft_msg_get_msn(FT_MSG ft_msg) {
    MSN rval = ft_msg -> msn;
    return rval;
}

static const DBT empty_dbt = {.data=0, .size=0, .ulen=0, .flags=0};
const DBT * get_dbt_empty(void) {
  	return &empty_dbt;
}
bool is_dbt_empty(const DBT * dbt) {
  	return (dbt == &empty_dbt);
}
void ft_msg_init(FT_MSG ft_msg, enum ft_msg_type type, MSN msn, XIDS xids, const DBT * key , const DBT * val){
    ft_msg->type = type;
    ft_msg->msn = msn;
    ft_msg->xids = xids;
    ft_msg->key = key;
    ft_msg->val = val?val:&empty_dbt;
    ft_msg->max_key = &empty_dbt;
    memset(&ft_msg->u,0, sizeof(ft_msg->u));
}

void ft_msg_multicast_init(FT_MSG ft_msg, enum ft_msg_type type, MSN msn, XIDS xids, const DBT * key, const DBT * max_key, const DBT * val, bool is_re, enum pacman_status pm_status){

    ft_msg->type = type;
    ft_msg->msn = msn;
    ft_msg->xids = xids;
    ft_msg->key = key;
    ft_msg->val = val?val:&empty_dbt;
    ft_msg->max_key = max_key;
    ft_msg->u.rd_extra.is_right_excl= is_re;
    ft_msg->u.rd_extra.pm_status = pm_status;
}

void
ft_msg_goto_init(FT_MSG ft_msg, MSN msn, XIDS xids,
                 const DBT *key, const DBT *max_key,
                 const DBT *not_lifted, BLOCKNUM blocknum, int height)
{
    ft_msg->type = FT_GOTO;
    ft_msg->msn = msn;
    ft_msg->xids = xids;
    ft_msg->key = key;
    ft_msg->max_key = max_key;
    ft_msg->val = not_lifted;
    ft_msg->u.gt_extra.blocknum = blocknum;
    ft_msg->u.gt_extra.height = height;
}

static void fill_dbt(DBT *dbt, bytevec k, ITEMLEN len) {
    memset(dbt,0, sizeof(*dbt));
    dbt->size=len;
    dbt->data=(char*)k;
}

void
ft_msg_read_from_rbuf(FT_MSG msg, DBT *k, DBT *v, DBT *m, struct rbuf *rb, XIDS *x, bool *is_fresh)
{
    const void *keyp, *valp;
    uint32_t keylen, vallen;
    enum ft_msg_type t = (enum ft_msg_type) rbuf_char(rb);
    *is_fresh = rbuf_char(rb);
    MSN msn = rbuf_msn(rb);
    xids_create_from_buffer(rb, x);
    rbuf_bytes(rb, &keyp, &keylen);
    rbuf_bytes(rb, &valp, &vallen);

    fill_dbt(k, keyp, keylen);
    fill_dbt(v, valp, vallen);
    if (ft_msg_type_is_multicast(t)) {
        bool is_right_excl = false;
        enum pacman_status pm_status = PM_UNCOMMITTED;
        const void* max_keyp;
        uint32_t max_key_len;
        rbuf_bytes(rb, &max_keyp, &max_key_len);
        paranoid_invariant(max_key_len > 0);
        fill_dbt(m, max_keyp, max_key_len);
        if (t == FT_DELETE_MULTICAST) {
            is_right_excl = rbuf_char(rb);
            pm_status  = (enum pacman_status)rbuf_int(rb);
        }
        ft_msg_multicast_init(msg,t, msn, *x ,k,m,v,is_right_excl, pm_status);
    } else if (FT_GOTO == t) {
        const void *max_keyp;
        uint32_t max_key_len;
        rbuf_bytes(rb, &max_keyp, &max_key_len);
        fill_dbt(m, max_keyp, max_key_len);
        BLOCKNUM blocknum = rbuf_blocknum(rb);
        int height = rbuf_int(rb);
        ft_msg_goto_init(msg, msn, *x, k, m, v, blocknum, height);
    } else {
        memset(m,0,sizeof(*m));
        ft_msg_init(msg, t, msn, *x, k, v);
    }
}

void ft_msg_write_to_wbuf(FT_MSG msg, struct wbuf *wb, int is_fresh)
{
    enum ft_msg_type type = ft_msg_get_type(msg);
    MSN msn = ft_msg_get_msn(msg);
    XIDS xids = ft_msg_get_xids(msg);
    void * key = (char *)ft_msg_get_key(msg);
    int keylen = ft_msg_get_keylen(msg);
    void * val = ft_msg_get_val(msg);
    int vallen = ft_msg_get_vallen(msg);
    wbuf_nocrc_char(wb, (unsigned char)type);
    wbuf_nocrc_char(wb, (unsigned char) is_fresh);
    wbuf_MSN(wb, msn);
    wbuf_nocrc_xids(wb, xids);
    wbuf_nocrc_bytes(wb, key, keylen);
    wbuf_nocrc_bytes(wb, val, vallen);
    if (ft_msg_type_is_multicast(type)) {
        void * max_key = ft_msg_get_max_key(msg);
        int max_keylen = ft_msg_get_max_keylen(msg);
        wbuf_nocrc_bytes(wb, max_key, max_keylen);

        if(type == FT_DELETE_MULTICAST) {
            bool is_re = ft_msg_is_multicast_rightexcl(msg);
            wbuf_nocrc_bool(wb, is_re);
            enum pacman_status pm_status = ft_msg_multicast_pm_status(msg);
            wbuf_nocrc_uint(wb, pm_status);
        }
    } else if (FT_GOTO == type) {
        void *max_key = ft_msg_get_max_key(msg);
        int max_keylen = ft_msg_get_max_keylen(msg);
        wbuf_nocrc_bytes(wb, max_key, max_keylen);

        struct goto_extra *gt = ft_msg_goto_extra(msg);
        wbuf_BLOCKNUM(wb, gt->blocknum);
        wbuf_int(wb, gt->height);
    }
}
