/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:

/* The purpose of this file is to provide access to the ft_msg,
 * which is the ephemeral version of the fifo_msg.
 */

#ifndef FT_MSG_H
#define FT_MSG_H

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

//#include "ft/wbuf.h"
//#include "ft/rbuf.h"

uint32_t ft_msg_get_keylen(FT_MSG ft_msg);

uint32_t ft_msg_get_vallen(FT_MSG ft_msg);

XIDS ft_msg_get_xids(FT_MSG ft_msg);

void * ft_msg_get_key(FT_MSG ft_msg);

void * ft_msg_get_val(FT_MSG ft_msg);

void * ft_msg_get_max_key(FT_MSG ft_msg);

uint32_t ft_msg_get_max_keylen(FT_MSG ft_msg);

enum pacman_status ft_msg_get_pm_status(FT_MSG ft_msg);

enum ft_msg_type ft_msg_get_type(FT_MSG ft_msg);

void ft_msg_from_fifo_msg(FT_MSG ft_msg, FIFO_MSG fifo_msg);

void ft_msg_init(FT_MSG, enum ft_msg_type, MSN, XIDS, const DBT *, const DBT *);

MSN ft_msg_get_msn(FT_MSG);
void ft_msg_multicast_init(FT_MSG, enum ft_msg_type, MSN, XIDS, const DBT *, const DBT *, const DBT *, bool, enum pacman_status);

void ft_msg_goto_init(FT_MSG, MSN, XIDS, const DBT *, const DBT *, const DBT *, BLOCKNUM, int);

struct goto_extra *ft_msg_goto_extra(FT_MSG);

void ft_msg_read_from_rbuf(FT_MSG, DBT *, DBT *, DBT *, struct rbuf *, XIDS *, bool *);
bool is_dbt_empty(const DBT *);
const DBT * get_dbt_empty(void);

void ft_msg_write_to_wbuf(FT_MSG, struct wbuf *, int );

#endif  // FT_MSG_H

