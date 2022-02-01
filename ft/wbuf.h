/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ifndef WBUF_H
#define WBUF_H
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

#include <memory.h>
#include <string.h>

#include <portability/toku_htonl.h>

#include "fttypes.h"
#include "x1764.h"

#define CRC_INCR

/* When serializing a value, write it into a buffer. */
/* This code requires that the buffer be big enough to hold whatever you put into it. */
/* This abstraction doesn't do a good job of hiding its internals.
 * Why?  The performance of this code is important, and we want to inline stuff */
//Why is size here an int instead of DISKOFF like in the initializer?
struct wbuf {
    unsigned char *buf;
    unsigned int  size;
    unsigned int  ndone;
    struct x1764  checksum;    // The checksum state
#ifdef FT_INDIRECT
    BP_IND_DATA ind_data;
    unsigned int subtract_bytes;
#endif
};

#ifdef FT_INDIRECT
#define WB_PFN_CNT(wb) ((wb->ind_data->pfn_cnt))
#define WB_COPY_PFN(wb, pfn) ((wb->ind_data->pfns)[wb->ind_data->pfn_cnt]) = pfn
#endif

static inline void wbuf_nocrc_init (struct wbuf *w, void *buf, DISKOFF size) {
    w->buf = (unsigned char *) buf;
    w->size = size;
    w->ndone = 0;
#ifdef FT_INDIRECT
    memset(&w->ind_data, 0, sizeof(w->ind_data));
    w->subtract_bytes = 0;
#endif
}

static inline void wbuf_init (struct wbuf *w, void *buf, DISKOFF size) {
    wbuf_nocrc_init(w, buf, size);
    x1764_init(&w->checksum);
}

static inline size_t wbuf_get_woffset(struct wbuf *w) {
    return w->ndone;
}

#ifdef FT_INDIRECT
static inline size_t wbuf_reserve_uint(struct wbuf *w) {
    size_t offset = w->ndone;
    w->ndone += 4;
    return offset;
}
#endif
/* Write a character. */
static inline void wbuf_nocrc_char (struct wbuf *w, unsigned char ch) {
    assert(w->ndone<w->size);
    w->buf[w->ndone++]=ch;
}

/* Write a character. */
static inline void wbuf_nocrc_uint8_t (struct wbuf *w, uint8_t ch) {
    assert(w->ndone<w->size);
    w->buf[w->ndone++]=ch;
}

static inline void wbuf_char (struct wbuf *w, unsigned char ch) {
    wbuf_nocrc_char (w, ch);
    x1764_add(&w->checksum, &w->buf[w->ndone-1], 1);
}

//Write an int that MUST be in network order regardless of disk order
static void wbuf_network_int (struct wbuf *w, int32_t i) __attribute__((__unused__));
static void wbuf_network_int (struct wbuf *w, int32_t i) {
    assert(w->ndone + 4 <= w->size);
    *(uint32_t*)(&w->buf[w->ndone]) = toku_htonl(i);
    x1764_add(&w->checksum, &w->buf[w->ndone], 4);
    w->ndone += 4;
}

static inline void wbuf_nocrc_int (struct wbuf *w, int32_t i) {
#if 0
    wbuf_nocrc_char(w, i>>24);
    wbuf_nocrc_char(w, i>>16);
    wbuf_nocrc_char(w, i>>8);
    wbuf_nocrc_char(w, i>>0);
#else
   assert(w->ndone + 4 <= w->size);
 #if 0
    w->buf[w->ndone+0] = i>>24;
    w->buf[w->ndone+1] = i>>16;
    w->buf[w->ndone+2] = i>>8;
    w->buf[w->ndone+3] = i>>0;
 #else
    *(uint32_t*)(&w->buf[w->ndone]) = toku_htod32(i);
 #endif
    w->ndone += 4;
#endif
}

#ifdef FT_INDIRECT
static inline void wbuf_nocrc_uint_offset(struct wbuf *w, size_t offset, uint32_t i) {
    int32_t tmp = i;
    *(uint32_t*)(&w->buf[offset]) = toku_htod32(tmp);
}
#endif

static inline void wbuf_int (struct wbuf *w, int32_t i) {
    wbuf_nocrc_int(w, i);
    x1764_add(&w->checksum, &w->buf[w->ndone-4], 4);
}

static inline void wbuf_nocrc_uint (struct wbuf *w, uint32_t i) {
    wbuf_nocrc_int(w, (int32_t)i);
}

static inline void wbuf_uint (struct wbuf *w, uint32_t i) {
    wbuf_int(w, (int32_t)i);
}

static inline void wbuf_nocrc_literal_bytes(struct wbuf *w, bytevec bytes_bv, uint32_t nbytes) {
    const unsigned char *bytes = (const unsigned char *) bytes_bv;
#if 0
    { int i; for (i=0; i<nbytes; i++) wbuf_nocrc_char(w, bytes[i]); }
#else
    assert(w->ndone + nbytes <= w->size);
    memcpy(w->buf + w->ndone, bytes, (size_t)nbytes);
    w->ndone += nbytes;
#endif
}

static inline void wbuf_literal_bytes(struct wbuf *w, bytevec bytes_bv, uint32_t nbytes) {
    wbuf_nocrc_literal_bytes(w, bytes_bv, nbytes);
    x1764_add(&w->checksum, &w->buf[w->ndone-nbytes], nbytes);
}

static void wbuf_nocrc_bytes (struct wbuf *w, bytevec bytes_bv, uint32_t nbytes) {
    wbuf_nocrc_uint(w, nbytes);
    wbuf_nocrc_literal_bytes(w, bytes_bv, nbytes);
}

static void wbuf_bytes (struct wbuf *w, bytevec bytes_bv, uint32_t nbytes) {
    wbuf_uint(w, nbytes);
    wbuf_literal_bytes(w, bytes_bv, nbytes);
}

static void wbuf_nocrc_ulonglong (struct wbuf *w, uint64_t ull) {
    wbuf_nocrc_uint(w, (uint32_t)(ull>>32));
    wbuf_nocrc_uint(w, (uint32_t)(ull&0xFFFFFFFF));
}

static void wbuf_ulonglong (struct wbuf *w, uint64_t ull) {
    wbuf_uint(w, (uint32_t)(ull>>32));
    wbuf_uint(w, (uint32_t)(ull&0xFFFFFFFF));
}

static inline void wbuf_nocrc_uint64_t(struct wbuf *w, uint64_t ull) {
    wbuf_nocrc_ulonglong(w, ull);
}


static inline void wbuf_uint64_t(struct wbuf *w, uint64_t ull) {
    wbuf_ulonglong(w, ull);
}

static inline void wbuf_nocrc_bool (struct wbuf *w, bool b) {
    wbuf_nocrc_uint8_t(w, (uint8_t)(b ? 1 : 0));
}

static inline void wbuf_nocrc_BYTESTRING (struct wbuf *w, BYTESTRING v) {
    wbuf_nocrc_bytes(w, v.data, v.len);
}

static inline void wbuf_BYTESTRING (struct wbuf *w, BYTESTRING v) {
    wbuf_bytes(w, v.data, v.len);
}

static inline void wbuf_uint8_t (struct wbuf *w, uint8_t v) {
    wbuf_char(w, v);
}

static inline void wbuf_nocrc_uint32_t (struct wbuf *w, uint32_t v) {
    wbuf_nocrc_uint(w, v);
}

static inline void wbuf_uint32_t (struct wbuf *w, uint32_t v) {
    wbuf_uint(w, v);
}

static inline void wbuf_DISKOFF (struct wbuf *w, DISKOFF off) {
    wbuf_ulonglong(w, (uint64_t)off);
}

static inline void wbuf_nocrc_DISKOFF (struct wbuf *w, DISKOFF off) {
    wbuf_nocrc_ulonglong(w, (uint64_t)off);
}


static inline void wbuf_BLOCKNUM (struct wbuf *w, BLOCKNUM b) {
    wbuf_ulonglong(w, b.b);
}
static inline void wbuf_nocrc_BLOCKNUM (struct wbuf *w, BLOCKNUM b) {
    wbuf_nocrc_ulonglong(w, b.b);
}

static inline void wbuf_nocrc_TXNID (struct wbuf *w, TXNID tid) {
    wbuf_nocrc_ulonglong(w, tid);
}

static inline void wbuf_nocrc_TXNID_PAIR (struct wbuf *w, TXNID_PAIR tid) {
    wbuf_nocrc_ulonglong(w, tid.parent_id64);
    wbuf_nocrc_ulonglong(w, tid.child_id64);
}


static inline void wbuf_TXNID (struct wbuf *w, TXNID tid) {
    wbuf_ulonglong(w, tid);
}

static inline void wbuf_nocrc_XIDP (struct wbuf *w, XIDP xid) {
    wbuf_nocrc_uint32_t(w, xid->formatID);
    wbuf_nocrc_uint8_t(w, xid->gtrid_length);
    wbuf_nocrc_uint8_t(w, xid->bqual_length);
    wbuf_nocrc_literal_bytes(w, xid->data, xid->gtrid_length+xid->bqual_length);
}

static inline void wbuf_nocrc_LSN (struct wbuf *w, LSN lsn) {
    wbuf_nocrc_ulonglong(w, lsn.lsn);
}

static inline void wbuf_LSN (struct wbuf *w, LSN lsn) {
    wbuf_ulonglong(w, lsn.lsn);
}

static inline void wbuf_MSN (struct wbuf *w, MSN msn) {
    wbuf_ulonglong(w, msn.msn);
}

static inline void wbuf_nocrc_MSN (struct wbuf *w, MSN msn) {
    wbuf_nocrc_ulonglong(w, msn.msn);
}


static inline void wbuf_nocrc_FILENUM (struct wbuf *w, FILENUM fileid) {
    wbuf_nocrc_uint(w, fileid.fileid);
}

static inline void wbuf_FILENUM (struct wbuf *w, FILENUM fileid) {
    wbuf_uint(w, fileid.fileid);
}

// 2954
static inline void wbuf_nocrc_FILENUMS (struct wbuf *w, FILENUMS v) {
    wbuf_nocrc_uint(w, v.num);
    uint32_t i;
    for (i = 0; i < v.num; i++) {
        wbuf_nocrc_FILENUM(w, v.filenums[i]);
    }
}

// 2954
static inline void wbuf_FILENUMS (struct wbuf *w, FILENUMS v) {
    wbuf_uint(w, v.num);
    uint32_t i;
    for (i = 0; i < v.num; i++) {
        wbuf_FILENUM(w, v.filenums[i]);
    }
}


#endif
