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

#ident "Copyright (c) 2011-2013 Tokutek Inc.  All rights reserved."
#ident "$Id$"

#include <toku_portability.h>
#include <zlib.h>

#include "compress.h"
#include "memory.h"
#include "quicklz.h"
#include "toku_assert.h"

/* dp 5/15/15: This funciton is inlined in Linux zlib; hard to export */
#ifdef TOKU_LINUX_MODULE
unsigned long deflateBound(z_streamp dontcare, unsigned long s)
{
        (void) (dontcare);
	return s + ((s + 7) >> 3) + ((s + 63) >> 6) + 11;
}
#endif

static inline enum toku_compression_method
normalize_compression_method(enum toku_compression_method method)
// Effect: resolve "friendly" names like "fast" and "small" into their real values.
{
    //return TOKU_NO_COMPRESSION;
    switch (method) {
    case TOKU_DEFAULT_COMPRESSION_METHOD:
    case TOKU_FAST_COMPRESSION_METHOD:
        return TOKU_QUICKLZ_METHOD;
    default:
        return method; // everything else is fine
    }
}

size_t toku_compress_bound (enum toku_compression_method a, size_t size)
// See compress.h for the specification of this function.
{
    a = normalize_compression_method(a);
    switch (a) {
    case TOKU_NO_COMPRESSION:
        return size + 1;
    case TOKU_QUICKLZ_METHOD:
        return size+400 + 1;  // quicklz manual says 400 bytes is enough.  We need one more byte for the rfc1950-style header byte.  bits 0-3 are 9, bits 4-7 are the QLZ_COMPRESSION_LEVEL.
#ifndef TOKU_LINUX_MODULE
    case TOKU_ZLIB_METHOD:
        return compressBound (size);
#else
        /* dep 5/15/15: Kernel zlib doesn't include a bound
         * estimation.  Just use no compression size */
    case TOKU_ZLIB_METHOD:
        //return size + 1;
        return compressBound(size);
#endif
    case TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD:
        return 2+deflateBound(nullptr, size); // We need one extra for the rfc1950-style header byte, and one extra to  store windowBits (a bit over cautious about future upgrades maybe).
    default:
        break;
    }
    // fall through for bad enum (thus compiler can warn us if we didn't use all the enums
#ifndef TOKU_LINUX_MODULE
    assert(0); 
#else
    abort();
#endif
    return 0;
}

void toku_compress (enum toku_compression_method a,
                    // the following types and naming conventions come from zlib.h
                    Bytef       *dest,   uLongf *destLen,
                    const Bytef *source, uLong   sourceLen)
// See compress.h for the specification of this function.
{
    static const int zlib_compression_level = 5;
    static const int zlib_without_checksum_windowbits = -15;

    a = normalize_compression_method(a);
    assert(sourceLen < (1LL << 32));
    switch (a) {
    case TOKU_NO_COMPRESSION:
        dest[0] = TOKU_NO_COMPRESSION;
        memcpy(dest + 1, source, sourceLen);
        *destLen = sourceLen + 1;
        return;
    case TOKU_ZLIB_METHOD: {
        int r = compress2(dest, destLen, source, sourceLen, zlib_compression_level);
        assert(r == Z_OK);
        assert((dest[0]&0xF) == TOKU_ZLIB_METHOD);
        return;
    }
    case  TOKU_QUICKLZ_METHOD: {
        if (sourceLen==0) {
            // quicklz requires at least one byte, so we handle this ourselves
            assert(1 <= *destLen);
            *destLen = 1;
        } else {
            qlz_state_compress *XCALLOC(qsc);
            size_t actual_destlen = qlz_compress(source, (char*)(dest+1), sourceLen, qsc);
            assert(actual_destlen +1 <= *destLen);
            *destLen = actual_destlen+1; // add one for the rfc1950-style header byte.
            toku_free(qsc);
        }
        // Fill in that first byte
        dest[0] = TOKU_QUICKLZ_METHOD + (QLZ_COMPRESSION_LEVEL << 4);
        return;
    }
    case TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD: {
        z_stream strm;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        strm.next_in = const_cast<Bytef *>(source);
        strm.avail_in = sourceLen;
        int r = deflateInit2_(&strm, zlib_compression_level, Z_DEFLATED,
		  zlib_without_checksum_windowbits, 8, Z_DEFAULT_STRATEGY,
          ZLIB_VERSION, (int)sizeof(z_stream));
        lazy_assert(r == Z_OK);
        strm.next_out = dest + 2;
        strm.avail_out = *destLen - 2;
        r = deflate(&strm, Z_FINISH);
        lazy_assert(r == Z_STREAM_END);
        r = deflateEnd(&strm);
        lazy_assert(r == Z_OK);
        *destLen = strm.total_out + 2;
        dest[0] = TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD + (zlib_compression_level << 4);
        dest[1] = zlib_without_checksum_windowbits;
        return;
    }
    default:
        break;
    }
    // default fall through to error.
#ifndef TOKU_LINUX_MODULE
    assert(0); 
#else
    abort();
#endif
}

void toku_decompress (Bytef       *dest,   uLongf destLen,
                      const Bytef *source, uLongf sourceLen)
// See compress.h for the specification of this function.
{
    assert(sourceLen>=1); // need at least one byte for the RFC header.
    switch (source[0] & 0xF) {
    case TOKU_NO_COMPRESSION:
        memcpy(dest, source + 1, sourceLen - 1);
        return;
    case TOKU_ZLIB_METHOD: {
        uLongf actual_destlen = destLen;
        int r = uncompress(dest, &actual_destlen, source, sourceLen);
        assert(r == Z_OK);
        assert(actual_destlen == destLen);
        return;
    }
    case TOKU_QUICKLZ_METHOD:
        if (sourceLen>1) {
            qlz_state_decompress *XCALLOC(qsd);
            uLongf actual_destlen = qlz_decompress((char*)source+1, dest, qsd);
            assert(actual_destlen == destLen);
            toku_free(qsd);
        } else {
            // length 1 means there is no data, so do nothing.
            assert(destLen==0);
        }
        return;
    case TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD: {
        int r = 0;
        z_stream strm;
        strm.next_in = const_cast<Bytef *>(source + 2);
        strm.avail_in = sourceLen - 2;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        char windowBits = source[1];
        r = inflateInit2(&strm, windowBits);
        lazy_assert(r == Z_OK);
        strm.next_out = dest;
        strm.avail_out = destLen;
        r = inflate(&strm, Z_FINISH);
        lazy_assert(r == Z_STREAM_END);
        r = inflateEnd(&strm);
        lazy_assert(r == Z_OK);
        return;
    }
    }
    // default fall through to error.
#ifndef TOKU_LINUX_MODULE
    assert(0); 
#else
    abort();
#endif
}
