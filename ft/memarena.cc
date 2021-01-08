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

#include <string.h>
#include <memory.h>

#include "memarena.h"

struct memarena_entry {
    char *buf;
    size_t buf_size;
};

struct memarena {
    struct memarena_entry entry;
    size_t buf_used;
    size_t size_of_other_bufs; // the buf_size of all the other bufs.
    struct memarena_entry *other_entries;
    int n_other_bufs;
};

MEMARENA memarena_create_presized (size_t initial_size) {
    MEMARENA XMALLOC(result);
    result->entry.buf_size = initial_size;
    result->buf_used = 0;
    result->other_entries = NULL;
    result->size_of_other_bufs = 0;
    result->n_other_bufs = 0;
    result->entry.buf = (char *) sb_malloc_sized(initial_size, true);
    return result;
}

MEMARENA memarena_create (void) {
    return memarena_create_presized(1024);
}

static size_t
round_to_page (size_t size) {
    const size_t _PAGE_SIZE = 4096;
    const size_t result = _PAGE_SIZE+((size-1)&~(_PAGE_SIZE-1));
    assert(0==(result&(_PAGE_SIZE-1))); // make sure it's aligned
    assert(result>=size);              // make sure it's not too small
    assert(result<size+_PAGE_SIZE);     // make sure we didn't grow by more than a page.
    return result;
}

void* malloc_in_memarena (MEMARENA ma, size_t size) {
    if (ma->entry.buf_size < ma->buf_used + size) {
        // The existing block isn't big enough.
        // Add the block to the vector of blocks.
        if (ma->entry.buf) {
            int old_n = ma->n_other_bufs;
            // XXX: This could be smarter, like pre-allocating a likely size,
            // or incrementing by more than one entry
            REALLOC_N(old_n+1, old_n, ma->other_entries);
            assert(ma->other_entries);
            ma->other_entries[old_n].buf=ma->entry.buf;
            ma->other_entries[old_n].buf_size=ma->entry.buf_size;
            ma->n_other_bufs = old_n+1;
            ma->size_of_other_bufs += ma->entry.buf_size;
        }
        // Make a new one
        {
            size_t new_size = 2*ma->entry.buf_size;
            if (new_size<size) new_size=size;
            new_size=round_to_page(new_size); // at least size, but round to the next page size
            ma->entry.buf = (char *) sb_malloc_sized(new_size, true);
            ma->buf_used = 0;
            ma->entry.buf_size = new_size;
        }
    }
    // allocate in the existing block.
    char *result=ma->entry.buf+ma->buf_used;
    ma->buf_used+=size;
    return result;
}

void *memarena_memdup (MEMARENA ma, const void *v, size_t len) {
    void *r=malloc_in_memarena(ma, len);
    memcpy(r,v,len);
    return r;
}

void memarena_close(MEMARENA *map) {
    MEMARENA ma=*map;
    if (ma->entry.buf) {
        size_t buf_size = ma->entry.buf_size;
        sb_free_sized(ma->entry.buf, buf_size);
        ma->entry.buf=0;
    }
    int i;
    for (i=0; i<ma->n_other_bufs; i++) {
        size_t buf_size = ma->other_entries[i].buf_size;
        sb_free_sized(ma->other_entries[i].buf, buf_size);
    }
    if (ma->other_entries) toku_free(ma->other_entries);
    ma->other_entries=0;
    ma->n_other_bufs=0;
    toku_free(ma);
    *map = 0;
}

#if TOKU_WINDOWS_32
#include <windows.h>
#include <crtdbg.h>
#endif

void memarena_move_buffers(MEMARENA dest, MEMARENA source) {
    int i;
    struct memarena_entry *other_bufs = dest->other_entries;
    static int move_counter = 0;
    move_counter++;
    REALLOC_N(dest->n_other_bufs + source->n_other_bufs + 1, dest->n_other_bufs, other_bufs);
#if TOKU_WINDOWS_32
    if (other_bufs == 0) {
        char **new_other_bufs;
        printf("_CrtCheckMemory:%d\n", _CrtCheckMemory());
        printf("Z: move_counter:%d dest:%p %p %d source:%p %p %d errno:%d\n",
               move_counter,
               dest, dest->other_bufs, dest->n_other_bufs,
               source, source->other_bufs, source->n_other_bufs,
               errno);
        new_other_bufs = toku_malloc((dest->n_other_bufs + source->n_other_bufs + 1)*sizeof (char **));
         printf("new_other_bufs=%p errno=%d\n", new_other_bufs, errno);
    }
#endif

    dest  ->size_of_other_bufs += source->size_of_other_bufs + source->entry.buf_size;
    source->size_of_other_bufs = 0;

    assert(other_bufs);
    dest->other_entries = other_bufs;
    for (i=0; i<source->n_other_bufs; i++) {
        dest->other_entries[dest->n_other_bufs].buf = source->other_entries[i].buf;
        dest->other_entries[dest->n_other_bufs++].buf_size = source->other_entries[i].buf_size;
    }
    dest->other_entries[dest->n_other_bufs].buf = source->entry.buf;
    dest->other_entries[dest->n_other_bufs++].buf_size = source->entry.buf_size;
    source->n_other_bufs = 0;
    toku_free(source->other_entries);
    source->other_entries = 0;
    source->entry.buf = 0;
    source->entry.buf_size = 0;
    source->buf_used = 0;

}

size_t
memarena_total_memory_size (MEMARENA m)
{
    return (memarena_total_size_in_use(m) +
            sizeof(*m) +
            m->n_other_bufs * sizeof(*m->other_entries));
}

size_t
memarena_total_size_in_use (MEMARENA m)
{
    return m->size_of_other_bufs + m->buf_used;
}
