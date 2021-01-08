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

#include <bndata.h>

static uint32_t klpair_size(KLPAIR klpair){
    return sizeof(*klpair) + klpair->keylen + leafentry_memsize(get_le_from_klpair(klpair));
}

static uint32_t klpair_disksize(KLPAIR klpair){
    return sizeof(*klpair) + klpair->keylen + leafentry_disksize(get_le_from_klpair(klpair));
}

void bn_data::init_zero() {
    toku_mempool_zero(&m_buffer_mempool);
}

void bn_data::initialize_empty() {
    toku_mempool_zero(&m_buffer_mempool);
    m_buffer.create();
}

void bn_data::initialize_from_data(uint32_t num_entries, unsigned char *buf, uint32_t data_size) {
    if (data_size == 0) {
        invariant_zero(num_entries);
        // I believe we can assume the m_buffer is already created here
        return;
    }
    KLPAIR *XMALLOC_N(num_entries, array); // create array of pointers to leafentries
    unsigned char *newmem = NULL;
    // add same wiggle room that toku_mempool_construct would, 25% extra
    uint32_t allocated_bytes = data_size + data_size/4;
    CAST_FROM_VOIDP(newmem, sb_malloc_sized(allocated_bytes, true));
    unsigned char* curr_src_pos = buf;
    unsigned char* curr_dest_pos = newmem;
    for (uint32_t i = 0; i < num_entries; i++) {
        KLPAIR curr_kl = (KLPAIR)curr_dest_pos;
        array[i] = curr_kl;

        uint8_t curr_type = curr_src_pos[0];
        curr_src_pos++;
        // first thing we do is lay out the key,
        // to do so, we must extract it from the leafentry
        // and write it in
        uint32_t keylen = 0;
        void* keyp = NULL;
        keylen = *(uint32_t *)curr_src_pos;
        curr_src_pos += sizeof(uint32_t);
        uint32_t clean_vallen = 0;
        uint32_t num_cxrs = 0;
        uint8_t num_pxrs = 0;
        if (curr_type == LE_CLEAN) {
            clean_vallen = toku_dtoh32(*(uint32_t *)curr_src_pos);
            curr_src_pos += sizeof(clean_vallen); // val_len
            keyp = curr_src_pos;
            curr_src_pos += keylen;
        }
        else {
            paranoid_invariant(curr_type >= LE_MVCC && curr_type < LE_MVCC_END);
            num_cxrs = toku_htod32(*(uint32_t *)curr_src_pos);
            curr_src_pos += sizeof(uint32_t); // num_cxrs
            num_pxrs = curr_src_pos[0];
            curr_src_pos += sizeof(uint8_t); //num_pxrs
            keyp = curr_src_pos;
            curr_src_pos += keylen;
        }
        // now that we have the keylen and the key, we can copy it
        // into the destination
        *(uint32_t *)curr_dest_pos = keylen;
        curr_dest_pos += sizeof(keylen);
        memcpy(curr_dest_pos, keyp, keylen);
        curr_dest_pos += keylen;
        // now curr_dest_pos is pointing to where the leafentry should be packed
        curr_dest_pos[0] = curr_type;
        curr_dest_pos++;
        if (curr_type == LE_CLEAN) {
             *(uint32_t *)curr_dest_pos = toku_htod32(clean_vallen);
             curr_dest_pos += sizeof(clean_vallen);
             memcpy(curr_dest_pos, curr_src_pos, clean_vallen); // copy the val
             curr_dest_pos += clean_vallen;
             curr_src_pos += clean_vallen;
        }
        else {
            // pack num_cxrs and num_pxrs
            *(uint32_t *)curr_dest_pos = toku_htod32(num_cxrs);
            curr_dest_pos += sizeof(num_cxrs);
            *(uint8_t *)curr_dest_pos = num_pxrs;
            curr_dest_pos += sizeof(num_pxrs);
            // now we need to pack the rest of the data
            uint32_t num_rest_bytes = leafentry_rest_memsize(num_pxrs, num_cxrs, curr_src_pos);
            memcpy(curr_dest_pos, curr_src_pos, num_rest_bytes);
            curr_dest_pos += num_rest_bytes;
            curr_src_pos += num_rest_bytes;
        }
    }
    uint32_t num_bytes_read UU() = (uint32_t)(curr_src_pos - buf);
    paranoid_invariant( num_bytes_read == data_size);
    uint32_t num_bytes_written = curr_dest_pos - newmem;
    paranoid_invariant( num_bytes_written == data_size);
    toku_mempool_init(&m_buffer_mempool, newmem, (size_t)(num_bytes_written), allocated_bytes);

    // destroy old omt that was created by toku_create_empty_bn(), so we can create a new one
    m_buffer.destroy();
    m_buffer.create_steal_sorted_array(&array, num_entries, num_entries);
}

uint64_t bn_data::get_memory_size() {
    uint64_t retval = 0;
    // include fragmentation overhead but do not include space in the
    // mempool that has not yet been allocated for leaf entries
    size_t poolsize = toku_mempool_footprint(&m_buffer_mempool);
    invariant(poolsize >= get_disk_size());
    retval += poolsize;
    retval += m_buffer.memory_size();
    return retval;
}

void bn_data::delete_leafentry (
    uint32_t idx,
    uint32_t keylen,
    uint32_t old_le_size
    )
{
    m_buffer.delete_at(idx);
    toku_mempool_mfree(&m_buffer_mempool, 0, old_le_size + keylen + sizeof(keylen)); // Must pass 0, since le is no good any more.
}

/* mempool support */

struct omt_compressor_state {
    struct mempool *new_kvspace;
    KLPAIR *newvals;
};

static int move_it (const KLPAIR &klpair, const uint32_t idx, struct omt_compressor_state * const oc) {
    uint32_t size = klpair_size(klpair);
    KLPAIR CAST_FROM_VOIDP(newdata, toku_mempool_malloc(oc->new_kvspace, size, 1));
    paranoid_invariant_notnull(newdata); // we do this on a fresh mempool, so nothing bad should happen
    memcpy(newdata, klpair, size);
    oc->newvals[idx] = newdata;
    return 0;
}

// Compress things, and grow the mempool if needed.
void bn_data::omt_compress_kvspace(size_t added_size, void **maybe_free) {
    uint32_t total_size_needed = toku_mempool_get_used_space(&m_buffer_mempool) + added_size;
    if (total_size_needed+total_size_needed >= m_buffer_mempool.size) {
        m_buffer_mempool.size = total_size_needed+total_size_needed;
    }
    struct mempool new_kvspace;
    toku_mempool_construct(&new_kvspace, m_buffer_mempool.size);
    uint32_t numvals = omt_size();
    size_t new_size_bytes = numvals * sizeof(KLPAIR);
    KLPAIR *newvals = NULL;
    if (new_size_bytes) {
        newvals = (KLPAIR *) sb_malloc_sized(new_size_bytes, true);
    }
    struct omt_compressor_state oc = { &new_kvspace, newvals };

    m_buffer.iterate_on_range< decltype(oc), move_it >(0, omt_size(), &oc);

    m_buffer.destroy();
    m_buffer.create_steal_sorted_array(&newvals, numvals, numvals);

    if (maybe_free) {
        *maybe_free = m_buffer_mempool.base;
    } else {
        sb_free_sized(m_buffer_mempool.base, m_buffer_mempool.size);
    }
    m_buffer_mempool = new_kvspace;
}

// Effect: Allocate a new object of size SIZE in MP.  If MP runs out of space, allocate new a new mempool space, and copy all the items
//  from the OMT (which items refer to items in the old mempool) into the new mempool.
//  If MAYBE_FREE is NULL then free the old mempool's space.
//  Otherwise, store the old mempool's space in maybe_free.
KLPAIR bn_data::mempool_malloc_from_omt(size_t size, void **maybe_free) {
    void *v = toku_mempool_malloc(&m_buffer_mempool, size, 1);
    if (v == NULL) {
        omt_compress_kvspace(size, maybe_free);
        v = toku_mempool_malloc(&m_buffer_mempool, size, 1);
        paranoid_invariant_notnull(v);
    }
    return (KLPAIR)v;
}

//TODO: probably not free the "maybe_free" right away?
void bn_data::get_space_for_overwrite(
    uint32_t idx,
    const void* keyp,
    uint32_t keylen,
    uint32_t old_le_size,
    uint32_t new_size,
    LEAFENTRY* new_le_space
    )
{
    void* maybe_free = nullptr;
    uint32_t size_alloc = new_size + keylen + sizeof(keylen);
    KLPAIR new_kl = mempool_malloc_from_omt(
        size_alloc,
        &maybe_free
        );
    uint32_t size_freed = old_le_size + keylen + sizeof(keylen);
    toku_mempool_mfree(&m_buffer_mempool, nullptr, size_freed);  // Must pass nullptr, since le is no good any more.
    new_kl->keylen = keylen;
    memcpy(new_kl->key_le, keyp, keylen);
    m_buffer.set_at(new_kl, idx);
    *new_le_space = get_le_from_klpair(new_kl);
    // free at end, so that the keyp and keylen
    // passed in is still valid
    if (maybe_free) {
        toku_free(maybe_free);
    }
}

//TODO: probably not free the "maybe_free" right away?
void bn_data::get_space_for_insert(
    uint32_t idx,
    const void* keyp,
    uint32_t keylen,
    size_t size,
    LEAFENTRY* new_le_space
    )
{
    void* maybe_free = nullptr;
    uint32_t size_alloc = size + keylen + sizeof(keylen);
    KLPAIR new_kl = mempool_malloc_from_omt(
        size_alloc,
        &maybe_free
        );
    new_kl->keylen = keylen;
    memcpy(new_kl->key_le, keyp, keylen);
    m_buffer.insert_at(new_kl, idx);
    *new_le_space = get_le_from_klpair(new_kl);
    // free at end, so that the keyp and keylen
    // passed in is still valid (you never know if
    // it was part of the old mempool, this is just
    // safer).
    if (maybe_free) {
        toku_free(maybe_free);
    }
}

#include "ft-ops.h"

void
bn_data::relift_leafentries(FT ft, DBT *old_lift, DBT *new_lift,
                            uint32_t lbi, uint32_t ube)
{
    // clone to old_bn and destroy this bn
    bn_data old_bn;
    old_bn.clone(this);
    this->destroy();

    assert(lbi <= ube);
    uint32_t size = ube - lbi;
    
    KLPAIR *newklpts = NULL;
    // KLPAIR *XMALLOC_N(size, newklpts);
    if (size) {
        newklpts = (KLPAIR *) sb_malloc_sized(size * sizeof(KLPAIR), true);
    }
    size_t mpsize = toku_mempool_get_used_space(&old_bn.m_buffer_mempool);
    struct mempool *src_mp = &old_bn.m_buffer_mempool;
    struct mempool *dst_mp = &this->m_buffer_mempool;
    toku_mempool_construct(dst_mp, mpsize);

    for (uint32_t i = lbi; i < ube; i++) {
        KLPAIR curr_kl;
        old_bn.m_buffer.fetch(i, &curr_kl);
        int old_keylen = curr_kl->keylen;
        void *old_keyp = curr_kl->key_le;
        LEAFENTRY old_le = get_le_from_klpair(curr_kl);
        int old_le_size = leafentry_memsize(old_le);

        DBT old_key, unlifted_key, new_key;
        toku_fill_dbt(&old_key, old_keyp, old_keylen);
        toku_init_dbt(&unlifted_key);
        int r;
        if (old_lift->size != 0 && new_lift->size != 0) {
            r = toku_ft_unlift_key(ft, &unlifted_key, &old_key, old_lift);
            assert_zero(r);
            r = toku_ft_lift_key_no_alloc(ft, &new_key, &unlifted_key, new_lift);
            assert_zero(r);
        } else if (old_lift->size != 0) {
            r = toku_ft_unlift_key(ft, &unlifted_key, &old_key, old_lift);
            assert_zero(r);
            toku_copy_dbt(&new_key, unlifted_key);
        } else if (new_lift->size != 0) {
            r = toku_ft_lift_key_no_alloc(ft, &new_key, &old_key, new_lift);
            assert_zero(r);
        } else {
            toku_copy_dbt(&new_key, old_key);
        }

        size_t old_kl_size = old_keylen + sizeof(*curr_kl) + old_le_size;
        size_t new_kl_size = new_key.size + sizeof(*curr_kl) + old_le_size;
        KLPAIR new_kl = NULL;
        CAST_FROM_VOIDP(new_kl, toku_mempool_malloc(dst_mp, new_kl_size, 1));
        new_kl->keylen = new_key.size;
        memcpy(new_kl->key_le, new_key.data, new_key.size);
        memcpy((char *)new_kl->key_le + new_key.size, old_le, old_le_size);

        newklpts[i - lbi] = new_kl;
        toku_mempool_mfree(src_mp, curr_kl, old_kl_size);
        toku_cleanup_dbt(&unlifted_key);
    }

    m_buffer.create_steal_sorted_array(&newklpts, size, size);

    // destroy old_bn
    old_bn.destroy();
}

void bn_data::move_leafentries_to(
     BN_DATA dest_bd,
     uint32_t lbi, //lower bound inclusive
     uint32_t ube //upper bound exclusive
     )
//Effect: move leafentries in the range [lbi, ube) from this to src_omt to newly created dest_omt
{
    paranoid_invariant(lbi < ube);
    paranoid_invariant(ube <= omt_size());
    size_t bytes = ((ube-lbi) * sizeof(KLPAIR));
    KLPAIR *newklpointers = (KLPAIR *)sb_malloc_sized(bytes, true); // create new omt

    size_t mpsize = toku_mempool_get_used_space(&m_buffer_mempool);   // overkill, but safe
    struct mempool *dest_mp = &dest_bd->m_buffer_mempool;
    struct mempool *src_mp  = &m_buffer_mempool;
    toku_mempool_construct(dest_mp, mpsize);

    uint32_t i = 0;
    for (i = lbi; i < ube; i++) {
        KLPAIR curr_kl;
        m_buffer.fetch(i, &curr_kl);

        size_t kl_size = klpair_size(curr_kl);
        KLPAIR new_kl = NULL;
        CAST_FROM_VOIDP(new_kl, toku_mempool_malloc(dest_mp, kl_size, 1));
        memcpy(new_kl, curr_kl, kl_size);
        newklpointers[i-lbi] = new_kl;
        toku_mempool_mfree(src_mp, curr_kl, kl_size);
    }

    dest_bd->m_buffer.create_steal_sorted_array(&newklpointers, ube-lbi, ube-lbi);
    // now remove the elements from src_omt
    for (i=ube-1; i >= lbi; i--) {
        m_buffer.delete_at(i);
    }
}

uint64_t bn_data::get_disk_size() {
    return toku_mempool_get_used_space(&m_buffer_mempool);
}

void bn_data::verify_mempool(void) {
    // TODO: implement something
}

uint32_t bn_data::omt_size(void) const {
    return m_buffer.size();
}

void bn_data::destroy(void) {
    // The buffer may have been freed already, in some cases.
    m_buffer.destroy();
    toku_mempool_destroy(&m_buffer_mempool);
}

//TODO: Splitting key/val requires changing this
void bn_data::replace_contents_with_clone_of_sorted_array(
    uint32_t num_les,
    const void** old_key_ptrs,
    uint32_t* old_keylens,
    LEAFENTRY* old_les,
    size_t *le_sizes,
    size_t mempool_size
    )
{
    if (num_les) {
        toku_mempool_construct(&m_buffer_mempool, mempool_size);
        KLPAIR *XMALLOC_N(num_les, le_array);
        for (uint32_t idx = 0; idx < num_les; idx++) {
            KLPAIR new_kl = (KLPAIR)toku_mempool_malloc(
                                                        &m_buffer_mempool,
                                                        le_sizes[idx] + old_keylens[idx] + sizeof(uint32_t),
                                                        1); // point to new location
            new_kl->keylen = old_keylens[idx];
            memcpy(new_kl->key_le, old_key_ptrs[idx], new_kl->keylen);
            memcpy(get_le_from_klpair(new_kl), old_les[idx], le_sizes[idx]);
            CAST_FROM_VOIDP(le_array[idx], new_kl);
        }
        //TODO: Splitting key/val requires changing this; keys are stored in old omt.. cannot delete it yet?
        m_buffer.destroy();
        m_buffer.create_steal_sorted_array(&le_array, num_les, num_les);
    } else {
        m_buffer.destroy();
    }
}


// get info about a single leafentry by index
int bn_data::fetch_le(uint32_t idx, LEAFENTRY *le) {
    KLPAIR klpair = NULL;
    int r = m_buffer.fetch(idx, &klpair);
    if (r == 0) {
        *le = get_le_from_klpair(klpair);
    }
    return r;
}

int bn_data::fetch_klpair(uint32_t idx, LEAFENTRY *le, uint32_t *len, void** key) {
    KLPAIR klpair = NULL;
    int r = m_buffer.fetch(idx, &klpair);
    if (r == 0) {
        *len = klpair->keylen;
        *key = klpair->key_le;
        *le = get_le_from_klpair(klpair);
    }
    return r;
}

int bn_data::fetch_klpair_disksize(uint32_t idx, size_t *size) {
    KLPAIR klpair = NULL;
    int r = m_buffer.fetch(idx, &klpair);
    if (r == 0) {
        *size = klpair_disksize(klpair);
    }
    return r;
}

int bn_data::fetch_le_key_and_len(uint32_t idx, uint32_t *len, void** key) {
    KLPAIR klpair = NULL;
    int r = m_buffer.fetch(idx, &klpair);
    if (r == 0) {
        *len = klpair->keylen;
        *key = klpair->key_le;
    }
    return r;
}


struct mp_pair {
    void* orig_base;
    void* new_base;
    klpair_omt_t* omt;
};

static int fix_mp_offset(const KLPAIR &klpair, const uint32_t idx,  struct mp_pair * const p) {
    char* old_value = (char *) klpair;
    char *new_value = old_value - (char *)p->orig_base + (char *)p->new_base;
    p->omt->set_at((KLPAIR)new_value, idx);
    return 0;
}

void bn_data::clone(bn_data* orig_bn_data) {
    toku_mempool_clone(&orig_bn_data->m_buffer_mempool, &m_buffer_mempool);
    m_buffer.clone(orig_bn_data->m_buffer);
    struct mp_pair p;
    p.orig_base = toku_mempool_get_base(&orig_bn_data->m_buffer_mempool);
    p.new_base = toku_mempool_get_base(&m_buffer_mempool);
    p.omt = &m_buffer;

    int r = m_buffer.iterate_on_range<decltype(p), fix_mp_offset>(0, omt_size(), &p);
    invariant_zero(r);
}
