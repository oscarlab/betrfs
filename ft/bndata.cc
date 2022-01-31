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
#include "ule-internal.h"

static uint32_t klpair_size(KLPAIR klpair){
    return sizeof(*klpair) + klpair->keylen + leafentry_memsize(get_le_from_klpair(klpair));
}

static uint32_t klpair_disksize(KLPAIR klpair){
    LEAFENTRY le = get_le_from_klpair(klpair);
    uint32_t retval = sizeof(*klpair) + klpair->keylen + leafentry_disksize(le);
#ifdef FT_INDIRECT
    retval += le_get_ind_size(le);
    retval -= sizeof(unsigned int*);
#endif
    return retval;
}

void bn_data::init_zero() {
    toku_mempool_zero(&m_buffer_mempool);
}

void bn_data::initialize_empty() {
    toku_mempool_zero(&m_buffer_mempool);
    m_buffer.create();
}

#ifdef FT_INDIRECT
/**
 * When we read k-v pairs from disk. The PFNs are not valid anymore.
 * We need to change the pfns in the values according to the pages we
 * just allocated to store the data just read in to memory
 *
 * full_pfns is the pfn array of the pages which store the page data of
 * the basement node. le_start is the address of the leaf entry.
 * The function updates the pfns in leafentry values with the pfns in pfn_arr.
 */
static void fixup_read_val(unsigned int *offset, uint8_t *le_start, uint8_t ubi_cnt,
                           unsigned long *pfns, uint32_t *pfns_index,
                           uint32_t *odd_data_size_so_far,
                           uint32_t *odd_page_cnt_so_far)
{
    LEAFENTRY le = (LEAFENTRY) le_start;
    struct ftfs_indirect_val *ptr;
    for (int i = 0 ; ubi_cnt > 0 && i < LE_NUM_VALS(le); i++) {
        if (offset[i] != 0) {
            ptr = IND_VAL_FROM_PTR(le_start + offset[i]);
            uint32_t size = ptr->size;
            assert(size <= FTFS_PAGE_SIZE);
            if (size < FTFS_PAGE_SIZE) {
                *odd_data_size_so_far += size;
                *odd_page_cnt_so_far += 1;
            }
            /* copy the pfn to the leaf entry */
            ptr->pfn = pfns[*pfns_index];
            ftfs_bn_get_page_list(&ptr->pfn, 1);
            *pfns_index += 1;
            ubi_cnt -= 1;
        }
    }
}
#endif

void bn_data::initialize_from_data(uint32_t num_entries,
                                   unsigned char *buf,
                                   uint32_t data_size,
                                   unsigned long *UU(pfns),
                                   uint32_t UU(num_pages),
                                   uint32_t *UU(odd_data_size),
                                   uint32_t *UU(odd_page_cnt))
{
    if (data_size == 0) {
        invariant_zero(num_entries);
        // I believe we can assume the m_buffer is already created here
        return;
    }
    KLPAIR *XMALLOC_N(num_entries, array); // create array of pointers to leafentries
    unsigned char *newmem = NULL;
    // add same wiggle room that toku_mempool_construct would, 25% extra
#ifdef FT_INDIRECT
    int size_tmp = sizeof(unsigned int*); // space for indirect_insert_offsets pointer
    data_size += size_tmp * num_entries;
#endif
    uint32_t allocated_bytes = data_size + data_size/4;
    CAST_FROM_VOIDP(newmem, sb_malloc_sized(allocated_bytes, true));
    unsigned char* curr_src_pos = buf;
    unsigned char* curr_dest_pos = newmem;
#ifdef FT_INDIRECT
    uint32_t pfns_index = 0;
    uint32_t odd_data_size_so_far = 0;
    uint32_t odd_page_cnt_so_far = 0;
    uint32_t cnt_ind_inserts = 0;
#endif

    for (uint32_t i = 0; i < num_entries; i++) {
        KLPAIR curr_kl = (KLPAIR)curr_dest_pos;
        array[i] = curr_kl;

#ifdef FT_INDIRECT
        // Store the start address of a leaf entry
        uint8_t* src_le_start = curr_src_pos;
#endif
        uint8_t curr_type = curr_src_pos[0];
        curr_src_pos++;
#ifdef FT_INDIRECT
        // uint8_t le_count_ubi_vals = curr_src_pos[0];
        // curr_src_pos++;
        uint32_t le_count_ubi_vals = *(uint32_t *)curr_src_pos;
        curr_src_pos += sizeof(uint32_t);
#endif
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

#ifdef FT_INDIRECT
	// Zero out ubi_val_offsets
        uint8_t *dest_le_start = curr_dest_pos;
        unsigned int **ubi_val_offsets = (unsigned int**)curr_dest_pos;
        *ubi_val_offsets = NULL;
        curr_dest_pos += size_tmp;
        // now curr_dest_pos is pointing to where the leafentry should be packed
        cnt_ind_inserts += le_count_ubi_vals;
        //curr_dest_pos[0] = le_count_ubi_vals;
        *(uint32_t *)curr_dest_pos = le_count_ubi_vals;
        curr_dest_pos += sizeof(uint32_t);
#endif
        curr_dest_pos[0] = curr_type;
        curr_dest_pos++;
        if (curr_type == LE_CLEAN) {
             *(uint32_t *)curr_dest_pos = toku_htod32(clean_vallen);
             curr_dest_pos += sizeof(clean_vallen);
             memcpy(curr_dest_pos, curr_src_pos, clean_vallen); // copy the val
#ifdef FT_INDIRECT
             if (le_count_ubi_vals) {
                 XCALLOC_N(1, *ubi_val_offsets);
                 (*ubi_val_offsets)[0] = (curr_dest_pos - dest_le_start);
                 struct ftfs_indirect_val *val = (struct ftfs_indirect_val *)(curr_dest_pos);
                 val->pfn = pfns[pfns_index];
                 ftfs_bn_get_page_list(&pfns[pfns_index], 1);
                 if (val->size < FTFS_PAGE_SIZE) {
                     odd_data_size_so_far += val->size;
                     odd_page_cnt_so_far++;
                 } else {
                     assert(val->size == FTFS_PAGE_SIZE);
                 }
                 pfns_index++;
             }
#endif
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
#ifdef FT_INDIRECT
            if (le_count_ubi_vals) {
                if (le_count_ubi_vals > num_cxrs + num_pxrs) {
                    printf("le_count_ubi_vals=%d, num_cxrs=%d, num_pxrs=%d\n", le_count_ubi_vals, num_cxrs, num_pxrs);
                    assert(false);
                }
                XCALLOC_N(num_cxrs + num_pxrs, *ubi_val_offsets);
            }
            uint32_t num_rest_bytes = leafentry_rest_memsize_fixup(num_pxrs,
                                                                   num_cxrs,
                                                                   curr_src_pos,
                                                                   *ubi_val_offsets,
                                                                   src_le_start,
                                                                   size_tmp - (keylen) - sizeof(keylen));
            assert(num_rest_bytes < data_size);
            memcpy(curr_dest_pos, curr_src_pos, num_rest_bytes);
            if (le_count_ubi_vals) {
                fixup_read_val(*ubi_val_offsets, dest_le_start, le_count_ubi_vals,
                                pfns, &pfns_index, &odd_data_size_so_far, &odd_page_cnt_so_far);

            }
#else
            uint32_t num_rest_bytes = leafentry_rest_memsize(num_pxrs, num_cxrs, curr_src_pos);
            memcpy(curr_dest_pos, curr_src_pos, num_rest_bytes);
#endif
            curr_dest_pos += num_rest_bytes;
            curr_src_pos += num_rest_bytes;
        }
    }

#ifdef FT_INDIRECT
    if (pfns_index != num_pages) {
        printf("%s: pfns_index=%d, num_pages=%d\n", __func__, pfns_index, num_pages);
        assert(false);
    }
    if (odd_data_size) {
        *odd_data_size = odd_data_size_so_far;
    }
    if (odd_page_cnt) {
        *odd_page_cnt = odd_page_cnt_so_far;
    }
#endif

    uint32_t num_bytes_read UU() = (uint32_t)(curr_src_pos - buf);
#ifdef FT_INDIRECT
    paranoid_invariant(num_bytes_read + size_tmp * num_entries == data_size);
#else
    paranoid_invariant( num_bytes_read == data_size);
#endif
    uint32_t num_bytes_written = curr_dest_pos - newmem;
    paranoid_invariant(num_bytes_written == data_size);
    toku_mempool_init(&m_buffer_mempool, newmem, (size_t)(num_bytes_written), allocated_bytes);

    // destroy old omt that was created by toku_create_empty_bn(), so we can create a new one
    m_buffer.destroy();
    m_buffer.create_steal_sorted_array(&array, num_entries, num_entries);
}

#ifdef FT_INDIRECT
bool bn_data::is_data_db(int fd) {
     return sb_is_data_db(fd);
}

#endif
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
#ifdef FT_INDIRECT
    KLPAIR curr_kl = nullptr;
    m_buffer.fetch(idx, &curr_kl);
    LEAFENTRY old_le = get_le_from_klpair(curr_kl);
    if (old_le->indirect_insert_offsets) {
        toku_free(old_le->indirect_insert_offsets);
    }
#endif
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
#ifdef FT_INDIRECT
    KLPAIR curr_kl;
    m_buffer.fetch(idx, &curr_kl);
    LEAFENTRY old_le = get_le_from_klpair(curr_kl);
    if (old_le->num_indirect_inserts > 0) {
        toku_free(old_le->indirect_insert_offsets);
    }
#endif
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

void bn_data::relift_leafentries(BN_DATA dst_bd, FT ft,
                                 DBT *old_lifted, DBT *new_lifted)
{
    int32_t size = omt_size();
    KLPAIR *newklpointers = NULL;
    if (size) {
        newklpointers = (KLPAIR *) sb_malloc_sized(size * sizeof(KLPAIR), true);
    }

    // The whole process is to unlift the key with
    // old_lifted and then lift it again with new_lifted
    int per_item_increase = old_lifted->size - new_lifted->size;
    // If the increase is negative, then we just set it to zero
    if (per_item_increase < 0) per_item_increase = 0;

    size_t mpsize = toku_mempool_get_used_space(&m_buffer_mempool);
    struct mempool *dst_mp = &dst_bd->m_buffer_mempool;
    struct mempool *src_mp = &m_buffer_mempool;
    // make the new mempool 25% larger than the old one.
    // I find that the assertion at line 539 can be triggered occasionally.
    toku_mempool_construct(dst_mp, mpsize + (mpsize >> 2) + per_item_increase * size);
    for (int i = 0; i < size; i++) {
        KLPAIR curr_kl = nullptr;
        m_buffer.fetch(i, &curr_kl);

        int old_keylen = curr_kl->keylen;
        void *old_keyp = curr_kl->key_le;
        LEAFENTRY old_le = get_le_from_klpair(curr_kl);
        int old_le_size = leafentry_memsize(old_le);

        DBT old_key, unlifted_key, new_key;
        toku_fill_dbt(&old_key, old_keyp, old_keylen);
        toku_init_dbt(&unlifted_key);
        int r;
        if (old_lifted->size != 0 && new_lifted->size != 0) {
            r = toku_ft_unlift_key(ft, &unlifted_key, &old_key, old_lifted);
            assert_zero(r);
            r = toku_ft_lift_key_no_alloc(ft, &new_key, &unlifted_key, new_lifted);
            assert_zero(r);
        } else if (old_lifted->size == 0) {
            assert(new_lifted->size != 0);
            r = toku_ft_lift_key_no_alloc(ft, &new_key, &old_key, new_lifted);
            assert_zero(r);
        } else {
            r = toku_ft_unlift_key(ft, &unlifted_key, &old_key, old_lifted);
            assert_zero(r);
            toku_copy_dbt(&new_key, unlifted_key);
        }
        size_t old_kl_size = old_keylen + sizeof(*curr_kl) + old_le_size;
        size_t new_kl_size = new_key.size + sizeof(*curr_kl) + old_le_size;
        KLPAIR new_kl = NULL;
        CAST_FROM_VOIDP(new_kl, toku_mempool_malloc(dst_mp, new_kl_size, 1));
        if (!new_kl) {
            printf("%s: old_lifted=%s\n", __func__, (char*)old_lifted->data);
            printf("%s: new_lifted=%s\n", __func__, (char*)new_lifted->data);
            assert(false);
        }
        new_kl->keylen = new_key.size;
        assert(new_key.data);
        memcpy(new_kl->key_le, new_key.data, new_key.size);
        memcpy((char *)new_kl->key_le + new_key.size, old_le, old_le_size);
        newklpointers[i] = new_kl;
        toku_mempool_mfree(src_mp, curr_kl, old_kl_size);
        toku_destroy_dbt(&old_key);
        toku_cleanup_dbt(&unlifted_key);
    }

    dst_bd->m_buffer.create_steal_sorted_array(&newklpointers, size, size);
    for (int i = size - 1; i >= 0; i--) {
        m_buffer.delete_at((uint32_t)i);
    }
}

// YZJ: count indirect insert when moving leafentry.
// Iterate through all the leafentry in the range [lbi, ube)
// and check the count and size of indirect insert in each leaf entry
// aggregate the count and size in these 4 num_indirect_xxx
// arguments as the output of this function. For non-page-sharing code
// these arguments are not used at all.
//Effect: move leafentries in the range [lbi, ube) from this to src_omt to newly created dest_omt
void bn_data::move_leafentries_to(
     BN_DATA dest_bd,
     uint32_t *UU(num_indirect_full_count),
     uint32_t *UU(num_indirect_full_size),
     uint32_t *UU(num_indirect_odd_count),
     uint32_t *UU(num_indirect_odd_size),
     uint32_t lbi, //lower bound inclusive
     uint32_t ube //upper bound exclusive
     )
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

#ifdef FT_INDIRECT
	// Copy ubi_val_offsets and count pages
	LEAFENTRY le = get_le_from_klpair(curr_kl);
        LEAFENTRY new_le = get_le_from_klpair((KLPAIR)new_kl);
	int num_ubi = le->num_indirect_inserts;
        for (int ii = 0 ; num_ubi > 0 && ii < LE_NUM_VALS(le); ii++) {
            if (le->indirect_insert_offsets[ii] > 0) {
                uint32_t size;
                memcpy(&size, (uint8_t*)le + le->indirect_insert_offsets[ii]+8, 4);
                if (size == FTFS_PAGE_SIZE) {
                    (*num_indirect_full_count)++;
                    (*num_indirect_full_size) += size;
                } else {
                    (*num_indirect_odd_count)++;
                    (*num_indirect_odd_size) += size;
                }
                num_ubi--;
            }
        }
        if (le->indirect_insert_offsets) {
            new_le->indirect_insert_offsets = le->indirect_insert_offsets;
        }
#endif
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
    size_t mempool_size,
    int UU(fd)
    )
{
#ifdef FT_INDIRECT
    // YZJ: Reset the variable to 0
    m_rebalance_indirect_full_size = 0;
    m_rebalance_indirect_odd_size = 0;
    m_rebalance_indirect_full_cnt = 0;
    m_rebalance_indirect_odd_cnt = 0;
#endif

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
            // If it is data db. This betrfs specific node.
            // For non-page-sharing code, it is no-ops.
            // for non-data-db, it is no-ops with extra check
            if (fd >= 0) {
                update_indirect_cnt(old_les[idx], fd);
            }
        }
        //TODO: Splitting key/val requires changing this; keys are stored in old omt.. cannot delete it yet?
        m_buffer.destroy();
        m_buffer.create_steal_sorted_array(&le_array, num_les, num_les);
    } else {
        m_buffer.destroy();
    }
}

#ifdef FT_INDIRECT
uint32_t bn_data::update_indirect_cnt(LEAFENTRY le, int fd)
{
    int num_ubi = le->num_indirect_inserts;

    if (!is_data_db(fd) && num_ubi > 0) { // should not happen
        printf("Only datadb can have indirect values, fd=%d\n", fd);
        assert(false);
    }

    for (int i = 0 ; num_ubi > 0 && i < LE_NUM_VALS(le); i++) {
        unsigned long curr_offset = le->indirect_insert_offsets[i];
        uint8_t *val_ptr = (uint8_t*)le + curr_offset;
        if (curr_offset > 0) {
            unsigned int size;
            memcpy(&size, val_ptr+8, sizeof(size));
            if (size < FTFS_PAGE_SIZE) {
                m_rebalance_indirect_odd_size += size;
                m_rebalance_indirect_odd_cnt += 1;
            } else {
                m_rebalance_indirect_full_size += size;
                m_rebalance_indirect_full_cnt += 1;
            }
            num_ubi--;
        }
    }
    return 0;
}
#else
inline uint32_t bn_data::update_indirect_cnt(LEAFENTRY UU(le), int UU(fd)) {
    return 0;
}
#endif

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

// Cloning a bn is to get snapshot of current state of a bn.
// We need to clone the indirect_insert_offsets as well.
// FIXME: allocate space from mempool instead of OS for all
// indirect_insert_offsets. This can reduce locking contention.
static int fix_mp_offset_and_page_ref(const KLPAIR &klpair, const uint32_t idx, struct mp_pair * const p) {
    char* old_value = (char *) klpair;
    char *new_value = old_value - (char *)p->orig_base + (char *)p->new_base;
    p->omt->set_at((KLPAIR)new_value, idx);
#ifdef FT_INDIRECT
    {
        LEAFENTRY le = get_le_from_klpair(klpair);
        LEAFENTRY new_le = get_le_from_klpair((KLPAIR)new_value);

        int num_ubi = le->num_indirect_inserts;
        assert(num_ubi == new_le->num_indirect_inserts);
        unsigned int *old_offsets = le->indirect_insert_offsets;
        if (num_ubi > 0) {
            XMALLOC_N(LE_NUM_VALS(le), new_le->indirect_insert_offsets);
            memcpy(new_le->indirect_insert_offsets, old_offsets, sizeof(unsigned int)*LE_NUM_VALS(le));
        }
        for (int i = 0 ; num_ubi > 0 && i < LE_NUM_VALS(le); i++) {
            if (new_le->indirect_insert_offsets[i] > 0) {
                struct ftfs_indirect_val *val = (struct ftfs_indirect_val *) ((uint8_t*)le + new_le->indirect_insert_offsets[i]);
                unsigned long pfn = val->pfn;
                ftfs_lock_page_list_for_clone(&pfn, 1);
                ftfs_set_page_list_private(&pfn, 1, FT_MSG_VAL_FOR_CLONE);
                ftfs_bn_get_page_list(&pfn, 1);
                num_ubi--;
            }
        }
    }
#endif
    return 0;
}

void bn_data::clone(bn_data* orig_bn_data) {
    toku_mempool_clone(&orig_bn_data->m_buffer_mempool, &m_buffer_mempool);
    m_buffer.clone(orig_bn_data->m_buffer);
    struct mp_pair p;
    p.orig_base = toku_mempool_get_base(&orig_bn_data->m_buffer_mempool);
    p.new_base = toku_mempool_get_base(&m_buffer_mempool);
    p.omt = &m_buffer;
    int r = m_buffer.iterate_on_range<decltype(p), fix_mp_offset_and_page_ref>(0, omt_size(), &p);
    invariant_zero(r);
}

static int fix_mp_offset(const KLPAIR &klpair, const uint32_t idx,  struct mp_pair * const p) {
    char* old_value = (char *) klpair;
    char *new_value = old_value - (char *)p->orig_base + (char *)p->new_base;
    p->omt->set_at((KLPAIR)new_value, idx);
    return 0;
}

void bn_data::clone_for_lift(bn_data* orig_bn_data) {
    toku_mempool_clone(&orig_bn_data->m_buffer_mempool, &m_buffer_mempool);
    m_buffer.clone(orig_bn_data->m_buffer);
    struct mp_pair p;
    p.orig_base = toku_mempool_get_base(&orig_bn_data->m_buffer_mempool);
    p.new_base = toku_mempool_get_base(&m_buffer_mempool);
    p.omt = &m_buffer;
    int r = m_buffer.iterate_on_range<decltype(p), fix_mp_offset>(0, omt_size(), &p);
    invariant_zero(r);
}
