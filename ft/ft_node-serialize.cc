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

#include "ft-internal.h"
#include "log-internal.h"
#include <compress.h>
#include <portability/toku_atomic.h>
#include <util/sort.h>
#include <util/threadpool.h>
#include "ft.h"
#include <util/status.h>
#include "ule-internal.h"


static FT_UPGRADE_STATUS_S ft_upgrade_status;

#define STATUS_INIT(k,c,t,l,inc) TOKUDB_STATUS_INIT(ft_upgrade_status, k, c, t, "brt upgrade: " l, inc)

static void
status_init(void)
{
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.
    STATUS_INIT(FT_UPGRADE_FOOTPRINT,             nullptr, UINT64, "footprint", TOKU_ENGINE_STATUS);
    ft_upgrade_status.initialized = true;
}
#undef STATUS_INIT

#define UPGRADE_STATUS_VALUE(x) ft_upgrade_status.status[x].value.num

void
toku_ft_upgrade_get_status(FT_UPGRADE_STATUS s) {
    if (!ft_upgrade_status.initialized) {
        status_init();
    }
    UPGRADE_STATUS_VALUE(FT_UPGRADE_FOOTPRINT) = toku_log_upgrade_get_footprint();
    *s = ft_upgrade_status;
}

static int num_cores = 0; // cache the number of cores for the parallelization
static struct toku_thread_pool *ft_pool = NULL;

int get_num_cores(void) {
    return num_cores;
}

struct toku_thread_pool *get_ft_pool(void) {
    return ft_pool;
}

void
toku_ft_serialize_layer_init(void) {
    num_cores = toku_os_get_number_active_processors();
    int r = toku_thread_pool_create(&ft_pool, num_cores); lazy_assert_zero(r);
}

void
toku_ft_serialize_layer_destroy(void) {
    toku_thread_pool_destroy(&ft_pool);
}

enum {FILE_CHANGE_INCREMENT = (16<<20)};

static inline uint64_t
alignup64(uint64_t a, uint64_t b) {
    return ((a+b-1)/b)*b;
}

// safe_file_size_lock must be held.
void
toku_maybe_truncate_file (int fd, uint64_t size_used, uint64_t expected_size, uint64_t *new_sizep)
// Effect: If file size >= SIZE+32MiB, reduce file size.
// (32 instead of 16.. hysteresis).
// Return 0 on success, otherwise an error number.
{
    int64_t file_size;
    {
        int r = toku_os_get_file_size(fd, &file_size);
        lazy_assert_zero(r);
        invariant(file_size >= 0);
    }
    invariant(expected_size == (uint64_t)file_size);
    // If file space is overallocated by at least 32M
    if ((uint64_t)file_size >= size_used + (2*FILE_CHANGE_INCREMENT)) {
        toku_off_t new_size = alignup64(size_used, (2*FILE_CHANGE_INCREMENT)); //Truncate to new size_used.
        invariant(new_size < file_size);
        invariant(new_size >= 0);
        *new_sizep = new_size;
    }
    else {
        *new_sizep = file_size;
    }
    return;
}

static int64_t
min64(int64_t a, int64_t b) {
    if (a<b) return a;
    return b;
}

void
toku_maybe_preallocate_in_file (int fd, int64_t size, int64_t expected_size, int64_t *new_size)
// Effect: make the file bigger by either doubling it or growing by 16MiB whichever is less, until it is at least size
// Return 0 on success, otherwise an error number.
{
    int64_t file_size;
    //TODO(yoni): Allow variable stripe_width (perhaps from ft) for larger raids
    const uint64_t stripe_width = 4096;
    {
        int r = toku_os_get_file_size(fd, &file_size);
        lazy_assert_zero(r);
    }
    invariant(file_size >= 0);
    invariant(expected_size == file_size);
    // We want to double the size of the file, or add 16MiB, whichever is less.
    // We emulate calling this function repeatedly until it satisfies the request.
    int64_t to_write = 0;
    if (file_size == 0) {
        // Prevent infinite loop by starting with stripe_width as a base case.
        to_write = stripe_width;
    }
    while (file_size + to_write < size) {
        to_write += alignup64(min64(file_size + to_write, FILE_CHANGE_INCREMENT), stripe_width);
    }
    if (to_write > 0) {
        assert(to_write%BLOCK_ALIGNMENT==0);

        toku_off_t start_write = alignup64(file_size, stripe_width);
        invariant(start_write >= file_size);
        // YZJ: This should not happen for SFS
        assert(false);
	*new_size = start_write + to_write;
    }
    else {
        *new_size = file_size;
    }
}

// Don't include the sub_block header
// Overhead calculated in same order fields are written to wbuf
enum {
    node_header_overhead = (8+   // magic "tokunode" or "tokuleaf" or "tokuroll"
                            4+   // layout_version
                            4+   // layout_version_original
                            4),  // build_id
};

#include "sub_block.h"
#include "sub_block_map.h"

// uncompressed header offsets
enum {
    uncompressed_magic_offset = 0,
    uncompressed_version_offset = 8,
};

uint32_t serialize_node_header_size(FTNODE node) {
    uint32_t retval = 0;
    retval += 8; // magic
    retval += sizeof(node->layout_version);
    retval += sizeof(node->layout_version_original);
    retval += 4; // BUILD_ID
    retval += 4; // n_children
    retval += node->n_children*8; // encode start offset and length of each partition
#ifdef FT_INDIRECT
    // page data size and offset and etc
    retval += serialize_node_header_increased_size_with_indirect(node);
#endif
    retval += 4; // checksum
    return retval;
}

void serialize_node_header(FTNODE node, FTNODE_DISK_DATA ndd, struct wbuf *wbuf) {
    if (node->height == 0)
        wbuf_nocrc_literal_bytes(wbuf, "tokuleaf", 8);
    else
        wbuf_nocrc_literal_bytes(wbuf, "tokunode", 8);
    paranoid_invariant(node->layout_version == FT_LAYOUT_VERSION);
    wbuf_nocrc_int(wbuf, node->layout_version);
    wbuf_nocrc_int(wbuf, node->layout_version_original);
    wbuf_nocrc_uint(wbuf, BUILD_ID);
    wbuf_nocrc_int (wbuf, node->n_children);
    for (int i=0; i<node->n_children; i++) {
        assert(BP_SIZE(ndd,i)>0);
        wbuf_nocrc_int(wbuf, BP_START(ndd, i)); // save the beginning of the partition
        wbuf_nocrc_int(wbuf, BP_SIZE (ndd, i));         // and the size
    }
#ifdef FT_INDIRECT
    serialize_node_header_page_data(node, ndd, wbuf);
    wbuf_nocrc_uint32_t(wbuf, node->node_info_size);
#endif
    // checksum the header
    uint32_t end_to_end_checksum = x1764_memory(wbuf->buf, wbuf_get_woffset(wbuf));
    wbuf_nocrc_int(wbuf, end_to_end_checksum);
    invariant(wbuf->ndone == wbuf->size);
}

int wbufwriteleafentry(const void* key, const uint32_t keylen, const LEAFENTRY &le,
                       const uint32_t UU(idx), struct wbuf * const wb) {
    // need to pack the leafentry as it was in versions
    // where the key was integrated into it
    uint32_t begin_spot UU() = wb->ndone;
    uint32_t le_disk_size = leafentry_disksize(le);
    wbuf_nocrc_uint8_t(wb, le->type);
#ifdef FT_INDIRECT
    wbuf_nocrc_uint32_t(wb, le->num_indirect_inserts);
#endif
    wbuf_nocrc_uint32_t(wb, keylen);

#ifdef FT_INDIRECT
    if (le->num_indirect_inserts > 0) {
        int num_ind_inserts = le->num_indirect_inserts;
        assert(wb->ind_data);
        for (int i = 0; num_ind_inserts > 0 && i < LE_NUM_VALS(le); i++) {
            unsigned long curr_offset = le->indirect_insert_offsets[i];
            struct ftfs_indirect_val *val_ptr = (struct ftfs_indirect_val *)((uint8_t*)le + curr_offset);
            if (curr_offset > 0) {
                unsigned int size = val_ptr->size;
                unsigned long pfn = val_ptr->pfn;
                assert (size <= FTFS_PAGE_SIZE);
                WB_COPY_PFN(wb, pfn);
                WB_PFN_CNT(wb) += 1;
                num_ind_inserts--;
            }
        }
    }
    /* le->ubi_val_offsets is not written to disk */
    le_disk_size -= sizeof(le->indirect_insert_offsets);
    wb->subtract_bytes += sizeof(le->indirect_insert_offsets);
#endif
    if (le->type == LE_CLEAN) {
        wbuf_nocrc_uint32_t(wb, le->u.clean.vallen);
        wbuf_nocrc_literal_bytes(wb, key, keylen);
        wbuf_nocrc_literal_bytes(wb, le->u.clean.val, le->u.clean.vallen);
    }
    else {
        paranoid_invariant(le->type >= LE_MVCC && le->type < LE_MVCC_END);
        wbuf_nocrc_uint32_t(wb, le->u.mvcc.num_cxrs);
        wbuf_nocrc_uint8_t(wb, le->u.mvcc.num_pxrs);
        wbuf_nocrc_literal_bytes(wb, key, keylen);
#ifdef FT_INDIRECT
        // Do not have ubi_val_offset + type + num_ubi_vals + num_cxrs + num_pxrs
        wbuf_nocrc_literal_bytes(wb, le->u.mvcc.xrs, le_disk_size - (1 + 4 + 4 + 1));
#else
        wbuf_nocrc_literal_bytes(wb, le->u.mvcc.xrs, le_disk_size - (1 + 4 + 1));
#endif
    }
    uint32_t end_spot UU() = wb->ndone;
    paranoid_invariant((end_spot - begin_spot) == keylen + sizeof(keylen) + le_disk_size);
    return 0;
}

uint32_t serialize_ftnode_partition_size(FTNODE node, int i)
{
    uint32_t result = 0;
    paranoid_invariant(node->bp[i].state == PT_AVAIL);
    result++; // Byte that states what the partition is
    if (node->height > 0) {
        result += 4; // size of bytes in buffer table
        result += toku_bnc_nbytesinbuf(BNC(node, i));
    }
    else {
        result += 4; // n_entries in buffer table
        result += BLB_NBYTESINDATA(node, i);
    }
    result += 4; // checksum
    return result;
}

#define FTNODE_PARTITION_OMT_LEAVES 0xaa
#define FTNODE_PARTITION_FIFO_MSG 0xbb
int serialize_msg_fn(FT_MSG msg, bool is_fresh, void* args){
    struct wbuf *wb = (struct wbuf *) args;
#ifdef FT_INDIRECT
    if (ft_msg_get_type(msg) == FT_UNBOUND_INSERT) {
        serialize_ubi_msg(msg, is_fresh, wb);
    }
#endif
    ft_msg_write_to_wbuf(msg, wb, is_fresh);
    return 0;
}

void serialize_nonleaf_childinfo(NONLEAF_CHILDINFO bnc, struct wbuf *wb)
{
    unsigned char ch = FTNODE_PARTITION_FIFO_MSG;
    wbuf_nocrc_char(wb, ch);
    // serialize the FIFO, first the number of entries, then the elements
    wbuf_nocrc_int(wb, toku_bnc_n_entries(bnc));
    toku_fifo_iterate(bnc->buffer, serialize_msg_fn, wb);
}

//
// Serialize the i'th partition of node into sb
// For leaf nodes, this would be the i'th basement node
// For internal nodes, this would be the i'th internal node
//
void serialize_ftnode_partition(FTNODE node, int i, struct sub_block *sb) {
#ifdef FT_INDIRECT
    uint32_t orig_uncomp_size = serialize_ftnode_partition_size_no_pages(node,i);
    struct wbuf wb;
    wbuf_init(&wb, sb->uncompressed_ptr, orig_uncomp_size);
    wb.ind_data = sb->ind_data;
#else
    assert(sb->uncompressed_size == 0);
    assert(sb->uncompressed_ptr == NULL);
    sb->uncompressed_size = serialize_ftnode_partition_size(node,i);
    sb->uncompressed_ptr = sb_malloc_sized(sb->uncompressed_size, true);
    //
    // Now put the data into sb->uncompressed_ptr
    //
    struct wbuf wb;
    wbuf_init(&wb, sb->uncompressed_ptr, sb->uncompressed_size);
#endif
    if (node->height > 0) {
        // TODO: (Zardosht) possibly exit early if there are no messages
        serialize_nonleaf_childinfo(BNC(node, i), &wb);
    }
    else {
        unsigned char ch = FTNODE_PARTITION_OMT_LEAVES;
        BN_DATA bd = BLB_DATA(node, i);

        wbuf_nocrc_char(&wb, ch);
        wbuf_nocrc_uint(&wb, bd->omt_size());
        //
        // iterate over leafentries and place them into the buffer
        //
        bd->omt_iterate<struct wbuf, wbufwriteleafentry>(&wb);
    }
#ifdef FT_INDIRECT
    uint32_t end_to_end_checksum = x1764_memory(sb->uncompressed_ptr, wbuf_get_woffset(&wb));
    wbuf_nocrc_int(&wb, end_to_end_checksum);
    sb->xsum = end_to_end_checksum;
    if (wb.ndone + wb.subtract_bytes != wb.size) {
        printf("wb.ndone + wb.subtract_bytes=%u\n", wb.ndone + wb.subtract_bytes);
        printf("wb.size=%u\n", wb.size);
        printf("wb.subtract_bytes=%u\n", wb.subtract_bytes);
    }
    invariant(wb.ndone + wb.subtract_bytes == wb.size);
    invariant(wb.ndone + wb.subtract_bytes == orig_uncomp_size);
    sb->uncompressed_size = orig_uncomp_size - wb.subtract_bytes;
#else
    uint32_t end_to_end_checksum = x1764_memory(sb->uncompressed_ptr, wbuf_get_woffset(&wb));
    wbuf_nocrc_int(&wb, end_to_end_checksum);
    invariant(wb.ndone == wb.size);
    invariant(sb->uncompressed_size==wb.ndone);
#endif
}

//
// Takes the data in sb->uncompressed_ptr, and compresses it
// into a newly allocated buffer sb->compressed_ptr
//
static void
compress_ftnode_sub_block(struct sub_block *sb, enum toku_compression_method method) {
    assert(sb->compressed_ptr == NULL);
    set_compressed_size_bound(sb, method);
    // add 8 extra bytes, 4 for compressed size,  4 for decompressed size
    sb->compressed_ptr = sb_malloc_sized(sb->compressed_size_bound + 8, true);
    //
    // This probably seems a bit complicated. Here is what is going on.
    // In TokuDB 5.0, sub_blocks were compressed and the compressed data
    // was checksummed. The checksum did NOT include the size of the compressed data
    // and the size of the uncompressed data. The fields of sub_block only reference the
    // compressed data, and it is the responsibility of the user of the sub_block
    // to write the length
    //
    // For Dr. No, we want the checksum to also include the size of the compressed data, and the
    // size of the decompressed data, because this data
    // may be read off of disk alone, so it must be verifiable alone.
    //
    // So, we pass in a buffer to compress_nocrc_sub_block that starts 8 bytes after the beginning
    // of sb->compressed_ptr, so we have space to put in the sizes, and then run the checksum.
    //
    sb->compressed_size = compress_nocrc_sub_block(
        sb,
        (char *)sb->compressed_ptr + 8,
        sb->compressed_size_bound,
        method
        );

    uint32_t* extra = (uint32_t *)(sb->compressed_ptr);
    // store the compressed and uncompressed size at the beginning
    extra[0] = toku_htod32(sb->compressed_size);
    extra[1] = toku_htod32(sb->uncompressed_size);
    // now checksum the entire thing
    sb->compressed_size += 8; // now add the eight bytes that we saved for the sizes
    sb->xsum = x1764_memory(sb->compressed_ptr,sb->compressed_size);

    //
    // This is the end result for Dr. No and forward. For ftnodes, sb->compressed_ptr contains
    // two integers at the beginning, the size and uncompressed size, and then the compressed
    // data. sb->xsum contains the checksum of this entire thing.
    //
    // In TokuDB 5.0, sb->compressed_ptr only contained the compressed data, sb->xsum
    // checksummed only the compressed data, and the checksumming of the sizes were not
    // done here.
    //
}

//
// Returns the size needed to serialize the ftnode info
// Does not include header information that is common with rollback logs
// such as the magic, layout_version, and build_id
// Includes only node specific info such as pivot information, n_children, and so on
//
uint32_t serialize_ftnode_info_size(FTNODE node)
{
    uint32_t retval = 0;
    retval += 8; // max_msn_applied_to_node_on_disk
    retval += 4; // nodesize
    retval += 4; // flags
    retval += 4; // height;
    retval += 8; // oldest_referenced_xid_known
    retval += node->totalchildkeylens; // total length of pivots
    retval += (node->n_children + 1) * 4; // encode length of each pivot and bound
    if (node->height > 0) {
        retval += node->n_children*8; // child blocknum's
        // lift
        for (int i = 0; i < node->n_children; i++) {
            retval += 4 + BP_LIFT(node, i).size;
        }
    }
    retval += 4; // checksum
    return retval;
}

void serialize_ftnode_info(FTNODE node, SUB_BLOCK sb) // output
{
    assert(sb->uncompressed_size == 0);
#ifdef FT_INDIRECT
    sb->uncompressed_size = serialize_ftnode_info_size(node);
#else
    assert(sb->uncompressed_ptr == NULL);
    sb->uncompressed_size = serialize_ftnode_info_size(node);
    sb->uncompressed_ptr = sb_malloc_sized(sb->uncompressed_size, true);
#endif
    struct wbuf wb;
    wbuf_init(&wb, sb->uncompressed_ptr, sb->uncompressed_size);
    wbuf_MSN(&wb, node->max_msn_applied_to_node_on_disk);
    wbuf_nocrc_uint(&wb, 0); // write a dummy value for where node->nodesize used to be
    wbuf_nocrc_uint(&wb, node->flags);
    wbuf_nocrc_int (&wb, node->height);
    wbuf_TXNID(&wb, node->oldest_referenced_xid_known);

    // pivot information
    // int sum=0;
    if (node->bound_l.size == 0) {
        wbuf_nocrc_uint(&wb, 0);
    } else {
        wbuf_nocrc_bytes(&wb, node->bound_l.data, node->bound_l.size);
    }
    if (node->bound_r.size == 0) {
        wbuf_nocrc_uint(&wb, 0);
    } else {
        wbuf_nocrc_bytes(&wb, node->bound_r.data, node->bound_r.size);
    }
    for (int i = 0; i < node->n_children-1; i++) {
	//sum+=node->childkeys[i].size;
        // printf("%s:sum=%d\n",__func__, sum);
        wbuf_nocrc_bytes(&wb, node->childkeys[i].data, node->childkeys[i].size);
    }
    // child blocks, only for internal nodes
    if (node->height > 0) {
        for (int i = 0; i < node->n_children; i++) {
            wbuf_nocrc_BLOCKNUM(&wb, BP_BLOCKNUM(node,i));
            if (BP_LIFT(node, i).size) {
                wbuf_nocrc_bytes(&wb, BP_LIFT(node, i).data, BP_LIFT(node, i).size);
            } else {
                wbuf_nocrc_uint(&wb, 0);
            }
        }
    }

    uint32_t end_to_end_checksum = x1764_memory(sb->uncompressed_ptr, wbuf_get_woffset(&wb));
    wbuf_nocrc_int(&wb, end_to_end_checksum);
    invariant(wb.ndone == wb.size);
    invariant(sb->uncompressed_size==wb.ndone);
}

// This is the size of the uncompressed data, not including the compression headers
unsigned int
toku_serialize_ftnode_size (FTNODE node) {
    unsigned int result = 0;
    //
    // As of now, this seems to be called if and only if the entire node is supposed
    // to be in memory, so we will assert it.
    //
    toku_assert_entire_node_in_memory(node);
    result += serialize_node_header_size(node);
    result += serialize_ftnode_info_size(node);
    for (int i = 0; i < node->n_children; i++) {
        result += serialize_ftnode_partition_size(node,i);
#ifdef FT_INDIRECT
        if (node->height > 0) {
            result += (BNC(node, i)->indirect_insert_odd_size);
            result += (BNC(node, i)->indirect_insert_full_size);
        } else {
            result += (BLB(node, i)->indirect_insert_odd_size);
            result += (BLB(node, i)->indirect_insert_full_size);
            uint32_t num_le = BLB_DATA(node, i)->omt_size();
            result -= (num_le * sizeof(unsigned int*));
        }
#endif
    }
    return result;
}

struct array_info {
    uint32_t offset;
    LEAFENTRY* le_array;
    uint32_t* key_sizes_array;
    const void** key_ptr_array;
};

static int
array_item(const void* key, const uint32_t keylen, const LEAFENTRY &le, const uint32_t idx, struct array_info *const ai) {
    ai->le_array[idx+ai->offset] = le;
    ai->key_sizes_array[idx+ai->offset] = keylen;
    ai->key_ptr_array[idx+ai->offset] = key;
    return 0;
}

// There must still be at least one child
// Requires that all messages in buffers above have been applied.
// Because all messages above have been applied, setting msn of all new basements
// to max msn of existing basements is correct.  (There cannot be any messages in
// buffers above that still need to be applied.)
void
rebalance_ftnode_leaf(FT UU(ft), FTNODE node, unsigned int basementnodesize)
{
    assert(node->height == 0);
    assert(node->dirty);
    int fd = -1;
    if (ft->cf) {
        fd = toku_cachefile_get_fd(ft->cf);
    }
    uint32_t num_orig_basements = node->n_children;
    // Count number of leaf entries in this leaf (num_le).
    uint32_t num_le = 0;
    uint64_t n_unbound_entry = node->unbound_insert_count;
    for (uint32_t i = 0; i < num_orig_basements; i++) {
        num_le += BLB_DATA(node, i)->omt_size();
    }
    uint32_t num_alloc = num_le ? num_le : 1;  // simplify logic below by always having at least one entry per array

    // Create an array of OMTVALUE's that store all the pointers to all the data.
    // Each element in leafpointers is a pointer to a leaf.
    LEAFENTRY *leafpointers;
    size_t leafpointers_bytes = num_alloc * sizeof(LEAFENTRY);
    leafpointers = (LEAFENTRY *) sb_malloc_sized(leafpointers_bytes, true);
    leafpointers[0] = NULL;
    size_t keypointers_bytes = num_alloc * sizeof(void *);
    const void **key_pointers = (const void **) sb_malloc_sized(keypointers_bytes, true);
    size_t keysizes_bytes = num_alloc * sizeof(uint32_t);
    uint32_t *key_sizes = (uint32_t *) sb_malloc_sized(keysizes_bytes, true);

    // Capture pointers to old mempools' buffers (so they can be destroyed)
    BASEMENTNODE *XMALLOC_N(num_orig_basements, old_bns);

    uint32_t curr_le = 0;
    for (uint32_t i = 0; i < num_orig_basements; i++) {
        BN_DATA bd = BLB_DATA(node, i);
        struct array_info ai {.offset = curr_le, .le_array = leafpointers, .key_sizes_array = key_sizes, .key_ptr_array = key_pointers };
        bd->omt_iterate<array_info, array_item>(&ai);
        curr_le += bd->omt_size();
    }

    // Create an array that will store indexes of new pivots.
    // Each element in new_pivots is the index of a pivot key.
    // (Allocating num_le of them is overkill, but num_le is an upper bound.)
    size_t new_pivots_bytes = num_alloc * sizeof(uint32_t);
    uint32_t *new_pivots = (uint32_t *) sb_malloc_sized(new_pivots_bytes, true);
    new_pivots[0] = 0;

    // Each element in le_sizes is the size of the leafentry pointed to by leafpointers.
    size_t le_sizes_bytes = num_alloc * sizeof(size_t);
    size_t *le_sizes = (size_t *) sb_malloc_sized(le_sizes_bytes, true);
    le_sizes[0] = 0;

    // Create an array that will store the size of each basement.
    // This is the sum of the leaf sizes of all the leaves in that basement.
    // We don't know how many basements there will be, so we use num_le as the upper bound.
    size_t bn_sizes_bytes = num_alloc * sizeof(size_t);
    size_t *bn_sizes = (size_t *) sb_malloc_sized(bn_sizes_bytes, true);
    bn_sizes[0] = 0;

    // TODO 4050: All these arrays should be combined into a single array of some bn_info struct (pivot, msize, num_les).
    // Each entry is the number of leafentries in this basement.  (Again, num_le is overkill upper baound.)
    size_t num_les_this_bn_bytes = num_alloc * sizeof(uint32_t);
    uint32_t *num_les_this_bn = (uint32_t *) sb_malloc_sized(num_les_this_bn_bytes, true);
    num_les_this_bn[0] = 0;

    // Figure out the new pivots.
    // We need the index of each pivot, and for each basement we need
    // the number of leaves and the sum of the sizes of the leaves (memory requirement for basement).
    uint32_t curr_pivot = 0;
    uint32_t num_le_in_curr_bn = 0;
    uint32_t bn_size_so_far = 0;
#ifdef FT_INDIRECT
    uint32_t bn_ind_data_size = 0;
#endif
    for (uint32_t i = 0; i < num_le; i++) {
        uint32_t curr_le_size = leafentry_disksize((LEAFENTRY) leafpointers[i]);
#ifdef FT_INDIRECT
        LEAFENTRY le = (LEAFENTRY) leafpointers[i];
        uint32_t curr_ind_data_size = get_leafentry_ind_data_size(le);
#endif
        le_sizes[i] = curr_le_size;
#ifdef FT_INDIRECT
        if ((bn_size_so_far + bn_ind_data_size + curr_le_size + curr_ind_data_size > basementnodesize) && (num_le_in_curr_bn != 0)) {
            new_pivots[curr_pivot] = i-1;
            curr_pivot++;
            num_le_in_curr_bn = 0;
            bn_size_so_far = 0;
            bn_ind_data_size = 0;
        }
#else
        if ((bn_size_so_far + curr_le_size > basementnodesize) && (num_le_in_curr_bn != 0)) {
            // cap off the current basement node to end with the element before i
            new_pivots[curr_pivot] = i-1;
            curr_pivot++;
            num_le_in_curr_bn = 0;
            bn_size_so_far = 0;
        }
#endif
        num_le_in_curr_bn++;
        num_les_this_bn[curr_pivot] = num_le_in_curr_bn;
        bn_size_so_far += curr_le_size + sizeof(uint32_t) + key_sizes[i];
#ifdef FT_INDIRECT
        bn_ind_data_size += curr_ind_data_size;
#endif
        bn_sizes[curr_pivot] = bn_size_so_far;
    }
    // curr_pivot is now the total number of pivot keys in the leaf node
    int num_pivots   = curr_pivot;
    int num_children = num_pivots + 1;

    // now we need to fill in the new basement nodes and pivots

    // TODO: (Zardosht) this is an ugly thing right now
    // Need to figure out how to properly deal with seqinsert.
    // I am not happy with how this is being
    // handled with basement nodes
    uint32_t tmp_seqinsert = BLB_SEQINSERT(node, num_orig_basements - 1);
    // choose the max msn applied to any basement as the max msn applied to all new basements
    MSN max_msn = ZERO_MSN;
    for (uint32_t i = 0; i < num_orig_basements; i++) {
        MSN curr_msn = BLB_MAX_MSN_APPLIED(node,i);
        max_msn = (curr_msn.msn > max_msn.msn) ? curr_msn : max_msn;
    }
    // remove the basement node in the node, we've saved a copy
    for (uint32_t i = 0; i < num_orig_basements; i++) {
        // save a reference to the old basement nodes
        // we will need them to ensure that the memory
        // stays intact
        old_bns[i] = toku_detach_bn(node, i);
    }
    // Now destroy the old basements, but do not destroy leaves
#ifdef FT_INDIRECT
    assert(node->is_cloned == false);
    // Tell toku_destroy_ftnode_internals that it is called from
    // rebalance_ftnode_leaf so that it will not dereference the
    // pages. New ftnode will still reference the pages.
    node->is_rebalancing = true;
#endif
    toku_destroy_ftnode_internals(node);
#ifdef FT_INDIRECT
    // reset this flag after toku_destroy_ftnode_internals is completed.
    node->is_rebalancing = false;
#endif

    // now reallocate pieces and start filling them in
    invariant(num_children > 0);
    node->totalchildkeylens = node->bound_l.size + node->bound_r.size;
    if (num_pivots > 0) {
        // allocate pointers to pivot structs
        XCALLOC_N(num_pivots, node->childkeys);
    } else {
        node->childkeys = NULL;
    }
    node->n_children = num_children;
    XCALLOC_N(num_children, node->bp);             // allocate pointers to basements (bp)
    for (int i = 0; i < num_children; i++) {
        set_BLB(node, i, toku_create_empty_bn());  // allocate empty basements and set bp pointers
    }

    // now we start to fill in the data

    // first the pivots
    for (int i = 0; i < num_pivots; i++) {
        uint32_t keylen = key_sizes[new_pivots[i]];
        const void *key = key_pointers[new_pivots[i]];
        toku_memdup_dbt(&node->childkeys[i], key, keylen);
        node->totalchildkeylens += keylen;
    }

    uint32_t baseindex_this_bn = 0;
    // now the basement nodes
    for (int i = 0; i < num_children; i++) {
        // put back seqinsert
        BLB_SEQINSERT(node, i) = tmp_seqinsert;

        // create start (inclusive) and end (exclusive) boundaries for data of basement node
        uint32_t curr_start = (i==0) ? 0 : new_pivots[i-1]+1;               // index of first leaf in basement
        uint32_t curr_end = (i==num_pivots) ? num_le : new_pivots[i]+1;     // index of first leaf in next basement
        uint32_t num_in_bn = curr_end - curr_start;                         // number of leaves in this basement

        // create indexes for new basement
        invariant(baseindex_this_bn == curr_start);
        uint32_t num_les_to_copy = num_les_this_bn[i];
        invariant(num_les_to_copy == num_in_bn);

        // construct mempool for this basement
        size_t size_this_bn = bn_sizes[i];

        BN_DATA bd = BLB_DATA(node, i);
        bd->replace_contents_with_clone_of_sorted_array(
            num_les_to_copy,
            &key_pointers[baseindex_this_bn],
            &key_sizes[baseindex_this_bn],
            &leafpointers[baseindex_this_bn],
            &le_sizes[baseindex_this_bn],
            size_this_bn,
            fd
            );

        BP_STATE(node,i) = PT_AVAIL;
        BP_TOUCH_CLOCK(node,i);
        BLB_MAX_MSN_APPLIED(node,i) = max_msn;
        baseindex_this_bn += num_les_to_copy;  // set to index of next bn
#ifdef FT_INDIRECT
        BLB(node, i)->indirect_insert_full_count = bd->m_rebalance_indirect_full_cnt;
        BLB(node, i)->indirect_insert_odd_count = bd->m_rebalance_indirect_odd_cnt;
        BLB(node, i)->indirect_insert_full_size = bd->m_rebalance_indirect_full_size;
        BLB(node, i)->indirect_insert_odd_size = bd->m_rebalance_indirect_odd_size;
#endif
    }

    node->max_msn_applied_to_node_on_disk = max_msn;

    //SOSP: Jun : hack on rebalance, since rebalancing only happens before checkpoint or serialization
    //either case is right before writing back.
    //there is no chance for split or merge to mess up with unbound entries then we can solely use the bn
    // to do node-level accouting for unbound entries.
    if (n_unbound_entry > 0) {
        toku_list *newhead = &BLB(node, 0)->unbound_inserts;
        paranoid_invariant(toku_list_empty(newhead));
        for (uint32_t i = 0; i < num_orig_basements; i++) {
            toku_list *head = &old_bns[i]->unbound_inserts;
            if (toku_list_empty(head)) {
                 assert_zero(old_bns[i]->unbound_insert_count);
                 continue;
            }

            toku_list *first = head->next;
            toku_list *last = head->prev;
            toku_list_insert_m_between(newhead->prev, first, last, newhead);
            toku_list_init(head);

            old_bns[i]->unbound_insert_count = 0;
        }
        BLB(node,0)->unbound_insert_count = n_unbound_entry;
    }
    // destroy buffers of old mempools
    node->unbound_insert_count = n_unbound_entry;

    //toku_ft_node_empty_unbound_inserts_validation(node);
    //toku_ft_node_unbound_inserts_validation(node);


    for (uint32_t i = 0; i < num_orig_basements; i++) {
        toku_list * head = &old_bns[i]->unbound_inserts;
	if (toku_list_empty(head)) {
	   if (old_bns[i]->unbound_insert_count != 0) {
               //printf("i=%d, unbound_insert_count=%d\n", i, old_bns[i]->unbound_insert_count);
	       paranoid_invariant(old_bns[i]->unbound_insert_count == 0);
           }
        }
        destroy_basement_node(old_bns[i]);
    }

    sb_free_sized(key_pointers, keypointers_bytes);
    sb_free_sized(key_sizes, keysizes_bytes);
    sb_free_sized(leafpointers, leafpointers_bytes);
    toku_free(old_bns);
    sb_free_sized(new_pivots, new_pivots_bytes);
    sb_free_sized(le_sizes, le_sizes_bytes);
    sb_free_sized(bn_sizes, bn_sizes_bytes);
    sb_free_sized(num_les_this_bn, num_les_this_bn_bytes);
}  // end of rebalance_ftnode_leaf()


struct serialize_times {
    tokutime_t serialize_time;
    tokutime_t compress_time;
};

static void
serialize_and_compress_partition(FTNODE node,
                                 int childnum,
                                 enum toku_compression_method compression_method,
                                 SUB_BLOCK sb,
                                 struct serialize_times *st)
{
    // serialize, compress, update status
    tokutime_t t0 = toku_time_now();
    serialize_ftnode_partition(node, childnum, sb);
    tokutime_t t1 = toku_time_now();
    compress_ftnode_sub_block(sb, compression_method);
    tokutime_t t2 = toku_time_now();

    st->serialize_time += t1 - t0;
    st->compress_time += t2 - t1;
}

void
toku_create_compressed_partition_from_available(
    FTNODE node,
    int childnum,
    enum toku_compression_method compression_method,
    SUB_BLOCK sb
    )
{
    struct serialize_times st;
    memset(&st, 0, sizeof(st));
#ifdef FT_INDIRECT
    uint32_t bp_size = serialize_ftnode_partition_size_no_pages(node, childnum);
    unsigned char *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, bp_size, serialize_buf);

    uint32_t full_size = 0;
    uint32_t odd_size = 0;
    int num_items = 0;
    int num_pages = 0;

    if (node->height == 0) {
        full_size = BLB(node, childnum)->indirect_insert_full_size;
        odd_size = BLB(node, childnum)->indirect_insert_odd_size;
        num_items = BLB_DATA(node, childnum)->omt_size();
        num_pages = BLB(node, childnum)->indirect_insert_full_count + BLB(node, childnum)->indirect_insert_odd_count;
    } else {
        full_size = BNC(node, childnum)->indirect_insert_full_size;
        odd_size = BNC(node, childnum)->indirect_insert_odd_size;
        num_items = toku_bnc_n_entries(BNC(node, childnum));
        num_pages = BNC(node, childnum)->indirect_insert_full_count + BNC(node, childnum)->indirect_insert_odd_count;
    }

    if (full_size > 0 || odd_size > 0) {
        XCALLOC_N(1, sb->ind_data);
        sb->ind_data->num_items = num_items;
        XCALLOC_N(num_pages, sb->ind_data->pfns);
    } else {
        sb->ind_data = NULL;
    }

    sb->uncompressed_ptr = (char *)serialize_buf;
    serialize_ftnode_partition(node, childnum, sb);
    if (sb->ind_data) {
        assert(sb->ind_data->pfn_cnt == num_pages);
    }
    compress_ftnode_sub_block_with_indirect(sb, compression_method, node->is_cloned, node->height);
    if (sb->ind_data) {
        toku_free(sb->ind_data->pfns);
        toku_free(sb->ind_data);
    }
    // free the pages in this partition
    assert(node->is_cloned == false);
    destroy_nonleaf_ubi_vals(BNC(node, childnum));
#else
    serialize_and_compress_partition(node, childnum, compression_method, sb, &st);
    toku_ft_status_update_serialize_times(node, st.serialize_time, st.compress_time);

    //
    // now we have an sb that would be ready for being written out,
    // but we are not writing it out, we are storing it in cache for a potentially
    // long time, so we need to do some cleanup
    //
    // The buffer created above contains metadata in the first 8 bytes, and is overallocated
    // It allocates a bound on the compressed length (evaluated before compression) as opposed
    // to just the amount of the actual compressed data. So, we create a new buffer and copy
    // just the compressed data.
    //
    uint32_t compressed_size = toku_dtoh32(*(uint32_t *)sb->compressed_ptr);
    void* compressed_data = sb_malloc_sized(compressed_size, true);
    memcpy(compressed_data, (char *)sb->compressed_ptr + 8, compressed_size);
    sb_free_sized(sb->compressed_ptr, sb->compressed_size);
    sb->compressed_ptr = compressed_data;
    sb->compressed_size = compressed_size;
#endif
    if (sb->uncompressed_ptr) {
        sb_free_sized(sb->uncompressed_ptr, sb->uncompressed_size);
        sb->uncompressed_ptr = NULL;
    }
}

static void
serialize_and_compress_serially(FTNODE node,
                                int npartitions,
                                enum toku_compression_method compression_method,
                                struct sub_block sb[],
                                struct serialize_times *st) {
    for (int i = 0; i < npartitions; i++) {
        serialize_and_compress_partition(node, i, compression_method, &sb[i], st);
    }
}

struct serialize_compress_work {
    struct work base;
    FTNODE node;
    int i;
    enum toku_compression_method compression_method;
    struct sub_block *sb;
    struct serialize_times st;
};

static void *
serialize_and_compress_worker(void *arg) {
    struct workset *ws = (struct workset *) arg;
    while (1) {
        struct serialize_compress_work *w = (struct serialize_compress_work *) workset_get(ws);
        if (w == NULL)
            break;
        int i = w->i;
        serialize_and_compress_partition(w->node, i, w->compression_method, &w->sb[i], &w->st);
    }
    workset_release_ref(ws);
    return arg;
}

static void
serialize_and_compress_in_parallel(FTNODE node,
                                   int npartitions,
                                   enum toku_compression_method compression_method,
                                   struct sub_block sb[],
                                   struct serialize_times *st) {
    if (npartitions == 1) {
        serialize_and_compress_partition(node, 0, compression_method, &sb[0], st);
    } else {
        int T = num_cores;
        if (T > npartitions)
            T = npartitions;
        if (T > 0)
            T = T - 1;
        struct workset ws;
        ZERO_STRUCT(ws);
        workset_init(&ws);
        struct serialize_compress_work work[npartitions];
        workset_lock(&ws);
        for (int i = 0; i < npartitions; i++) {
            work[i] = (struct serialize_compress_work) { .base = {{NULL}},
                                                         .node = node,
                                                         .i = i,
                                                         .compression_method = compression_method,
                                                         .sb = sb,
                                                         .st = { .serialize_time = 0, .compress_time = 0} };
            workset_put_locked(&ws, &work[i].base);
        }
        workset_unlock(&ws);
        toku_thread_pool_run(ft_pool, 0, &T, serialize_and_compress_worker, &ws);
        workset_add_ref(&ws, T);
        serialize_and_compress_worker(&ws);
        workset_join(&ws);
        workset_destroy(&ws);

        // gather up the statistics from each thread's work item
        for (int i = 0; i < npartitions; i++) {
            st->serialize_time += work[i].st.serialize_time;
            st->compress_time += work[i].st.compress_time;
        }
    }
}

static void
serialize_and_compress_sb_node_info(FTNODE node, struct sub_block *sb,
        enum toku_compression_method compression_method, struct serialize_times *st) {
    // serialize, compress, update serialize times.
    tokutime_t t0 = toku_time_now();
    serialize_ftnode_info(node, sb);
    tokutime_t t1 = toku_time_now();
    compress_ftnode_sub_block(sb, compression_method);
    tokutime_t t2 = toku_time_now();

    st->serialize_time += t1 - t0;
    st->compress_time += t2 - t1;
}

int toku_serialize_ftnode_to_memory(FT h,
				    FTNODE node,
                                    FTNODE_DISK_DATA* ndd,
                                    unsigned int basementnodesize,
                                    enum toku_compression_method compression_method,
                                    bool do_rebalancing,
                                    bool in_parallel, // for loader is true, for toku_ftnode_flush_callback, is false
                            /*out*/ size_t *n_bytes_to_write,
                            /*out*/ size_t *n_uncompressed_bytes,
                            /*out*/ char  **bytes_to_write)
// Effect: Writes out each child to a separate malloc'd buffer, then compresses
//   all of them, and writes the uncompressed header, to bytes_to_write,
//   which is malloc'd.
//
//   The resulting buffer is guaranteed to be 4096-byte aligned and the total length is a multiple of 4096 (so we pad with zeros at the end if needed).
//   4096-byte padding is for O_DIRECT to work.
{
    toku_assert_entire_node_in_memory(node);

    if (do_rebalancing && node->height == 0) {
	rebalance_ftnode_leaf(h, node, basementnodesize);
    }
    const int npartitions = node->n_children;

    // Each partition represents a compressed sub block
    // For internal nodes, a sub block is a message buffer
    // For leaf nodes, a sub block is a basement node
    struct sub_block *XMALLOC_N(npartitions, sb);
    // This code does not require realloc, except as a convenient
    // way to handle being passed a null pointer.
    // Everything in *ndd from 0..npartitions will be overwritten below
    if (*ndd) {
        toku_free(*ndd);
    }
    XMALLOC_N(npartitions, *ndd);

    struct sub_block sb_node_info;
    for (int i = 0; i < npartitions; i++) {
        sub_block_init(&sb[i]);;
    }
    sub_block_init(&sb_node_info);

    //
    // First, let's serialize and compress the individual sub blocks
    //
    struct serialize_times st;
    memset(&st, 0, sizeof(st));
    if (in_parallel) {
        serialize_and_compress_in_parallel(node, npartitions, compression_method, sb, &st);
    }
    else {
        serialize_and_compress_serially(node, npartitions, compression_method, sb, &st);
    }

    //
    // Now lets create a sub-block that has the common node information,
    // This does NOT include the header
    //
    serialize_and_compress_sb_node_info(node, &sb_node_info, compression_method, &st);

    // update the serialize times, ignore the header for simplicity. we captured all
    // of the partitions' serialize times so that's probably good enough.
    toku_ft_status_update_serialize_times(node, st.serialize_time, st.compress_time);

    // now we have compressed each of our pieces into individual sub_blocks,
    // we can put the header and all the subblocks into a single buffer
    // and return it.

    // The total size of the node is:
    // size of header + disk size of the n+1 sub_block's created above
    uint32_t total_node_size = (serialize_node_header_size(node) // uncompressed header
                                 + sb_node_info.compressed_size   // compressed nodeinfo (without its checksum)
                                 + 4);                            // nodeinfo's checksum
    uint32_t total_uncompressed_size = (serialize_node_header_size(node) // uncompressed header
                                 + sb_node_info.uncompressed_size   // uncompressed nodeinfo (without its checksum)
                                 + 4);                            // nodeinfo's checksum
    // store the BP_SIZESs
    for (int i = 0; i < node->n_children; i++) {
        uint32_t len         = sb[i].compressed_size + 4; // data and checksum
        BP_SIZE (*ndd,i) = len;
        BP_START(*ndd,i) = total_node_size;
        total_node_size += sb[i].compressed_size + 4;
        total_uncompressed_size += sb[i].uncompressed_size + 4;
    }

    uint32_t total_buffer_size = roundup_to_multiple(BLOCK_ALIGNMENT, total_node_size); // make the buffer be 4096 bytes.
    char *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, total_buffer_size, data);
    char *curr_ptr = data;
    // now create the final serialized node

    // write the header
    struct wbuf wb;
    wbuf_init(&wb, curr_ptr, serialize_node_header_size(node));
    serialize_node_header(node, *ndd, &wb);
    assert(wb.ndone == wb.size);
    curr_ptr += serialize_node_header_size(node);

    // now write sb_node_info
    memcpy(curr_ptr, sb_node_info.compressed_ptr, sb_node_info.compressed_size);
    curr_ptr += sb_node_info.compressed_size;
    // write the checksum
    *(uint32_t *)curr_ptr = toku_htod32(sb_node_info.xsum);
    curr_ptr += sizeof(sb_node_info.xsum);

    for (int i = 0; i < npartitions; i++) {
        memcpy(curr_ptr, sb[i].compressed_ptr, sb[i].compressed_size);
        curr_ptr += sb[i].compressed_size;
        // write the checksum
        *(uint32_t *)curr_ptr = toku_htod32(sb[i].xsum);
        curr_ptr += sizeof(sb[i].xsum);
    }
    // Zero the rest of the buffer
    for (uint32_t i=total_node_size; i<total_buffer_size; i++) {
        data[i]=0;
    }

    assert(curr_ptr - data == total_node_size);
    *bytes_to_write = data;
    *n_bytes_to_write = total_buffer_size;
    *n_uncompressed_bytes = total_uncompressed_size;

    //
    // now that node has been serialized, go through sub_block's and free
    // memory
    //
    sb_free_sized(sb_node_info.compressed_ptr, sb_node_info.compressed_size);
    sb_free_sized(sb_node_info.uncompressed_ptr, sb_node_info.uncompressed_size);
    for (int i = 0; i < npartitions; i++) {
        sb_free_sized(sb[i].compressed_ptr, sb[i].compressed_size);
        sb_free_sized(sb[i].uncompressed_ptr, sb[i].uncompressed_size);
    }

    assert(0 == (*n_bytes_to_write)%BLOCK_ALIGNMENT);
    assert(0 == ((unsigned long long)(*bytes_to_write))%BLOCK_ALIGNMENT);
    toku_free(sb);
    return 0;
}

int
toku_serialize_ftnode_to (int fd, BLOCKNUM blocknum, FTNODE node, FTNODE_DISK_DATA* ndd, bool do_rebalancing, FT h, bool for_checkpoint, DISKOFF * p_size, DISKOFF * p_offset, bool is_blocking) {

    // If this function is called with is_blocking true, the compressed buffer is internally freed
    // If not, it is up to the caller to find and wait for it (provided by SFS)
    // XXX: may be better to pass the buffer reference out to the caller...

    size_t n_to_write;
    size_t n_uncompressed_bytes;
    char *compressed_buf = nullptr;
    // because toku_serialize_ftnode_to is only called for
    // in toku_ftnode_flush_callback, we pass false
    // for in_parallel. The reasoning is that when we write
    // nodes to disk via toku_ftnode_flush_callback, we
    // assume that it is being done on a non-critical
    // background thread (probably for checkpointing), and therefore
    // should not hog CPU,
    //
    // Should the above facts change, we may want to revisit
    // passing false for in_parallel here
    //
    // alternatively, we could have made in_parallel a parameter
    // for toku_serialize_ftnode_to, but instead we did this.
    int r = toku_serialize_ftnode_to_memory(
	h,
        node,
        ndd,
        h->h->basementnodesize,
        h->h->compression_method,
        do_rebalancing,
        false, // in_parallel
        &n_to_write,
        &n_uncompressed_bytes,
        &compressed_buf
        );
    if (r != 0) {
        return r;
    }

    // If the node has never been written, then write the whole buffer, including the zeros
    invariant(blocknum.b>=0);
    DISKOFF offset;

    toku_blocknum_realloc_on_disk(h->blocktable, blocknum, n_to_write, &offset,
                                  h, fd, for_checkpoint); //dirties h

    tokutime_t t0 = toku_time_now();

    toku_os_full_pwrite(fd, compressed_buf, n_to_write, offset, is_blocking);
    /* If is_blocking is true, we can free the buf after write returns */
    if (is_blocking) {
        toku_free(compressed_buf);
    }

    tokutime_t t1 = toku_time_now();

    tokutime_t io_time = t1 - t0;
    toku_ft_status_update_flush_reason(node, n_uncompressed_bytes, n_to_write, io_time, for_checkpoint);
    node->dirty = 0;  // See #1957.   Must set the node to be clean after serializing it so that it doesn't get written again on the next checkpoint or eviction.
    if(p_size && p_offset && has_unbound_msgs(node)) {
    	 *p_size = n_to_write;
     	 *p_offset = offset;
    }
    return 0;
}

void deserialize_child_buffer(NONLEAF_CHILDINFO bnc, struct rbuf *rbuf,
                         DESCRIPTOR desc, ft_compare_func cmp,
                         BP_IND_DATA UU(bp_ind))
{
    int r;
    int n_in_this_buffer;
    int32_t *fresh_offsets = NULL, *stale_offsets = NULL;
    int32_t *broadcast_offsets = NULL;
    int nfresh = 0, nstale = 0;
    int nbroadcast_offsets = 0;
#ifdef FT_INDIRECT
    int page_index = 0;
    unsigned int num_odd_inserts = 0;
    unsigned int num_full_inserts = 0;
    unsigned int odd_size = 0;
    unsigned int full_size = 0;
    unsigned long *pfns = NULL;
    if (bp_ind && (bp_ind->pfn_cnt > 0)) {
        pfns = bp_ind->pfns;
    }
#endif
    n_in_this_buffer = rbuf_int(rbuf);
    if (cmp) {
        int size = n_in_this_buffer * sizeof(int32_t);
        stale_offsets = (int32_t*) sb_malloc_sized(size, true);
        fresh_offsets = (int32_t*) sb_malloc_sized(size, true);
        broadcast_offsets = (int32_t*) sb_malloc_sized(size, true);
    }
    for (int i = 0; i < n_in_this_buffer; i++) {
        FT_MSG_S msg;
        XIDS xids;
        bool is_fresh;
        DBT k, v, m;
        ft_msg_read_from_rbuf(&msg, &k, &v, &m, rbuf, &xids, &is_fresh);

        int32_t *dest;
        enum ft_msg_type type = ft_msg_get_type(&msg);
        if (cmp) {
            if (ft_msg_type_applies_once(type)) {
                if (is_fresh) {
                    dest = &fresh_offsets[nfresh];
                    nfresh++;
                } else {
                    dest = &stale_offsets[nstale];
                    nstale++;
                }
#ifdef FT_INDIRECT
                if (type == FT_UNBOUND_INSERT) {
                    // copy pfns to fifo
                    unsigned int size;
                    memcpy(&size, (char*)v.data + 8, sizeof(unsigned int));
                    assert(size <= FTFS_PAGE_SIZE);
                    if (size < FTFS_PAGE_SIZE) {
                        num_odd_inserts += 1;
                        odd_size += size;
                    } else {
                        num_full_inserts += 1;
                        full_size += size;
                    }
                    unsigned long pfn = pfns[page_index];
                    memcpy((char*)v.data, &pfn, sizeof(unsigned long));
                    page_index += 1;
                    // increase refcount of pfn
                    ftfs_fifo_get_page_list(&pfn, 1);
                    ftfs_set_page_list_private(&pfn, 1, FT_MSG_VAL_BOUND_BIT);
                }
#endif
            } else if (ft_msg_type_applies_multiple(type) || ft_msg_type_does_nothing(type)) {
                dest = &broadcast_offsets[nbroadcast_offsets];
                nbroadcast_offsets++;
            } else {
                abort();
            }
        } else {
            dest = NULL;
        }
        r = toku_fifo_enq(bnc->buffer, &msg, is_fresh, dest); /* Copies the data into the fifo */
        lazy_assert_zero(r);
        xids_destroy(&xids);
    }

    invariant(rbuf->ndone == rbuf->size);
#ifdef FT_INDIRECT
    if (bp_ind) {
        bnc->indirect_insert_full_count = num_full_inserts;
        bnc->indirect_insert_odd_count = num_odd_inserts;
        bnc->indirect_insert_full_size = full_size;
        bnc->indirect_insert_odd_size = odd_size;
    }
#endif
    if (cmp) {
        struct toku_fifo_entry_key_msn_cmp_extra extra = { .desc = desc, .cmp = cmp, .fifo = bnc->buffer };
        r = toku::sort<int32_t, const struct toku_fifo_entry_key_msn_cmp_extra, toku_fifo_entry_key_msn_cmp>::mergesort_r(fresh_offsets, nfresh, extra);
        assert_zero(r);
        bnc->fresh_message_tree.destroy();
        bnc->fresh_message_tree.create_steal_sorted_array(&fresh_offsets, nfresh, n_in_this_buffer);
        r = toku::sort<int32_t, const struct toku_fifo_entry_key_msn_cmp_extra, toku_fifo_entry_key_msn_cmp>::mergesort_r(stale_offsets, nstale, extra);
        assert_zero(r);
        bnc->stale_message_tree.destroy();
        bnc->stale_message_tree.create_steal_sorted_array(&stale_offsets, nstale, n_in_this_buffer);
        bnc->broadcast_list.destroy();
        bnc->broadcast_list.create_steal_sorted_array(&broadcast_offsets, nbroadcast_offsets, n_in_this_buffer);
    }
}


// dump a buffer to stderr
// no locking around this for now
void
dump_bad_block(unsigned char *vp, uint64_t size) {
    const uint64_t linesize = 64;
    uint64_t n = size / linesize;
    for (uint64_t i = 0; i < n; i++) {
        dprintf(STDERR, "%p: ", vp);
        for (uint64_t j = 0; j < linesize; j++) {
            unsigned char c = vp[j];
            dprintf(STDERR, "%2.2X", c);
        }
        dprintf(STDERR, "\n");
        vp += linesize;
    }
    size = size % linesize;
    for (uint64_t i=0; i<size; i++) {
        if ((i % linesize) == 0)
            dprintf(STDERR, "%p: ", vp+i);
        dprintf(STDERR, "%2.2X", vp[i]);
        if (((i+1) % linesize) == 0)
            dprintf(STDERR, "\n");
    }
    dprintf(STDERR, "\n");
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

BASEMENTNODE toku_create_empty_bn(void) {
    BASEMENTNODE bn = toku_create_empty_bn_no_buffer();
    bn->data_buffer.initialize_empty();
    return bn;
}

//FIXME: both clones should also set the unbound_insert entry's node list to the clone -- not really -- even though the unbound msgs locate the old pair
BASEMENTNODE toku_clone_bn(BASEMENTNODE orig_bn) {
    BASEMENTNODE bn = toku_create_empty_bn_no_buffer();
    bn->max_msn_applied = orig_bn->max_msn_applied;
    bn->seqinsert = orig_bn->seqinsert;
    bn->stale_ancestor_messages_applied = orig_bn->stale_ancestor_messages_applied;
    bn->stat64_delta = orig_bn->stat64_delta;
    bn->data_buffer.clone(&orig_bn->data_buffer);

    bn->unbound_insert_count = orig_bn->unbound_insert_count;
#ifdef FT_INDIRECT
    bn->indirect_insert_full_count = orig_bn->indirect_insert_full_count;
    bn->indirect_insert_odd_count = orig_bn->indirect_insert_odd_count;
    bn->indirect_insert_full_size = orig_bn->indirect_insert_full_size;
    bn->indirect_insert_odd_size = orig_bn->indirect_insert_odd_size;
#endif
    //SOSP TODO: Bill not sure what to do here. should we copy the
    //list? only put it in one of the two clones? initiate a writeback
    //for safety? ugh.
    //lets do the list move clone is only used at the checkpoint COW. plus you already updated count_unbound .... -Jun
    if(!toku_list_empty(&orig_bn->unbound_inserts)) {
        paranoid_invariant(orig_bn->unbound_insert_count > 0);
        toku_list_move(&bn->unbound_inserts, &orig_bn->unbound_inserts);
        orig_bn->unbound_insert_count = 0;
    } else {
        paranoid_invariant(orig_bn->unbound_insert_count == 0);
        toku_list_init(&bn->unbound_inserts);
    }

    return bn;
}

BASEMENTNODE toku_create_empty_bn_no_buffer(void) {
    BASEMENTNODE XMALLOC(bn);
    bn->max_msn_applied.msn = 0;
    bn->seqinsert = 0;
    bn->stale_ancestor_messages_applied = false;
    bn->stat64_delta = ZEROSTATS;
    bn->data_buffer.init_zero();
    bn->unbound_insert_count = 0;
#ifdef FT_INDIRECT
    bn->indirect_insert_odd_count = 0;
    bn->indirect_insert_full_count = 0;
    bn->indirect_insert_full_size = 0;
    bn->indirect_insert_odd_size = 0;
#endif
    toku_list_init(&bn->unbound_inserts);
    return bn;
}

NONLEAF_CHILDINFO toku_create_empty_nl(void)
{
    NONLEAF_CHILDINFO XMALLOC(cn);
    int r = toku_fifo_create(&cn->buffer); assert_zero(r);
    cn->fresh_message_tree.create();
    cn->stale_message_tree.create();
    cn->broadcast_list.create();
    cn->unbound_insert_count = 0;
#ifdef FT_INDIRECT
    cn->indirect_insert_odd_count = 0;
    cn->indirect_insert_full_count = 0;
    cn->indirect_insert_full_size = 0;
    cn->indirect_insert_odd_size = 0;
#endif
    toku_list_init(&cn->unbound_inserts);
    return cn;
}

// does NOT create OMTs, just the FIFO
NONLEAF_CHILDINFO toku_clone_nl(NONLEAF_CHILDINFO orig_childinfo) {
    NONLEAF_CHILDINFO XMALLOC(cn);
    toku_fifo_clone(orig_childinfo->buffer, &cn->buffer);
    cn->fresh_message_tree.create_no_array();
    cn->stale_message_tree.create_no_array();
    cn->broadcast_list.create_no_array();

#ifdef FT_INDIRECT
    cn->indirect_insert_full_count = orig_childinfo->indirect_insert_full_count;
    cn->indirect_insert_full_size = orig_childinfo->indirect_insert_full_size;
    cn->indirect_insert_odd_count = orig_childinfo->indirect_insert_odd_count;
    cn->indirect_insert_odd_size = orig_childinfo->indirect_insert_odd_size;
#endif
    cn->unbound_insert_count = orig_childinfo->unbound_insert_count;
    if(!toku_list_empty(&orig_childinfo->unbound_inserts)) {
	paranoid_invariant(orig_childinfo->unbound_insert_count > 0);
        toku_list_move(&cn->unbound_inserts, &orig_childinfo->unbound_inserts);
	orig_childinfo->unbound_insert_count = 0;
    } else {
	paranoid_invariant(orig_childinfo->unbound_insert_count == 0);
        toku_list_init(&cn->unbound_inserts);
    }

#ifdef FT_INDIRECT
    toku_nonleaf_get_page_list(cn);
#endif
    return cn;
}

extern TOKULOGGER global_logger;

void destroy_basement_node (BASEMENTNODE bn)
{
    bn->data_buffer.destroy();

    if (!toku_list_empty(&bn->unbound_inserts)) {
        toku_list *unbound_msgs;
        unbound_msgs = &(bn->unbound_inserts);

        struct toku_list *list = unbound_msgs->next;

        while (list != unbound_msgs) {
            struct ubi_entry *entry = toku_list_struct(list, struct ubi_entry, node_list);
            paranoid_invariant(entry->state == UBI_UNBOUND);
            toku_mutex_lock(&global_logger->ubi_lock);
            toku_list_remove(&entry->in_or_out);
            toku_mutex_unlock(&global_logger->ubi_lock);
            list = list->next;
        }

        list = unbound_msgs->next;

        while (list != unbound_msgs) {
            struct ubi_entry *entry = toku_list_struct(list, struct ubi_entry, node_list);
            paranoid_invariant(entry->state == UBI_UNBOUND);

            toku_list *to_be_removed = list;
            list = list->next;
            toku_list_remove(to_be_removed);
            bn->unbound_insert_count--;
        }
    }

    paranoid_invariant(bn->unbound_insert_count == 0);
    paranoid_invariant(toku_list_empty(&bn->unbound_inserts));
    toku_free(bn);
}

void destroy_nonleaf_childinfo (NONLEAF_CHILDINFO nl)
{
    toku_fifo_free(&nl->buffer);
    nl->fresh_message_tree.destroy();
    nl->stale_message_tree.destroy();
    nl->broadcast_list.destroy();
    paranoid_invariant(nl->unbound_insert_count == 0);
    paranoid_invariant(toku_list_empty(&nl->unbound_inserts));
    toku_free(nl);
}

void read_block_from_fd_into_rbuf(
    int fd,
    BLOCKNUM blocknum,
    FT h,
    struct rbuf *rb
    )
{
    // get the file offset and block size for the block
    DISKOFF offset, size;
    toku_translate_blocknum_to_offset_size(h->blocktable, blocknum, &offset, &size);
    DISKOFF size_aligned = roundup_to_multiple(BLOCK_ALIGNMENT, size);
    uint8_t *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, size_aligned, raw_block);
    rbuf_init(rb, raw_block, size);
    // read the block
    ssize_t rlen = toku_os_pread(fd, raw_block, size_aligned, offset);
    assert((DISKOFF)rlen >= size);
    assert((DISKOFF)rlen <= size_aligned);
}

static const int read_header_heuristic_max = 32*1024;


#ifndef MIN
#define MIN(a,b) (((a)>(b)) ? (b) : (a))
#endif

static void read_ftnode_header_from_fd_into_rbuf_if_small_enough (int fd, BLOCKNUM blocknum, FT ft, struct rbuf *rb, struct ftnode_fetch_extra *bfe)
// Effect: If the header part of the node is small enough, then read it into the rbuf.  The rbuf will be allocated to be big enough in any case.
{
    DISKOFF offset, size;
    toku_translate_blocknum_to_offset_size(ft->blocktable, blocknum, &offset, &size);
    DISKOFF read_size = roundup_to_multiple(BLOCK_ALIGNMENT, MIN(read_header_heuristic_max, size));
    uint8_t *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, roundup_to_multiple(BLOCK_ALIGNMENT, size), raw_block);
    rbuf_init(rb, raw_block, read_size);

    // read the block
    tokutime_t t0 = toku_time_now();
    ssize_t rlen = toku_os_pread(fd, raw_block, read_size, offset);
    tokutime_t t1 = toku_time_now();

    assert(rlen >= 0);
    rbuf_init(rb, raw_block, rlen);

    bfe->bytes_read = rlen;
    bfe->io_time = t1 - t0;
    toku_ft_status_update_pivot_fetch_reason(bfe);
}

//
// read the compressed partition into the sub_block,
// validate the checksum of the compressed data
//
int
read_compressed_sub_block(struct rbuf *rb, struct sub_block *sb)
{
    int r = 0;
    sb->compressed_size = rbuf_int(rb);
    sb->uncompressed_size = rbuf_int(rb);
    bytevec* cp = (bytevec*)&sb->compressed_ptr;
    rbuf_literal_bytes(rb, cp, sb->compressed_size);
    sb->xsum = rbuf_int(rb);
    // let's check the checksum
    uint32_t actual_xsum = x1764_memory((char *)sb->compressed_ptr-8, 8+sb->compressed_size);
    if (sb->xsum != actual_xsum) {
        r = TOKUDB_BAD_CHECKSUM;
    }
    return r;
}

static int
read_and_decompress_sub_block(struct rbuf *rb, struct sub_block *sb)
{
    int r = 0;
    r = read_compressed_sub_block(rb, sb);
    if (r != 0) {
        goto exit;
    }
    just_decompress_sub_block(sb);
exit:
    return r;
}

// Allocates space for the sub-block and de-compresses the data from
// the supplied compressed pointer..
void
just_decompress_sub_block(struct sub_block *sb)
{
    // <CER> TODO: Add assert that the subblock was read in.
    sb->uncompressed_ptr = sb_malloc_sized(sb->uncompressed_size, true);

    toku_decompress(
        (Bytef *) sb->uncompressed_ptr,
        sb->uncompressed_size,
        (Bytef *) sb->compressed_ptr,
        sb->compressed_size
        );
}
// verify the checksum
int
verify_ftnode_sub_block (struct sub_block *sb)
{
#ifdef FT_INDIRECT
    (void)sb;
    return 0;
#else
    int r = 0;
    // first verify the checksum
    uint32_t data_size = sb->uncompressed_size - 4; // checksum is 4 bytes at end
    uint32_t stored_xsum = toku_dtoh32(*((uint32_t *)((char *)sb->uncompressed_ptr + data_size)));
    uint32_t actual_xsum = x1764_memory(sb->uncompressed_ptr, data_size);
    if (stored_xsum != actual_xsum) {
        dump_bad_block((Bytef *) sb->uncompressed_ptr, sb->uncompressed_size);
        r = TOKUDB_BAD_CHECKSUM;
    }
    return r;
#endif
}

// This function deserializes the data stored by serialize_ftnode_info
static int
deserialize_ftnode_info(struct sub_block *sb, FTNODE node)
{
    // sb_node_info->uncompressed_ptr stores the serialized node information
    // this function puts that information into node
    // first verify the checksum
    int r = 0;
    r = verify_ftnode_sub_block(sb);
    if (r != 0) {
        goto exit;
    }
    uint32_t data_size;
    data_size = sb->uncompressed_size - 4; // checksum is 4 bytes at end
    // now with the data verified, we can read the information into the node
    struct rbuf rb;
    rbuf_init(&rb, (unsigned char *) sb->uncompressed_ptr, data_size);

    node->max_msn_applied_to_node_on_disk = rbuf_msn(&rb);
    (void)rbuf_int(&rb);
    node->flags = rbuf_int(&rb);
    node->height = rbuf_int(&rb);
    if (node->layout_version_read_from_disk < FT_LAYOUT_VERSION_19) {
        (void) rbuf_int(&rb); // optimized_for_upgrade
    }
    if (node->layout_version_read_from_disk >= FT_LAYOUT_VERSION_22) {
        rbuf_TXNID(&rb, &node->oldest_referenced_xid_known);
    }

    // now create the basement nodes or childinfos, depending on whether this is a
    // leaf node or internal node
    // now the subtree_estimates

    // n_children is now in the header, nd the allocatio of the node->bp is in deserialize_ftnode_from_rbuf.

    // now the pivots
    node->totalchildkeylens = 0;
    bytevec childkeyptr;
    unsigned int cklen;
    cklen = rbuf_int(&rb);
    if (cklen == 0) {
        toku_init_dbt(&node->bound_l);
    } else {
        rbuf_literal_bytes(&rb, &childkeyptr, cklen);
        toku_memdup_dbt(&node->bound_l, childkeyptr, cklen);
        node->totalchildkeylens += cklen;
    }
    cklen = rbuf_int(&rb);
    if (cklen == 0) {
        toku_init_dbt(&node->bound_r);
    } else {
        rbuf_literal_bytes(&rb, &childkeyptr, cklen);
        toku_memdup_dbt(&node->bound_r, childkeyptr, cklen);
        node->totalchildkeylens += cklen;
    }
    if (node->n_children > 1) {
        XMALLOC_N(node->n_children - 1, node->childkeys);
        for (int i=0; i < node->n_children-1; i++) {
            rbuf_bytes(&rb, &childkeyptr, &cklen);
            toku_memdup_dbt(&node->childkeys[i], childkeyptr, cklen);
            node->totalchildkeylens += cklen;
        }
    } else {
        node->childkeys = NULL;
    }

    // if this is an internal node, unpack the block nums, and fill in necessary fields
    // of childinfo
    if (node->height > 0) {
        for (int i = 0; i < node->n_children; i++) {
            BP_BLOCKNUM(node,i) = rbuf_blocknum(&rb);
            BP_WORKDONE(node, i) = 0;
            uint32_t len = rbuf_int(&rb);
            if (len) {
                bytevec lift;
                rbuf_literal_bytes(&rb, &lift, len);
                toku_memdup_dbt(&BP_LIFT(node, i), lift, len);
            } else {
                toku_init_dbt(&BP_LIFT(node, i));
            }
        }
    } else {
        for (int i = 0; i < node->n_children; i++) {
            toku_init_dbt(&BP_LIFT(node, i));
        }
    }

    // make sure that all the data was read
    if (data_size != rb.ndone) {
        dump_bad_block(rb.buf, rb.size);
        abort();
    }
exit:
    return r;
}

void setup_available_ftnode_partition(FTNODE node, int i)
{
    if (node->height == 0) {
        set_BLB(node, i, toku_create_empty_bn());
        BLB_MAX_MSN_APPLIED(node, i) = node->max_msn_applied_to_node_on_disk;
    } else {
        set_BNC(node, i, toku_create_empty_nl());
    }
}

void setup_leaf_prefetch_state(FTNODE node)
{
    assert(node != NULL);
    assert(node->height == 0);
    for (int i=0; i < node->n_children; i++) {
        BP_PREFETCH_PFN(node, i)    = NULL;
        BP_PREFETCH_PFN_CNT(node, i) = 0;
        BP_PREFETCH_FLAG(node, i) = false;
        BP_PREFETCH_BUF(node, i) = NULL;
    }
}

// Assign the child_to_read member of the bfe from the given brt node
// that has been brought into memory.
static void
update_bfe_using_ftnode(FTNODE node, struct ftnode_fetch_extra *bfe)
{
    if (bfe->type == ftnode_fetch_subset && bfe->search != NULL) {
        // we do not take into account prefetching yet
        // as of now, if we need a subset, the only thing
        // we can possibly require is a single basement node
        // we find out what basement node the query cares about
        // and check if it is available
        bfe->child_to_read = toku_ft_search_which_child(
            bfe->h,
            node,
            bfe->search
            );
    } else if (bfe->type == ftnode_fetch_keymatch) {
        // we do not take into account prefetching yet
        // as of now, if we need a subset, the only thing
        // we can possibly require is a single basement node
        // we find out what basement node the query cares about
        // and check if it is available
        paranoid_invariant(bfe->h->key_ops.keycmp);
        if (node->height == 0) {
            int left_child = toku_bfe_leftmost_child_wanted(bfe, node);
            int right_child = toku_bfe_rightmost_child_wanted(bfe, node);
            if (left_child == right_child) {
                bfe->child_to_read = left_child;
            }
        }
    }
}

// Using the search parameters in the bfe, this function will
// initialize all of the given brt node's partitions.
static void
setup_partitions_using_bfe(FTNODE node,
                           struct ftnode_fetch_extra *bfe,
                           bool data_in_memory)
{
    // Leftmost and Rightmost Child bounds.
    int lc, rc;
    if (bfe->type == ftnode_fetch_subset || bfe->type == ftnode_fetch_prefetch) {
        lc = toku_bfe_leftmost_child_wanted(bfe, node);
        rc = toku_bfe_rightmost_child_wanted(bfe, node);
    } else {
        lc = -1;
        rc = -1;
    }

    //
    // setup memory needed for the node
    //
    //printf("node height %d, blocknum %" PRId64 ", type %d lc %d rc %d\n", node->height, node->thisnodename.b, bfe->type, lc, rc);
    for (int i = 0; i < node->n_children; i++) {
        BP_INIT_UNTOUCHED_CLOCK(node,i);
        if (data_in_memory) {
            if (!toku_need_compression()) {
                BP_STATE(node, i) = PT_AVAIL;
            } else {
                BP_STATE(node, i) = ((toku_bfe_wants_child_available(bfe, i) || (lc <= i && i <= rc))
                                    ? PT_AVAIL : PT_COMPRESSED);
            }
        } else {
            BP_STATE(node, i) = PT_ON_DISK;
        }
        BP_WORKDONE(node,i) = 0;

        switch (BP_STATE(node,i)) {
        case PT_AVAIL:
            setup_available_ftnode_partition(node, i);
            BP_TOUCH_CLOCK(node,i);
            break;
        case PT_COMPRESSED:
            set_BSB(node, i, sub_block_creat());
            break;
        case PT_ON_DISK:
            set_BNULL(node, i);
            break;
        case PT_INVALID:
            abort();
        }
    }
}

static void setup_ftnode_partitions(FTNODE node, struct ftnode_fetch_extra* bfe, bool data_in_memory)
// Effect: Used when reading a ftnode into main memory, this sets up the partitions.
//   We set bfe->child_to_read as well as the BP_STATE and the data pointers (e.g., with set_BSB or set_BNULL or other set_ operations).
// Arguments:  Node: the node to set up.
//             bfe:  Describes the key range needed.
//             data_in_memory: true if we have all the data (in which case we set the BP_STATE to be either PT_AVAIL or PT_COMPRESSED depending on the bfe.
//                             false if we don't have the partitions in main memory (in which case we set the state to PT_ON_DISK.
{
    // Set bfe->child_to_read.
    update_bfe_using_ftnode(node, bfe);

    // Setup the partitions.
    setup_partitions_using_bfe(node, bfe, data_in_memory);
}

/* deserialize the partition from the sub-block's uncompressed buffer
 * and destroy the uncompressed buffer
 */
static int
deserialize_ftnode_partition(
    struct sub_block *sb,
    FTNODE node,
    int childnum,      // which partition to deserialize
    DESCRIPTOR desc,
    ft_compare_func cmp
    )
{
    int r = 0;
    r = verify_ftnode_sub_block(sb);
    if (r != 0) {
        goto exit;
    }
    uint32_t data_size;
    data_size = sb->uncompressed_size - 4; // checksum is 4 bytes at end

    // now with the data verified, we can read the information into the node
    struct rbuf rb;
    rbuf_init(&rb, (unsigned char *) sb->uncompressed_ptr, data_size);
    unsigned char ch;
    ch = rbuf_char(&rb);
#ifdef FT_INDIRECT
    if (node->height > 0) {
        assert(ch == FTNODE_PARTITION_FIFO_MSG);
        deserialize_child_buffer(BNC(node, childnum), &rb, desc, cmp, sb->ind_data);
        BP_WORKDONE(node, childnum) = 0;
    } else {
        assert(ch == FTNODE_PARTITION_OMT_LEAVES);
        BLB_SEQINSERT(node, childnum) = 0;
        uint32_t num_entries = rbuf_int(&rb);
        assert(data_size == rb.size);
        if (!sb->ind_data) {
            // we are now at the first byte of first leafentry
            data_size -= rb.ndone; // remaining bytes of leafentry data
            BASEMENTNODE bn = BLB(node, childnum);
            bn->data_buffer.initialize_from_data(num_entries, rb.buf+rb.ndone, data_size, NULL, 0, NULL, NULL);
            rb.ndone += data_size;
        } else {
            uint32_t num_pages = sb->ind_data->pfn_cnt;
            uint32_t odd_data_size = 0;
            uint32_t odd_page_cnt = 0;
            uint32_t full_page_cnt = 0;
            data_size -= rb.ndone;
            BASEMENTNODE bn = BLB(node, childnum);
            bn->data_buffer.initialize_from_data(num_entries, rb.buf+rb.ndone, data_size,
                                                 sb->ind_data->pfns, num_pages,
                                                 &odd_data_size, &odd_page_cnt);
            full_page_cnt = num_pages - odd_page_cnt;
            bn->indirect_insert_full_count = full_page_cnt;
            bn->indirect_insert_full_size  = full_page_cnt * FTFS_PAGE_SIZE;
            bn->indirect_insert_odd_count  = odd_page_cnt;
            bn->indirect_insert_odd_size   = odd_data_size;
            rb.ndone += data_size;
        }
    }
    if (toku_need_compression()) {
        sb_free_sized(sb->uncompressed_ptr, sb->uncompressed_size);
    }
    assert(rb.ndone == rb.size);
#else
    if (node->height > 0) {
        assert(ch == FTNODE_PARTITION_FIFO_MSG);
        deserialize_child_buffer(BNC(node, childnum), &rb, desc, cmp, NULL);
        BP_WORKDONE(node, childnum) = 0;
    }
    else {
        assert(ch == FTNODE_PARTITION_OMT_LEAVES);
        BLB_SEQINSERT(node, childnum) = 0;
        uint32_t num_entries = rbuf_int(&rb);
        // we are now at the first byte of first leafentry
        data_size -= rb.ndone; // remaining bytes of leafentry data

        BASEMENTNODE bn = BLB(node, childnum);
        bn->data_buffer.initialize_from_data(num_entries, &rb.buf[rb.ndone], data_size, NULL, 0, NULL, NULL);
        rb.ndone += data_size;
    }
    assert(rb.ndone == rb.size);
    sb_free_sized(sb->uncompressed_ptr, sb->uncompressed_size);
#endif
exit:
    return r;
}

static int
decompress_and_deserialize_worker(struct rbuf curr_rbuf, struct sub_block curr_sb, FTNODE node, int child,
        DESCRIPTOR desc, ft_compare_func cmp, tokutime_t *decompress_time)
{
    int r = 0;
    tokutime_t t0 = toku_time_now();
    r = read_and_decompress_sub_block(&curr_rbuf, &curr_sb);
    tokutime_t t1 = toku_time_now();
    if (r == 0) {
        // at this point, sb->uncompressed_ptr stores the serialized node partition
        r = deserialize_ftnode_partition(&curr_sb, node, child, desc, cmp);
    }
    *decompress_time = t1 - t0;
    return r;
}

static int
check_and_copy_compressed_sub_block_worker(struct rbuf curr_rbuf, struct sub_block UU(curr_sb), FTNODE node, int child)
{
#ifdef FT_INDIRECT
    SUB_BLOCK bp_sb;
    bp_sb = BSB(node, child);
    bp_sb->compressed_size = curr_rbuf.size;
    bp_sb->compressed_ptr = sb_malloc_sized(bp_sb->compressed_size, true);
    memcpy(bp_sb->compressed_ptr, curr_rbuf.buf, bp_sb->compressed_size);
    return 0;
#else
    int r = 0;
    r = read_compressed_sub_block(&curr_rbuf, &curr_sb);
    if (r != 0) {
        goto exit;
    }

    SUB_BLOCK bp_sb;
    bp_sb = BSB(node, child);
    bp_sb->compressed_size = curr_sb.compressed_size;
    bp_sb->uncompressed_size = curr_sb.uncompressed_size;
    bp_sb->compressed_ptr = sb_malloc_sized(bp_sb->compressed_size, true);
    memcpy(bp_sb->compressed_ptr, curr_sb.compressed_ptr, bp_sb->compressed_size);
exit:
    return r;
#endif
}

static FTNODE alloc_ftnode_for_deserialize(uint32_t fullhash, BLOCKNUM blocknum) {
// Effect: Allocate an FTNODE and fill in the values that are not read from
    FTNODE node = (FTNODE) sb_malloc_sized(sizeof(*node), true);
    node->fullhash = fullhash;
    node->thisnodename = blocknum;
    node->dirty = 0;
    node->unbound_insert_count = 0;
#ifdef FT_INDIRECT
    node->pfn_array = nullptr;
    node->leaf_le_num = 0;
    node->nonleaf_fifo_entries = 0;
    node->is_cloned = false;
    node->is_rebalancing = false;
#endif
    node->bp = nullptr;
    node->oldest_referenced_xid_known = TXNID_NONE;
    toku_init_dbt(&node->bound_l);
    toku_init_dbt(&node->bound_r);
    return node;
}

static int
deserialize_ftnode_header_from_rbuf_if_small_enough (FTNODE *ftnode,
                                                      FTNODE_DISK_DATA* ndd,
                                                      BLOCKNUM blocknum,
                                                      uint32_t fullhash,
                                                      struct ftnode_fetch_extra *bfe,
                                                      struct rbuf *rb,
                                                      int fd)
// If we have enough information in the rbuf to construct a header, then do so.
// Also fetch in the basement node if needed.
// Return 0 if it worked.  If something goes wrong (including that we are looking at some old data format that doesn't have partitions) then return nonzero.
{
    int r = 0;

    tokutime_t t0, t1;
    tokutime_t decompress_time = 0;
    tokutime_t deserialize_time = 0;

    t0 = toku_time_now();

    FTNODE node = alloc_ftnode_for_deserialize(fullhash, blocknum);

    if (rb->size < 24) {
        // TODO: What error do we return here?
        // Does it even matter?
        r = toku_db_badformat();
        goto cleanup;
    }

    bytevec magic;
    rbuf_literal_bytes(rb, &magic, 8);
    if (memcmp(magic, "tokuleaf", 8)!=0 &&
        memcmp(magic, "tokunode", 8)!=0) {
        r = toku_db_badformat();
        goto cleanup;
    }

    node->layout_version_read_from_disk = rbuf_int(rb);
    if (node->layout_version_read_from_disk < FT_FIRST_LAYOUT_VERSION_WITH_BASEMENT_NODES) {
        // This code path doesn't have to worry about upgrade.
        r = toku_db_badformat();
        goto cleanup;
    }

    // If we get here, we know the node is at least
    // FT_FIRST_LAYOUT_VERSION_WITH_BASEMENT_NODES.  We haven't changed
    // the serialization format since then (this comment is correct as of
    // version 20, which is Deadshot) so we can go ahead and say the
    // layout version is current (it will be as soon as we finish
    // deserializing).
    // TODO(leif): remove node->layout_version (#5174)
    node->layout_version = FT_LAYOUT_VERSION;

    node->layout_version_original = rbuf_int(rb);
    node->build_id = rbuf_int(rb);
    node->n_children = rbuf_int(rb);
    // Guaranteed to be have been able to read up to here.  If n_children
    // is too big, we may have a problem, so check that we won't overflow
    // while reading the partition locations.
    unsigned int nhsize;
    nhsize =  serialize_node_header_size(node); // we can do this because n_children is filled in.
    unsigned int needed_size;
    needed_size = nhsize + 12; // we need 12 more so that we can read the compressed block size information that follows for the nodeinfo.
    if (needed_size > rb->size) {
        printf("%s: header size is too large: %d\n", __func__, needed_size);
        r = toku_db_badformat();
        goto cleanup;
    }

    assert(node->n_children > 0);
    XMALLOC_N(node->n_children, node->bp);
    XMALLOC_N(node->n_children, *ndd);
    // read the partition locations
    for (int i=0; i<node->n_children; i++) {
        BP_START(*ndd,i) = rbuf_int(rb);
        BP_SIZE (*ndd,i) = rbuf_int(rb);
        BP_PREFETCH_PFN(node,i)    = NULL;
        BP_PREFETCH_PFN_CNT(node,i) = 0;
        BP_PREFETCH_FLAG(node,i) = false;
        BP_PREFETCH_BUF(node,i) = NULL;
    }
#ifdef FT_INDIRECT
    for (int i=0; i<node->n_children; i++) {
        BP_PAGE_DATA_START(*ndd,i) = rbuf_int(rb);
        BP_PAGE_DATA_SIZE (*ndd,i) = rbuf_int(rb);
    }
    node->node_info_size = rbuf_int(rb);
#endif
    uint32_t checksum;
    checksum = x1764_memory(rb->buf, rb->ndone);
    uint32_t stored_checksum;
    stored_checksum = rbuf_int(rb);
    if (stored_checksum != checksum) {
        printf("ftnode header chechsum mismatch: stored_checksum=%d, checksum=%d\n", stored_checksum, checksum);
       	assert(false);
    }

    // Now we want to read the pivot information.
    struct sub_block sb_node_info;
    sub_block_init(&sb_node_info);
#ifdef FT_INDIRECT
    get_node_info_from_rbuf(&sb_node_info, rb, node->node_info_size);
#else
    sb_node_info.compressed_size = rbuf_int(rb); // we'll be able to read these because we checked the size earlier.
    sb_node_info.uncompressed_size = rbuf_int(rb);
    if (rb->size-rb->ndone < sb_node_info.compressed_size + 8) {
        r = toku_db_badformat();
        goto cleanup;
    }
    // Finish reading compressed the sub_block
    bytevec* cp;
    cp = (bytevec*)&sb_node_info.compressed_ptr;
    rbuf_literal_bytes(rb, cp, sb_node_info.compressed_size);
    sb_node_info.xsum = rbuf_int(rb);
    // let's check the checksum
    uint32_t actual_xsum;
    actual_xsum = x1764_memory((char *)sb_node_info.compressed_ptr-8, 8+sb_node_info.compressed_size);
    if (sb_node_info.xsum != actual_xsum) {
        r = TOKUDB_BAD_CHECKSUM;
        goto cleanup;
    }

    // Now decompress the subblock
    sb_node_info.uncompressed_ptr = sb_malloc_sized(sb_node_info.uncompressed_size, true);
    {
        tokutime_t decompress_t0 = toku_time_now();
        toku_decompress(
            (Bytef *) sb_node_info.uncompressed_ptr,
            sb_node_info.uncompressed_size,
            (Bytef *) sb_node_info.compressed_ptr,
            sb_node_info.compressed_size
            );
        tokutime_t decompress_t1 = toku_time_now();
        decompress_time = decompress_t1 - decompress_t0;
    }
#endif
    // at this point sb->uncompressed_ptr stores the serialized node info.
    r = deserialize_ftnode_info(&sb_node_info, node);
    if (r != 0) {
        goto cleanup;
    }
#ifndef FT_INDIRECT
    sb_free_sized(sb_node_info.uncompressed_ptr, sb_node_info.uncompressed_size);
#endif
    sb_node_info.uncompressed_ptr = NULL;

    // Now we have the ftnode_info.  We have a bunch more stuff in the
    // rbuf, so we might be able to store the compressed data for some
    // objects.
    // We can proceed to deserialize the individual subblocks.
    paranoid_invariant(is_valid_ftnode_fetch_type(bfe->type));

    // setup the memory of the partitions
    // for partitions being decompressed, create either FIFO or basement node
    // for partitions staying compressed, create sub_block
    setup_ftnode_partitions(node, bfe, false);

    // We must capture deserialize and decompression time before
    // the pf_callback, otherwise we would double-count.
    t1 = toku_time_now();
    deserialize_time = (t1 - t0) - decompress_time;

    // do partial fetch if necessary
    if (bfe->type != ftnode_fetch_none) {
        PAIR_ATTR attr;
        r = toku_ftnode_pf_callback(node, *ndd, bfe, fd, &attr);
        if (r != 0) {
            goto cleanup;
        }
    }

    // handle clock
    for (int i = 0; i < node->n_children; i++) {
        if (toku_bfe_wants_child_available(bfe, i)) {
            paranoid_invariant(BP_STATE(node,i) == PT_AVAIL);
            BP_TOUCH_CLOCK(node,i);
        }
    }
    *ftnode = node;
    r = 0;

cleanup:
    if (r == 0) {
        bfe->deserialize_time += deserialize_time;
        bfe->decompress_time += decompress_time;
        toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);
    }
    if (r != 0) {
        if (node) {
            toku_free(*ndd);
            toku_free(node->bp);
            sb_free_sized(node, sizeof(*node));
        }
    }
    return r;
}

static int
read_and_decompress_block_from_fd_into_rbuf(int fd, BLOCKNUM blocknum,
                                            DISKOFF offset, DISKOFF size,
                                            FT h,
                                            struct rbuf *rb,
                                            /* out */ int *layout_version_p);

static int
deserialize_ftnode_from_rbuf(
    FTNODE *ftnode,
    FTNODE_DISK_DATA* ndd,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    struct ftnode_fetch_extra* bfe,
    STAT64INFO UU(info),
    struct rbuf *rb,
    int UU(fd)
    )
// Effect: deserializes a ftnode that is in rb (with pointer of rb just past the magic) into a FTNODE.
{
    int r = 0;
    struct sub_block sb_node_info;
#ifdef FT_INDIRECT
    struct sub_block *sb = NULL;
    int total_page_size = 0;
#endif
    tokutime_t t0, t1;
    tokutime_t decompress_time = 0;
    tokutime_t deserialize_time = 0;

    t0 = toku_time_now();

    FTNODE node = alloc_ftnode_for_deserialize(fullhash, blocknum);
    // now start reading from rbuf
    // first thing we do is read the header information
    bytevec magic;
    rbuf_literal_bytes(rb, &magic, 8);
    if (memcmp(magic, "tokuleaf", 8)!=0 &&
        memcmp(magic, "tokunode", 8)!=0) {
        r = toku_db_badformat();
        assert(false);
        goto cleanup;
    }

    node->layout_version_read_from_disk = rbuf_int(rb);
    lazy_assert(node->layout_version_read_from_disk >= FT_LAYOUT_MIN_SUPPORTED_VERSION);

    // Check if we are reading in an older node version.
    if (node->layout_version_read_from_disk <= FT_LAYOUT_VERSION_14) {
        assert(false);
        goto cleanup;
    }

    // Upgrade versions after 14 to current.  This upgrade is trivial, it
    // removes the optimized for upgrade field, which has already been
    // removed in the deserialization code (see
    // deserialize_ftnode_info()).
    node->layout_version = FT_LAYOUT_VERSION;
    node->layout_version_original = rbuf_int(rb);
    node->build_id = rbuf_int(rb);
    node->n_children = rbuf_int(rb);
    assert(node->n_children > 0);
    node->bp = (ftnode_partition *)sb_malloc_sized(node->n_children * sizeof(*node->bp), true);
    *ndd = (FTNODE_DISK_DATA) sb_malloc_sized(node->n_children * sizeof(*(*ndd)), true);
    // read the partition locations
    for (int i=0; i<node->n_children; i++) {
        BP_START(*ndd,i) = rbuf_int(rb);
        BP_SIZE (*ndd,i) = rbuf_int(rb);
        BP_PREFETCH_PFN(node,i)    = NULL;
        BP_PREFETCH_PFN_CNT(node,i) = 0;
        BP_PREFETCH_FLAG(node,i) = false;
        BP_PREFETCH_BUF(node,i) = NULL;
    }
#ifdef FT_INDIRECT
    XMALLOC_N(node->n_children, sb); // now n_children is valid
    DISKOFF node_offset, node_size;
    toku_translate_blocknum_to_offset_size(bfe->h->blocktable, blocknum, &node_offset, &node_size);

    for (int i=0; i<node->n_children; i++) {
        BP_PAGE_DATA_START(*ndd,i) = rbuf_int(rb);
        BP_PAGE_DATA_SIZE (*ndd,i) = rbuf_int(rb);
    }
    node->node_info_size = rbuf_int(rb);
#endif
    // verify checksum of header stored
    uint32_t checksum;
    checksum = x1764_memory(rb->buf, rb->ndone);
    uint32_t stored_checksum;
    stored_checksum = rbuf_int(rb);
    if (stored_checksum != checksum) {
        dump_bad_block(rb->buf, rb->size);
        invariant(stored_checksum == checksum);
    }

    // now we read and decompress the pivot and child information
    sub_block_init(&sb_node_info);
#ifdef FT_INDIRECT
    get_node_info_from_rbuf(&sb_node_info, rb, node->node_info_size);
#else

    {
        tokutime_t sb_decompress_t0 = toku_time_now();
        r = read_and_decompress_sub_block(rb, &sb_node_info);
        tokutime_t sb_decompress_t1 = toku_time_now();
        decompress_time += sb_decompress_t1 - sb_decompress_t0;
    }
    if (r != 0) {
        assert(false);
        goto cleanup;
    }
#endif
    // at this point, sb->uncompressed_ptr stores the serialized node info
    r = deserialize_ftnode_info(&sb_node_info, node);
    if (r != 0) {
        assert(false);
        goto cleanup;
    }
#ifndef FT_INDIRECT
    sb_free_sized(sb_node_info.uncompressed_ptr, sb_node_info.uncompressed_size);
#endif
    // now that the node info has been deserialized, we can proceed to deserialize
    // the individual sub blocks
    paranoid_invariant(is_valid_ftnode_fetch_type(bfe->type));

    // setup the memory of the partitions
    // for partitions being decompressed, create either FIFO or basement node
    // for partitions staying compressed, create sub_block
    setup_ftnode_partitions(node, bfe, true);
    // This loop is parallelizeable, since we don't have a dependency on the work done so far.
    for (int i = 0; i < node->n_children; i++) {
        uint32_t curr_offset = BP_START(*ndd,i);
        uint32_t curr_size   = BP_SIZE(*ndd,i);
        // the compressed, serialized partitions start at where rb is currently pointing,
        // which would be rb->buf + rb->ndone
        // we need to intialize curr_rbuf to point to this place
        struct rbuf curr_rbuf  = {.buf = NULL, .size = 0, .ndone = 0};
        rbuf_init(&curr_rbuf, rb->buf + curr_offset, curr_size);

        //
        // now we are at the point where we have:
        //  - read the entire compressed node off of disk,
        //  - decompressed the pivot and offset information,
        //  - have arrived at the individual partitions.
        //
        // Based on the information in bfe, we want to decompress a subset of
        // of the compressed partitions (also possibly none or possibly all)
        // The partitions that we want to decompress and make available
        // to the node, we do, the rest we simply copy in compressed
        // form into the node, and set the state of the partition to PT_COMPRESSED
        //

        struct sub_block curr_sb ;
        sub_block_init(&curr_sb);
        // curr_rbuf is passed by value to decompress_and_deserialize_worker, so there's no ugly race condition.
        // This would be more obvious if curr_rbuf were an array.

        // deserialize_ftnode_info figures out what the state
        // should be and sets up the memory so that we are ready to use it

        switch (BP_STATE(node,i)) {
        case PT_AVAIL: {
#ifdef FT_INDIRECT
                (void)decompress_and_deserialize_worker; // pacify compiler
                if (!toku_need_compression()) {
                    int page_size = toku_read_bp_page(node, *ndd, i, &sb[i], node_offset, fd);
                    total_page_size += page_size;
                    curr_sb = sb[i];
                    curr_sb.uncompressed_ptr = curr_rbuf.buf;
                    curr_sb.uncompressed_size = curr_size;
                } else {
                    // decompress data and copy data
                    int page_size = 0;
                    ft_ind_decompress_sub_block(node, &curr_rbuf, &sb[i], &page_size);
                    total_page_size += page_size;
                    curr_sb = sb[i];
                }
                r = deserialize_ftnode_partition(&curr_sb, node, i, &bfe->h->cmp_descriptor, bfe->h->key_ops.keycmp);
                if (curr_sb.ind_data && curr_sb.ind_data->pfns) {
                    toku_free(curr_sb.ind_data->pfns);
                }
                if (curr_sb.ind_data) {
                    toku_free(curr_sb.ind_data);
                }
#else
                //  case where we read and decompress the partition
                tokutime_t partition_decompress_time;
                r = decompress_and_deserialize_worker(curr_rbuf, curr_sb, node, i,
                        &bfe->h->cmp_descriptor, bfe->h->key_ops.keycmp, &partition_decompress_time);
                decompress_time += partition_decompress_time;
#endif
                if (r != 0) {
                    assert(false);
                    goto cleanup;
                }
                break;
            }
        case PT_COMPRESSED:
            // case where we leave the partition in the compressed state
            r = check_and_copy_compressed_sub_block_worker(curr_rbuf, curr_sb, node, i);
            if (r != 0) {
                assert(false);
                goto cleanup;
            }
            break;
        case PT_INVALID: // this is really bad
        case PT_ON_DISK: // it's supposed to be in memory.
            abort();
        }
    }
    *ftnode = node;
    r = 0;

cleanup:
    if (r == 0) {
        t1 = toku_time_now();
        deserialize_time = (t1 - t0) - decompress_time;
        bfe->deserialize_time += deserialize_time;
        bfe->decompress_time += decompress_time;
        toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);
    }
    if (r != 0) {
        // NOTE: Right now, callers higher in the stack will assert on
        // failure, so this is OK for production.  However, if we
        // create tools that use this function to search for errors in
        // the BRT, then we will leak memory.
        if (node) {
            sb_free_sized(node, sizeof(*node));
        }
    }
#ifdef FT_INDIRECT
    if (sb != NULL) {
        toku_free(sb);
    }
#endif
    return r;
}

int
toku_deserialize_bp_from_disk(FTNODE node, FTNODE_DISK_DATA ndd, int childnum, int fd, struct ftnode_fetch_extra* bfe) {
    int r = 0;
    assert(BP_STATE(node,childnum) == PT_ON_DISK);
    assert(node->bp[childnum].ptr.tag == BCT_NULL);

    //
    // setup the partition
    //
    setup_available_ftnode_partition(node, childnum);
    BP_STATE(node,childnum) = PT_AVAIL;

    //
    // read off disk and make available in memory
    //
    // get the file offset and block size for the block
    DISKOFF node_offset, total_node_disk_size;
    toku_translate_blocknum_to_offset_size(
        bfe->h->blocktable,
        node->thisnodename,
        &node_offset,
        &total_node_disk_size
        );

    uint8_t *raw_block;
    ssize_t rlen = 0;
    uint32_t curr_offset = BP_START(ndd, childnum);
    uint32_t curr_size   = BP_SIZE (ndd, childnum);
    struct rbuf rb = {.buf = NULL, .size = 0, .ndone = 0};
    uint32_t pad_at_beginning = (node_offset+curr_offset)%BLOCK_ALIGNMENT;
    uint32_t padded_size = roundup_to_multiple(BLOCK_ALIGNMENT, pad_at_beginning + curr_size);

    int page_data_len = 0; // page_sharing specific
    struct sub_block curr_sb;
    sub_block_init(&curr_sb);

    tokutime_t t0 = toku_time_now();
    if (!BP_PREFETCH_FLAG(node, childnum)) {
        // XXX: Revisit whether alignment is really needed, or if this is always of a size
        //      to use our cache - aligned allocation is somewhat expensive in the current southbound
        raw_block = (uint8_t *) toku_xmalloc_aligned(BLOCK_ALIGNMENT, padded_size * sizeof(uint8_t));
        rbuf_init(&rb, pad_at_beginning+raw_block, curr_size);

        // read the block
        assert(0==((unsigned long long)raw_block)%BLOCK_ALIGNMENT); // for O_DIRECT
        assert(0==(padded_size)%BLOCK_ALIGNMENT);
        assert(0==(node_offset+curr_offset-pad_at_beginning)%BLOCK_ALIGNMENT);
        rlen = toku_os_pread(fd, raw_block, padded_size, node_offset+curr_offset-pad_at_beginning);
        assert((DISKOFF)rlen >= pad_at_beginning + curr_size); // we read in at least enough to get what we wanted
        assert((DISKOFF)rlen <= padded_size);                  // we didn't read in too much.
#ifdef FT_INDIRECT
        if (!toku_need_compression()) {
            page_data_len = toku_read_bp_page(node, ndd, childnum, &curr_sb, node_offset, fd);
        }
#endif
    } else {
#ifdef FT_INDIRECT // bn prefetching for page sharing
        unsigned long *pfns = BP_PREFETCH_PFN(node, childnum);
        int aligned_index_size = roundup_to_multiple(BLOCK_ALIGNMENT, curr_size);
        int nr_index_pages = aligned_index_size / BLOCK_ALIGNMENT;

        for (int i = 0; i < nr_index_pages; i++) {
            sb_wait_read_page(pfns[i]);
        }

        raw_block = BP_PREFETCH_BUF(node, childnum);
        rbuf_init(&rb, pad_at_beginning+raw_block, curr_size);

        uint32_t nr_pages = BP_PAGE_DATA_SIZE(ndd, childnum) >> FTFS_PAGE_SIZE_SHIFT;
        if (nr_pages > 0) {
            assert(BP_PREFETCH_PFN_CNT(node, childnum) - nr_index_pages == nr_pages);
            XMALLOC_N(1, curr_sb.ind_data);
            curr_sb.ind_data->pfn_cnt = nr_pages;
            XMALLOC_N(nr_pages, curr_sb.ind_data->pfns);
            for (int i = nr_index_pages; i < BP_PREFETCH_PFN_CNT(node, childnum); i++) {
                curr_sb.ind_data->pfns[i-nr_index_pages] = pfns[i];
            }
        }
        page_data_len = BP_PAGE_DATA_SIZE(ndd, childnum);
        rlen = aligned_index_size;
#else
        int nr_pages = BP_PREFETCH_PFN_CNT(node, childnum);
        unsigned long *pfns = BP_PREFETCH_PFN(node, childnum);
        if (padded_size != (uint32_t)nr_pages * BLOCK_ALIGNMENT) {
            printf("padded_size=%d, nr_pages=%d\n", padded_size, nr_pages);
            assert(false);
        }

        for (int p = 0; p < nr_pages; p++) {
            sb_wait_read_page(pfns[p]);
        }

        raw_block = BP_PREFETCH_BUF(node, childnum);
        rbuf_init(&rb, pad_at_beginning+raw_block, curr_size);
        rlen = padded_size;
#endif
        toku_free(BP_PREFETCH_PFN(node, childnum));
        BP_PREFETCH_PFN(node, childnum) = NULL;
        BP_PREFETCH_PFN_CNT(node, childnum) = 0;
        BP_PREFETCH_FLAG(node, childnum) = false;
        BP_PREFETCH_BUF(node, childnum) = NULL;
    }
    tokutime_t t1 = toku_time_now();

#ifdef FT_INDIRECT
    if (!toku_need_compression()) {
        // read page data
        curr_sb.uncompressed_size = curr_size;
        curr_sb.uncompressed_ptr = rb.buf;
    } else {
        // decompress data and copy data
        page_data_len = 0;
        ft_ind_decompress_sub_block(node, &rb, &curr_sb, &page_data_len);
    }

    if (page_data_len) {
        assert(curr_sb.ind_data);
        assert(curr_sb.ind_data->pfns);
    } else {
        assert(curr_sb.ind_data == NULL);
    }
#else
    // decompress
    r = read_and_decompress_sub_block(&rb, &curr_sb);
#endif
    // deserialize
    tokutime_t t2 = toku_time_now();
    if (r == 0) {
        // at this point, sb->uncompressed_ptr stores the serialized node partition
        r = deserialize_ftnode_partition(&curr_sb, node, childnum, &bfe->h->cmp_descriptor, bfe->h->key_ops.keycmp);
    }

    tokutime_t t3 = toku_time_now();

    // capture stats
    tokutime_t io_time = t1 - t0;
    tokutime_t decompress_time = t2 - t1;
    tokutime_t deserialize_time = t3 - t2;
    bfe->deserialize_time += deserialize_time;
    bfe->decompress_time += decompress_time;
    toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);
    bfe->bytes_read = rlen + page_data_len;
    bfe->io_time = io_time;
#ifdef FT_INDIRECT
    if (curr_sb.ind_data && curr_sb.ind_data->pfns) {
        toku_free(curr_sb.ind_data->pfns);
    }
    toku_free(curr_sb.ind_data);
#endif

    // Aligned pointers should just be freed via toku_free
    toku_free(raw_block);
    return r;
}

// Take a ftnode partition that is in the compressed state, and make it avail
int
toku_deserialize_bp_from_compressed(FTNODE node, int childnum, struct ftnode_fetch_extra *bfe) {
    int r = 0;
    assert(BP_STATE(node, childnum) == PT_COMPRESSED);
    SUB_BLOCK curr_sb = BSB(node, childnum);
    assert(curr_sb->uncompressed_ptr == NULL);
#ifdef FT_INDIRECT
    setup_available_ftnode_partition(node, childnum);
    BP_STATE(node,childnum) = PT_AVAIL;

    struct rbuf rb = {.buf = (uint8_t*)curr_sb->compressed_ptr, .size = curr_sb->compressed_size, .ndone = 0};
    int page_data_len = 0;

    // decompress the sub_block
    tokutime_t t0 = toku_time_now();
    r = page_data_len = ft_ind_decompress_sub_block(node, &rb, curr_sb, &page_data_len);
    tokutime_t t1 = toku_time_now();

#else
    curr_sb->uncompressed_ptr = sb_malloc_sized(curr_sb->uncompressed_size, true);

    setup_available_ftnode_partition(node, childnum);
    BP_STATE(node,childnum) = PT_AVAIL;

    // decompress the sub_block
    tokutime_t t0 = toku_time_now();

    toku_decompress(
        (Bytef *) curr_sb->uncompressed_ptr,
        curr_sb->uncompressed_size,
        (Bytef *) curr_sb->compressed_ptr,
        curr_sb->compressed_size
        );

    tokutime_t t1 = toku_time_now();
#endif

    r = deserialize_ftnode_partition(curr_sb, node, childnum, &bfe->h->cmp_descriptor, bfe->h->key_ops.keycmp);

    tokutime_t t2 = toku_time_now();

    tokutime_t decompress_time = t1 - t0;
    tokutime_t deserialize_time = t2 - t1;
    bfe->deserialize_time += deserialize_time;
    bfe->decompress_time += decompress_time;
    toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);

#ifdef FT_INDIRECT
    if (curr_sb->ind_data && curr_sb->ind_data->pfns) {
        toku_free(curr_sb->ind_data->pfns);
    }
    if (curr_sb->ind_data) {
        toku_free(curr_sb->ind_data);
    }
#endif
    sb_free_sized(curr_sb->compressed_ptr, curr_sb->compressed_size);
    toku_free(curr_sb);
    return r;
}

static int
deserialize_ftnode_from_fd(int fd,
                            BLOCKNUM blocknum,
                            uint32_t fullhash,
                            FTNODE *ftnode,
                            FTNODE_DISK_DATA *ndd,
                            struct ftnode_fetch_extra *bfe,
                            STAT64INFO info)
{
    struct rbuf rb = RBUF_INITIALIZER;

    tokutime_t t0 = toku_time_now();
    read_block_from_fd_into_rbuf(fd, blocknum, bfe->h, &rb);
    tokutime_t t1 = toku_time_now();

    // Decompress and deserialize the ftnode. Time statistics
    // are taken inside this function.
    int r = deserialize_ftnode_from_rbuf(ftnode, ndd, blocknum, fullhash, bfe, info, &rb, fd);
    if (r != 0) {
        printf("%s: %d found bad block, let's abort\n", __func__, __LINE__);
        assert(false);
    }

    bfe->bytes_read = rb.size;
    bfe->io_time = t1 - t0;
    toku_free(rb.buf);
    return r;
}

// Read brt node from file into struct.  Perform version upgrade if necessary.
int
toku_deserialize_ftnode_from (int fd,
                               BLOCKNUM blocknum,
                               uint32_t fullhash,
                               FTNODE *ftnode,
                               FTNODE_DISK_DATA* ndd,
                               struct ftnode_fetch_extra* bfe
    )
// Effect: Read a node in.  If possible, read just the header.
{
    int r = 0;
    struct rbuf rb = RBUF_INITIALIZER;

    // each function below takes the appropriate io/decompression/deserialize statistics
    if (!bfe->read_all_partitions) {
        read_ftnode_header_from_fd_into_rbuf_if_small_enough(fd, blocknum, bfe->h, &rb, bfe);
        r = deserialize_ftnode_header_from_rbuf_if_small_enough(ftnode, ndd, blocknum, fullhash, bfe, &rb, fd);
    } else {
        // force us to do it the old way
        r = -1;
    }
    if (r != 0) {
        // Something went wrong, go back to doing it the old way.
        r = deserialize_ftnode_from_fd(fd, blocknum, fullhash, ftnode, ndd, bfe, NULL);
    }
    assert(*ndd);
    // Use ndd to cache rbuf to reduce future read
    // If rbuf is cached, then do not free the buf
    if (rb.buf) {
        toku_free(rb.buf);
    }
    return r;
}

void
toku_verify_or_set_counts(FTNODE UU(node)) {
}

int
toku_db_badformat(void) {
    return DB_BADFORMAT;
}

static size_t
serialize_rollback_log_size(ROLLBACK_LOG_NODE log) {
    size_t size = node_header_overhead //8 "tokuroll", 4 version, 4 version_original, 4 build_id
                 +16 //TXNID_PAIR
                 +8 //sequence
                 +8 //blocknum
                 +8 //previous (blocknum)
                 +8 //resident_bytecount
                 +8 //memarena_size_needed_to_load
                 +log->rollentry_resident_bytecount;
    return size;
}

static void
serialize_rollback_log_node_to_buf(ROLLBACK_LOG_NODE log, char *buf, size_t calculated_size, int UU(n_sub_blocks), struct sub_block UU(sub_block[])) {
    struct wbuf wb;
    wbuf_init(&wb, buf, calculated_size);
    {   //Serialize rollback log to local wbuf
        wbuf_nocrc_literal_bytes(&wb, "tokuroll", 8);
        lazy_assert(log->layout_version == FT_LAYOUT_VERSION);
        wbuf_nocrc_int(&wb, log->layout_version);
        wbuf_nocrc_int(&wb, log->layout_version_original);
        wbuf_nocrc_uint(&wb, BUILD_ID);
        wbuf_nocrc_TXNID_PAIR(&wb, log->txnid);
        wbuf_nocrc_ulonglong(&wb, log->sequence);
        wbuf_nocrc_BLOCKNUM(&wb, log->blocknum);
        wbuf_nocrc_BLOCKNUM(&wb, log->previous);
        wbuf_nocrc_ulonglong(&wb, log->rollentry_resident_bytecount);
        //Write down memarena size needed to restore
        wbuf_nocrc_ulonglong(&wb, memarena_total_size_in_use(log->rollentry_arena));

        {
            //Store rollback logs
            struct roll_entry *item;
            size_t done_before = wb.ndone;
            for (item = log->newest_logentry; item; item = item->prev) {
                toku_logger_rollback_wbuf_nocrc_write(&wb, item);
            }
            lazy_assert(done_before + log->rollentry_resident_bytecount == wb.ndone);
        }
    }
    lazy_assert(wb.ndone == wb.size);
    lazy_assert(calculated_size==wb.ndone);
}

static void
serialize_uncompressed_block_to_memory(char * uncompressed_buf,
                                       int n_sub_blocks,
                                       struct sub_block sub_block[/*n_sub_blocks*/],
                                       enum toku_compression_method method,
                               /*out*/ size_t *n_bytes_to_write,
                               /*out*/ char  **bytes_to_write)
// Guarantees that the malloc'd BYTES_TO_WRITE is 4096-byte aligned (so that O_DIRECT will work)
{
    // allocate space for the compressed uncompressed_buf
    size_t compressed_len = get_sum_compressed_size_bound(n_sub_blocks, sub_block, method);
    size_t sub_block_header_len = sub_block_header_size(n_sub_blocks);
    size_t header_len = node_header_overhead + sub_block_header_len + sizeof (uint32_t); // node + sub_block + checksum
    char *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, roundup_to_multiple(BLOCK_ALIGNMENT, header_len + compressed_len), compressed_buf);

    // copy the header
    memcpy(compressed_buf, uncompressed_buf, node_header_overhead);
    if (0) printf("First 4 bytes before compressing data are %02x%02x%02x%02x\n",
                  uncompressed_buf[node_header_overhead],   uncompressed_buf[node_header_overhead+1],
                  uncompressed_buf[node_header_overhead+2], uncompressed_buf[node_header_overhead+3]);

    // compress all of the sub blocks
    char *uncompressed_ptr = uncompressed_buf + node_header_overhead;
    char *compressed_ptr = compressed_buf + header_len;
    compressed_len = compress_all_sub_blocks(n_sub_blocks, sub_block, uncompressed_ptr, compressed_ptr, num_cores, ft_pool, method);

    //if (0) printf("Block %" PRId64 " Size before compressing %u, after compression %" PRIu64 "\n", blocknum.b, calculated_size-node_header_overhead, (uint64_t) compressed_len);

    // serialize the sub block header
    uint32_t *ptr = (uint32_t *)(compressed_buf + node_header_overhead);
    *ptr++ = toku_htod32(n_sub_blocks);
    for (int i=0; i<n_sub_blocks; i++) {
        ptr[0] = toku_htod32(sub_block[i].compressed_size);
        ptr[1] = toku_htod32(sub_block[i].uncompressed_size);
        ptr[2] = toku_htod32(sub_block[i].xsum);
        ptr += 3;
    }

    // compute the header checksum and serialize it
    uint32_t header_length = (char *)ptr - (char *)compressed_buf;
    uint32_t xsum = x1764_memory(compressed_buf, header_length);
    *ptr = toku_htod32(xsum);

    uint32_t padded_len = roundup_to_multiple(BLOCK_ALIGNMENT, header_len + compressed_len);
    // Zero out padding.
    for (uint32_t i = header_len+compressed_len; i < padded_len; i++) {
        compressed_buf[i] = 0;
    }
    *n_bytes_to_write = padded_len;
    *bytes_to_write   = compressed_buf;
}

void
toku_serialize_rollback_log_to_memory_uncompressed(ROLLBACK_LOG_NODE log, SERIALIZED_ROLLBACK_LOG_NODE serialized) {
    // get the size of the serialized node
    size_t calculated_size = serialize_rollback_log_size(log);

    serialized->len = calculated_size;
    serialized->n_sub_blocks = 0;
    // choose sub block parameters
    int sub_block_size = 0;
    size_t data_size = calculated_size - node_header_overhead;
    choose_sub_block_size(data_size, max_sub_blocks, &sub_block_size, &serialized->n_sub_blocks);
    lazy_assert(0 < serialized->n_sub_blocks && serialized->n_sub_blocks <= max_sub_blocks);
    lazy_assert(sub_block_size > 0);

    // set the initial sub block size for all of the sub blocks
    for (int i = 0; i < serialized->n_sub_blocks; i++)
        sub_block_init(&serialized->sub_block[i]);
    set_all_sub_block_sizes(data_size, sub_block_size, serialized->n_sub_blocks, serialized->sub_block);

    // allocate space for the serialized node
    serialized->data = (char *) sb_malloc_sized(calculated_size * sizeof(*(serialized->data)), true);
    // serialize the node into buf
    serialize_rollback_log_node_to_buf(log, serialized->data, calculated_size, serialized->n_sub_blocks, serialized->sub_block);
    serialized->blocknum = log->blocknum;
}

int
toku_serialize_rollback_log_to (int fd, ROLLBACK_LOG_NODE log, SERIALIZED_ROLLBACK_LOG_NODE serialized_log, bool is_serialized,
                                FT h, bool for_checkpoint) {
    size_t n_to_write;
    char *compressed_buf;
    struct serialized_rollback_log_node serialized_local;

    if (is_serialized) {
        invariant_null(log);
    } else {
        invariant_null(serialized_log);
        serialized_log = &serialized_local;
        toku_serialize_rollback_log_to_memory_uncompressed(log, serialized_log);
    }
    BLOCKNUM blocknum = serialized_log->blocknum;

    //Compress and malloc buffer to write
    serialize_uncompressed_block_to_memory(serialized_log->data,
            serialized_log->n_sub_blocks, serialized_log->sub_block,
            h->h->compression_method, &n_to_write, &compressed_buf);

    {
        lazy_assert(blocknum.b>=0);
        DISKOFF offset;
        toku_blocknum_realloc_on_disk(h->blocktable, blocknum, n_to_write, &offset,
                                      h, fd, for_checkpoint); //dirties h
        bool is_blocking;
#ifdef TOKU_LINUX_MODULE
        if (ftfs_is_hdd()) {
            is_blocking = true; /* for hdd: we use the old to write node */
        } else if (!for_checkpoint) {
            is_blocking = false; /* for evictor and partial checkpoint: we async write the ftnode to reduce dio write latency. Integrity can be guaranteed later */
        } else {
            is_blocking = true; /* for checkpointer: there is a fsync very soon, we need to wait for data to be durable any way. The benefit to async write is marginal. */
        }
#else
        is_blocking = true;
#endif
        toku_os_full_pwrite(fd, compressed_buf, n_to_write, offset, is_blocking);
        /* If is_blocking is true, we can free the buf after write returns */
        if (is_blocking) {
            toku_free(compressed_buf);
        }
	toku_ft_status_update_io_reason(FT_DISK_IO_ROLLBACK, n_to_write);
    }

    if (!is_serialized) {
        toku_static_serialized_rollback_log_destroy(&serialized_local);
        log->dirty = 0;  // See #1957.   Must set the node to be clean after serializing it so that it doesn't get written again on the next checkpoint or eviction.
    }
    return 0;
}

static int
deserialize_rollback_log_from_rbuf (BLOCKNUM blocknum, ROLLBACK_LOG_NODE *log_p, struct rbuf *rb) {
    ROLLBACK_LOG_NODE MALLOC(result);
    int r;
    if (result==NULL) {
        r=ENOMEM;
	if (0) { died0: toku_free(result); }
	return r;
    }

    //printf("Deserializing %lld datasize=%d\n", off, datasize);
    bytevec magic;
    rbuf_literal_bytes(rb, &magic, 8);
    lazy_assert(!memcmp(magic, "tokuroll", 8));

    result->layout_version    = rbuf_int(rb);
    lazy_assert(result->layout_version == FT_LAYOUT_VERSION);
    result->layout_version_original = rbuf_int(rb);
    result->layout_version_read_from_disk = result->layout_version;
    result->build_id = rbuf_int(rb);
    result->dirty = false;
    //TODO: Maybe add descriptor (or just descriptor version) here eventually?
    //TODO: This is hard.. everything is shared in a single dictionary.
    rbuf_TXNID_PAIR(rb, &result->txnid);
    result->sequence = rbuf_ulonglong(rb);
    result->blocknum = rbuf_blocknum(rb);
    if (result->blocknum.b != blocknum.b) {
        r = toku_db_badformat();
        goto died0;
    }
    result->previous       = rbuf_blocknum(rb);
    result->rollentry_resident_bytecount = rbuf_ulonglong(rb);

    size_t arena_initial_size = rbuf_ulonglong(rb);
    result->rollentry_arena = memarena_create_presized(arena_initial_size);
    if (0) { died1: memarena_close(&result->rollentry_arena); goto died0; }

    //Load rollback entries
    lazy_assert(rb->size > 4);
    //Start with empty list
    result->oldest_logentry = result->newest_logentry = NULL;
    while (rb->ndone < rb->size) {
        struct roll_entry *item;
        uint32_t rollback_fsize = rbuf_int(rb); //Already read 4.  Rest is 4 smaller
        bytevec item_vec;
        rbuf_literal_bytes(rb, &item_vec, rollback_fsize-4);
        unsigned char* item_buf = (unsigned char*)item_vec;
        r = toku_parse_rollback(item_buf, rollback_fsize-4, &item, result->rollentry_arena);
        if (r!=0) {
            r = toku_db_badformat();
            goto died1;
        }
        //Add to head of list
        if (result->oldest_logentry) {
            result->oldest_logentry->prev = item;
            result->oldest_logentry       = item;
            item->prev = NULL;
        }
        else {
            result->oldest_logentry = result->newest_logentry = item;
            item->prev = NULL;
        }
    }

    toku_free(rb->buf);
    rb->buf = NULL;
    *log_p = result;
    return 0;
}

static int
deserialize_rollback_log_from_rbuf_versioned (uint32_t version, BLOCKNUM blocknum,
                                              ROLLBACK_LOG_NODE *log,
                                              struct rbuf *rb) {
    int r = 0;
    ROLLBACK_LOG_NODE rollback_log_node = NULL;
    invariant(version==FT_LAYOUT_VERSION); //Rollback log nodes do not survive version changes.
    r = deserialize_rollback_log_from_rbuf(blocknum, &rollback_log_node, rb);
    if (r==0) {
        *log = rollback_log_node;
    }
    return r;
}

int
decompress_from_raw_block_into_rbuf(uint8_t *raw_block, size_t raw_block_size, struct rbuf *rb, BLOCKNUM blocknum) {
    int r = 0;
    // get the number of compressed sub blocks
    int n_sub_blocks;
    unsigned char *buf;

    n_sub_blocks = toku_dtoh32(*(uint32_t*)(&raw_block[node_header_overhead]));

    // verify the number of sub blocks
    invariant(0 <= n_sub_blocks);
    invariant(n_sub_blocks <= max_sub_blocks);

    { // verify the header checksum
        uint32_t header_length = node_header_overhead + sub_block_header_size(n_sub_blocks);
        invariant(header_length <= raw_block_size);
        uint32_t xsum = x1764_memory(raw_block, header_length);
        uint32_t stored_xsum = toku_dtoh32(*(uint32_t *)(raw_block + header_length));
        if (xsum != stored_xsum) {
            r = TOKUDB_BAD_CHECKSUM;
        }
    }

    // deserialize the sub block header
    struct sub_block sub_block[n_sub_blocks];
    uint32_t *sub_block_header = (uint32_t *) &raw_block[node_header_overhead+4];
    for (int i = 0; i < n_sub_blocks; i++) {
        sub_block_init(&sub_block[i]);
        sub_block[i].compressed_size = toku_dtoh32(sub_block_header[0]);
        sub_block[i].uncompressed_size = toku_dtoh32(sub_block_header[1]);
        sub_block[i].xsum = toku_dtoh32(sub_block_header[2]);
        sub_block_header += 3;
    }

    // This predicate needs to be here and instead of where it is set
    // for the compiler.
    if (r == TOKUDB_BAD_CHECKSUM) {
        goto exit;
    }

    // verify sub block sizes
    for (int i = 0; i < n_sub_blocks; i++) {
        uint32_t compressed_size = sub_block[i].compressed_size;
        if (compressed_size<=0   || compressed_size>(1<<30)) {
            r = toku_db_badformat();
            goto exit;
        }

        uint32_t uncompressed_size = sub_block[i].uncompressed_size;
        if (0) printf("Block %" PRId64 " Compressed size = %u, uncompressed size=%u\n", blocknum.b, compressed_size, uncompressed_size);
        if (uncompressed_size<=0 || uncompressed_size>(1<<30)) {
            r = toku_db_badformat();
            goto exit;
        }
    }

    // sum up the uncompressed size of the sub blocks
    size_t uncompressed_size;
    uncompressed_size = get_sum_uncompressed_size(n_sub_blocks, sub_block);

    // allocate the uncompressed buffer
    size_t size;
    size = node_header_overhead + uncompressed_size;
    buf = (unsigned char *) sb_malloc_sized(size * sizeof(*buf), true);
    rbuf_init(rb, buf, size);

    // copy the uncompressed node header to the uncompressed buffer
    memcpy(rb->buf, raw_block, node_header_overhead);

    // point at the start of the compressed data (past the node header, the sub block header, and the header checksum)
    unsigned char *compressed_data;
    compressed_data = raw_block + node_header_overhead + sub_block_header_size(n_sub_blocks) + sizeof (uint32_t);

    // point at the start of the uncompressed data
    unsigned char *uncompressed_data;
    uncompressed_data = rb->buf + node_header_overhead;

    // decompress all the compressed sub blocks into the uncompressed buffer
    r = decompress_all_sub_blocks(n_sub_blocks, sub_block, compressed_data, uncompressed_data, num_cores, ft_pool);
    if (r != 0) {
        dprintf(STDERR, "%s:%d block %" PRId64 " failed %d at %p size %lu\n", __FUNCTION__, __LINE__, blocknum.b, r, raw_block, raw_block_size);
        dump_bad_block(raw_block, raw_block_size);
        goto exit;
    }

    rb->ndone=0;
 exit:
    return r;
}

static int
decompress_from_raw_block_into_rbuf_versioned(uint32_t version, uint8_t *raw_block, size_t raw_block_size, struct rbuf *rb, BLOCKNUM blocknum) {
    // This function exists solely to accomodate future changes in compression.
    int r = 0;
    switch (version) {
        case FT_LAYOUT_VERSION_13:
        case FT_LAYOUT_VERSION_14:
        case FT_LAYOUT_VERSION:
            r = decompress_from_raw_block_into_rbuf(raw_block, raw_block_size, rb, blocknum);
            break;
        default:
            abort();
    }
    return r;
}

static int
read_and_decompress_block_from_fd_into_rbuf(int fd, BLOCKNUM blocknum,
                                            DISKOFF offset, DISKOFF size,
                                            FT h,
                                            struct rbuf *rb,
                                  /* out */ int *layout_version_p) {
    int r = 0;
    if (0) printf("Deserializing Block %" PRId64 "\n", blocknum.b);

    DISKOFF size_aligned = roundup_to_multiple(BLOCK_ALIGNMENT, size);
    uint8_t *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, size_aligned, raw_block);
    {
        // read the (partially compressed) block
        ssize_t rlen = toku_os_pread(fd, raw_block, size_aligned, offset);
        lazy_assert((DISKOFF)rlen >= size);
        lazy_assert((DISKOFF)rlen <= size_aligned);
    }
    // get the layout_version
    int layout_version;
    {
        uint8_t *magic = raw_block + uncompressed_magic_offset;
        if (memcmp(magic, "tokuleaf", 8)!=0 &&
            memcmp(magic, "tokunode", 8)!=0 &&
            memcmp(magic, "tokuroll", 8)!=0) {
            r = toku_db_badformat();
            goto cleanup;
        }
        uint8_t *version = raw_block + uncompressed_version_offset;
        layout_version = toku_dtoh32(*(uint32_t*)version);
        if (layout_version < FT_LAYOUT_MIN_SUPPORTED_VERSION || layout_version > FT_LAYOUT_VERSION) {
            r = toku_db_badformat();
            goto cleanup;
        }
    }

    r = decompress_from_raw_block_into_rbuf_versioned(layout_version, raw_block, size, rb, blocknum);
    if (r != 0) {
        // We either failed the checksome, or there is a bad format in
        // the buffer.
        if (r == TOKUDB_BAD_CHECKSUM) {
            dprintf(STDERR,
                    "Checksum failure while reading raw block in file %s.\n",
                    toku_cachefile_fname_in_env(h->cf));
            abort();
        } else {
            r = toku_db_badformat();
            goto cleanup;
        }
    }

    *layout_version_p = layout_version;
cleanup:
    if (r!=0) {
        if (rb->buf) toku_free(rb->buf);
        rb->buf = NULL;
    }
    if (raw_block) {
        toku_free(raw_block);
    }
    return r;
}

// Read rollback log node from file into struct.  Perform version upgrade if necessary.
int
toku_deserialize_rollback_log_from (int fd, BLOCKNUM blocknum, ROLLBACK_LOG_NODE *logp, FT h) {
    int layout_version = 0;
    int r;
    struct rbuf rb = {.buf = NULL, .size = 0, .ndone = 0};

    // get the file offset and block size for the block
    DISKOFF offset, size;
    toku_translate_blocknum_to_offset_size(h->blocktable, blocknum, &offset, &size);
    // if the size is 0, then the blocknum is unused
    if (size == 0) {
        // blocknum is unused, just create an empty one and get out
        ROLLBACK_LOG_NODE XMALLOC(log);
        rollback_empty_log_init(log);
        log->blocknum.b = blocknum.b;
        r = 0;
        *logp = log;
        goto cleanup;
    }

    r = read_and_decompress_block_from_fd_into_rbuf(fd, blocknum, offset, size, h, &rb, &layout_version);
    if (r!=0) goto cleanup;

    {
        uint8_t *magic = rb.buf + uncompressed_magic_offset;
        if (memcmp(magic, "tokuroll", 8)!=0) {
            r = toku_db_badformat();
            goto cleanup;
        }
    }

    r = deserialize_rollback_log_from_rbuf_versioned(layout_version, blocknum, logp, &rb);

cleanup:
    if (rb.buf) toku_free(rb.buf);
    return r;
}

int
toku_upgrade_subtree_estimates_to_stat64info(int fd, FT h)
{
    int r = 0;
    // 15 was the last version with subtree estimates
    invariant(h->layout_version_read_from_disk <= FT_LAYOUT_VERSION_15);

    FTNODE unused_node = NULL;
    FTNODE_DISK_DATA unused_ndd = NULL;
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_min_read(&bfe, h);
    r = deserialize_ftnode_from_fd(fd, h->h->root_blocknum, 0, &unused_node, &unused_ndd,
                                   &bfe, &h->h->on_disk_stats);
    h->in_memory_stats = h->h->on_disk_stats;

    if (unused_node) {
        toku_ftnode_free(&unused_node);
    }
    if (unused_ndd) {
        toku_free(unused_ndd);
    }
    return r;
}

int
toku_upgrade_msn_from_root_to_header(int fd, FT h)
{
    int r;
    // 21 was the first version with max_msn_in_ft in the header
    invariant(h->layout_version_read_from_disk <= FT_LAYOUT_VERSION_20);

    FTNODE node;
    FTNODE_DISK_DATA ndd;
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_min_read(&bfe, h);
    r = deserialize_ftnode_from_fd(fd, h->h->root_blocknum, 0, &node, &ndd, &bfe, nullptr);
    if (r != 0) {
        goto exit;
    }

    h->h->max_msn_in_ft = node->max_msn_applied_to_node_on_disk;
    toku_ftnode_free(&node);
    toku_free(ndd);
 exit:
    return r;
}

#undef UPGRADE_STATUS_VALUE
