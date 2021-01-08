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


#include <toku_portability.h>
#include "ft-internal.h"        // ugly but pragmatic, need access to dirty bits while holding translation lock
#include "fttypes.h"
#include "block_table.h"
#include "memory.h"
#include "toku_assert.h"
#include <toku_pthread.h>
#include "block_allocator.h"
#include "rbuf.h"
#include "wbuf.h"
#include <util/nb_mutex.h>
#include <util/growable_array.h>

//When the translation (btt) is stored on disk:
//  In Header:
//      size_on_disk
//      location_on_disk
//  In block translation table (in order):
//      smallest_never_used_blocknum
//      blocknum_freelist_head
//      array
//      a checksum

//This is the BTT (block translation table)
struct translation {
    enum translation_type type;
    // Number of elements in array (block_translation).
    //   always >= smallest_never_used_blocknum
    int64_t length_of_array;
    BLOCKNUM smallest_never_used_blocknum;
    // next (previously used) unused blocknum (free list)
    BLOCKNUM blocknum_freelist_head;
    struct block_translation_pair *block_translation;
    toku::GrowableArray<uint64_t> log_block_log;
    // Where and how big is the block translation vector stored on disk.
    // size_on_disk is stored in block_translation[RESERVED_BLOCKNUM_TRANSLATION].size
    // location_on is stored in block_translation[RESERVED_BLOCKNUM_TRANSLATION].u.diskoff
};

// in a freelist, this indicates end of list
static const BLOCKNUM freelist_null  = {-1};
// value of block_translation_pair.size if blocknum is unused
static const DISKOFF  size_is_free   = (DISKOFF)-1;
// value of block_translation_pair.u.diskoff if blocknum is used but does not yet have a diskblock
static const DISKOFF  diskoff_unused = (DISKOFF)-2;

/********
 *  There are three copies of the translation table (btt) in the block table:
 *
 *    checkpointed   Is initialized by deserializing from disk,
 *                   and is the only version ever read from disk.
 *                   When read from disk it is copied to current.
 *                   It is immutable. It can be replaced by an inprogress btt.
 *
 *    inprogress     Is only filled by copying from current,
 *                   and is the only version ever serialized to disk.
 *                   (It is serialized to disk on checkpoint and clean shutdown.)
 *                   At end of checkpoint it replaces 'checkpointed'.
 *                   During a checkpoint, any 'pending' dirty writes will update
 *                   inprogress.
 *
 *    current        Is initialized by copying from checkpointed,
 *                   is the only version ever modified while the database is in use,
 *                   and is the only version ever copied to inprogress.
 *                   It is never stored on disk.
 ********/


struct block_table {
    // The current translation is the one used by client threads.
    //   It is not represented on disk.
    struct translation current;
    // the translation used by the checkpoint currently in progress.
    //   If the checkpoint thread allocates a block,
    //     it must also update the current translation.
    struct translation inprogress;
    // the translation for the data that shall remain inviolate on disk until
    //   the next checkpoint finishes, after which any blocks used only in this
    //   translation can be freed.
    struct translation checkpointed;

    // The in-memory data structure for block allocation.
    // There is no on-disk data structure for block allocation.
    // Note: This is *allocation* not *translation*.
    // The block_allocator is unaware of which blocks are used for which
    //   translation, but simply allocates and deallocates blocks.
    BLOCK_ALLOCATOR block_allocator;
    toku_mutex_t mutex;
    struct nb_mutex safe_file_size_lock;
    bool checkpoint_skipped;
    uint64_t safe_file_size;
};

static inline uint64_t
calculate_size_on_disk (struct translation *t)
{
    uint64_t r = (8 + // smallest_never_used_blocknum
                  8 + // blocknum_freelist_head
                  t->smallest_never_used_blocknum.b * sizeof(struct block_translation_pair) + // Array
                  4); // 4 for checksum
    return r;
}

// We cannot free the disk space allocated to this blocknum if it is still in use by the given translation table.
static inline bool
translation_prevents_freeing(struct translation *t, BLOCKNUM b,
                             struct block_translation_pair *old_pair)
{
    return (t->block_translation &&
            b.b < t->smallest_never_used_blocknum.b &&
            old_pair->u.diskoff == t->block_translation[b.b].u.diskoff);
}

static inline void
blocktable_lock_init(BLOCK_TABLE bt)
{
    memset(&bt->mutex, 0, sizeof(bt->mutex));
    toku_mutex_init(&bt->mutex, NULL);
}

static inline void
blocktable_lock_destroy(BLOCK_TABLE bt)
{
    toku_mutex_destroy(&bt->mutex);
}

static inline void
lock_for_blocktable(BLOCK_TABLE bt)
{
    // Locks the blocktable_mutex.
    toku_mutex_lock(&bt->mutex);
}

static inline void
unlock_for_blocktable(BLOCK_TABLE bt)
{
    toku_mutex_unlock(&bt->mutex);
}

static inline void
assert_blocktable_locked(BLOCK_TABLE bt)
{
    toku_mutex_assert_locked(&bt->mutex);
}

static void
ft_set_dirty(FT ft, bool for_checkpoint)
{
    assert_blocktable_locked(ft->blocktable);
    paranoid_invariant(ft->h->type == FT_CURRENT);
    if (for_checkpoint) {
        paranoid_invariant(ft->checkpoint_header->type == FT_CHECKPOINT_INPROGRESS);
        ft->checkpoint_header->dirty = 1;
    }
    else {
        ft->h->dirty = 1;
    }
}

static void update_safe_file_size(int fd, BLOCK_TABLE bt) {
    int64_t file_size;
    int r = toku_os_get_file_size(fd, &file_size);
    lazy_assert_zero(r);
    bt->safe_file_size = file_size;
}

static void
maybe_truncate_file(BLOCK_TABLE bt, int fd, uint64_t size_needed_before)
{
    assert_blocktable_locked(bt);
    uint64_t new_size_needed = block_allocator_allocated_limit(bt->block_allocator);
    //Save a call to toku_os_get_file_size (kernel call) if unlikely to be useful.
    update_safe_file_size(fd, bt);
    if (new_size_needed < size_needed_before && new_size_needed < bt->safe_file_size) {
        nb_mutex_lock(&bt->safe_file_size_lock, &bt->mutex);

        // Must hold safe_file_size_lock to change safe_file_size.
        if (new_size_needed < bt->safe_file_size) {
            uint64_t safe_file_size_before = bt->safe_file_size;
            // Not safe to use the 'to-be-truncated' portion until truncate is done.
            bt->safe_file_size = new_size_needed;
            unlock_for_blocktable(bt);

            uint64_t size_after;
            toku_maybe_truncate_file(fd, new_size_needed, safe_file_size_before, &size_after);
            lock_for_blocktable(bt);

            bt->safe_file_size = size_after;
        }
        nb_mutex_unlock(&bt->safe_file_size_lock);
    }
}

void
toku_maybe_truncate_file_on_open(BLOCK_TABLE bt, int fd)
{
    lock_for_blocktable(bt);
    maybe_truncate_file(bt, fd, bt->safe_file_size);
    unlock_for_blocktable(bt);
}

static void
copy_translation(struct translation *dst, struct translation *src,
                 enum translation_type newtype)
{
    paranoid_invariant(src->length_of_array >= src->smallest_never_used_blocknum.b);
    paranoid_invariant((newtype == TRANSLATION_DEBUG) ||
                       (src->type == TRANSLATION_CURRENT      && newtype == TRANSLATION_INPROGRESS) ||
                       (src->type == TRANSLATION_CHECKPOINTED && newtype == TRANSLATION_CURRENT));
    dst->type                         = newtype;
    dst->smallest_never_used_blocknum = src->smallest_never_used_blocknum;
    dst->blocknum_freelist_head       = src->blocknum_freelist_head;
    // destination btt is of fixed size.  Allocate+memcpy the exact length necessary.
    dst->length_of_array              = dst->smallest_never_used_blocknum.b;
    dst->block_translation            = (struct block_translation_pair *)
    sb_malloc_sized(dst->length_of_array * sizeof(struct block_translation_pair), true);
    memcpy(dst->block_translation,
           src->block_translation,
           dst->length_of_array * sizeof(*dst->block_translation));
    if (src->type == TRANSLATION_CHECKPOINTED) {
        dst->log_block_log.init();
    } else {
        // copy log_block log
        size_t s = src->log_block_log.get_size();
        dst->log_block_log.init();
        for (uint32_t i = 0; i < s; i++) {
            dst->log_block_log.push(src->log_block_log.fetch_unchecked(i));
        }
    }
    // New version of btt is not yet stored on disk.
    dst->block_translation[RESERVED_BLOCKNUM_TRANSLATION].u.diskoff = diskoff_unused;
    dst->block_translation[RESERVED_BLOCKNUM_TRANSLATION].size      = 0;
    dst->block_translation[RESERVED_BLOCKNUM_TRANSLATION].refc      = 1;
}

int64_t
toku_block_get_blocks_in_use_unlocked(BLOCK_TABLE bt)
{
    BLOCKNUM b;
    struct translation *t = &bt->current;
    int64_t num_blocks = 0;
    {
        //Reserved blocknums do not get upgraded; They are part of the header.
        for (b.b = RESERVED_BLOCKNUMS; b.b < t->smallest_never_used_blocknum.b; b.b++) {
            if (t->block_translation[b.b].size != size_is_free) {
                num_blocks++;
            }
        }
    }
    return num_blocks;
}

int64_t toku_get_largest_used_size(BLOCK_TABLE bt) {
    struct translation *t = &bt->checkpointed;
    BLOCKNUM b;
    int64_t largest = 0;
    int64_t offset = 0;
    int64_t size = 0;

    for (b.b = RESERVED_BLOCKNUMS; b.b < t->smallest_never_used_blocknum.b; b.b++) {
        offset = t->block_translation[b.b].u.diskoff;
        size = t->block_translation[b.b].size;
        if (offset + size > largest) {
            largest = offset + size;
        }
    }
    b = make_blocknum(RESERVED_BLOCKNUM_TRANSLATION);
    offset = t->block_translation[b.b].u.diskoff;
    size = t->block_translation[b.b].size;
    if (offset + size > largest) {
       largest = offset + size;
    }
    return largest;
}

static void
maybe_optimize_translation(struct translation *t)
{
    // Reduce 'smallest_never_used_blocknum.b' (completely free blocknums instead
    //   of just on a free list.  Doing so requires us to regenerate the
    //   free list.
    // This is O(n) work, so do it only if you're already doing that.
    paranoid_invariant(t->smallest_never_used_blocknum.b >= RESERVED_BLOCKNUMS);
    //Calculate how large the free suffix is.
    BLOCKNUM b;
    int64_t freed;
    {
        for (b.b = t->smallest_never_used_blocknum.b; b.b > RESERVED_BLOCKNUMS; b.b--) {
            if (t->block_translation[b.b - 1].size != size_is_free) {
                break;
            }
        }
        freed = t->smallest_never_used_blocknum.b - b.b;
    }
    if (freed > 0) {
        t->smallest_never_used_blocknum.b = b.b;
        if (t->length_of_array / 4 > t->smallest_never_used_blocknum.b) {
            // We're using more memory than necessary to represent this now.
            // Reduce.
            uint64_t new_length = t->smallest_never_used_blocknum.b * 2;
            XREALLOC_N(t->length_of_array, new_length, t->block_translation);
            t->length_of_array = new_length;
            // No need to zero anything out.
        }

        //Regenerate free list.
        t->blocknum_freelist_head.b = freelist_null.b;
        for (b.b = RESERVED_BLOCKNUMS; b.b < t->smallest_never_used_blocknum.b; b.b++) {
            if (t->block_translation[b.b].size == size_is_free) {
                t->block_translation[b.b].u.next_free_blocknum = t->blocknum_freelist_head;
                t->blocknum_freelist_head                      = b;
            }
        }
    }
}

// block table must be locked by caller of this function
void
toku_block_translation_note_start_checkpoint_unlocked(BLOCK_TABLE bt)
{
    assert_blocktable_locked(bt);
    // Copy current translation to inprogress translation.
    paranoid_invariant(bt->inprogress.block_translation == NULL);
    // We're going to do O(n) work to copy the translation, so we
    // can afford to do O(n) work by optimizing the translation
    maybe_optimize_translation(&bt->current);
    copy_translation(&bt->inprogress, &bt->current, TRANSLATION_INPROGRESS);

    bt->checkpoint_skipped = false;
}

//#define PRNTF(str, b, siz, ad, bt) printf("%s[%d] %s %" PRId64 " %" PRId64 " %" PRId64 "\n", __FUNCTION__, __LINE__, str, b, siz, ad); fflush(stdout); if (bt) block_allocator_validate(((BLOCK_TABLE)(bt))->block_allocator);
//Debugging function
#define PRNTF(str, b, siz, ad, bt)

void toku_block_translation_note_skipped_checkpoint(BLOCK_TABLE bt)
{
    // Purpose: alert block translation that the checkpoint was skipped,
    //   e.x. for a non-dirty header
    lock_for_blocktable(bt);
    paranoid_invariant_notnull(bt->inprogress.block_translation);
    bt->checkpoint_skipped = true;
    unlock_for_blocktable(bt);
}

void block_table_log_get_block(BLOCK_TABLE bt, uint64_t offset)
{
    lock_for_blocktable(bt);
    block_allocator_get_block(bt->block_allocator, offset);
    bt->current.log_block_log.push(offset);
    unlock_for_blocktable(bt);
    //does not matter for *get* multiple times -- put will just do as many times.
}

static void
block_table_log_block_log_put_blocks(BLOCK_ALLOCATOR ba, LBL lbl)
{
    size_t size = lbl.get_size();
    for (uint32_t i = 0; i < size; i++) {
        block_allocator_put_block(ba, lbl.fetch_unchecked(i));
    }
}

static void dealias_log_block_log(LBL *thaw, LBL *frozen)
{
    size_t thaw_size = thaw->get_size();
    size_t frozen_size = frozen->get_size();
    if (thaw_size == frozen_size) {
        thaw->deinit();
        thaw->init();
        return;
    } else {
        paranoid_invariant(thaw_size > frozen_size);
        LBL temp;
        temp.init();
        for (uint32_t i = 0; i < thaw_size; i++) {
            temp.push(thaw->fetch_unchecked(i));
        }
        thaw->deinit();
        thaw->init();

        for(uint32_t i = frozen_size; i < thaw_size; i++) {
            thaw->push(temp.fetch_unchecked(i));
        }
        temp.deinit();
        return;
    }
}
// Purpose: free any disk space used by previous checkpoint that isn't in use by either
//
//
//           - current state
//           - in-progress checkpoint
//          capture inprogress as new checkpointed.
// For each entry in checkpointBTT
//   if offset does not match offset in inprogress
//      assert offset does not match offset in current
//      free (offset,len) from checkpoint
// move inprogress to checkpoint (resetting type)
// inprogress = NULL
void
toku_block_translation_note_end_checkpoint (BLOCK_TABLE bt, int UU(fd)) {
    // Free unused blocks
    lock_for_blocktable(bt);
    paranoid_invariant_notnull(bt->inprogress.block_translation);
    if (bt->checkpoint_skipped) {
        sb_free_sized(bt->inprogress.block_translation, bt->inprogress.length_of_array * sizeof(struct block_translation_pair));
        memset(&bt->inprogress, 0, sizeof(bt->inprogress));
        goto end;
    }

    //Make certain inprogress was allocated space on disk
    //
    assert(bt->inprogress.block_translation[RESERVED_BLOCKNUM_TRANSLATION].size > 0);
    assert(bt->inprogress.block_translation[RESERVED_BLOCKNUM_TRANSLATION].u.diskoff > 0);
    assert(bt->inprogress.block_translation[RESERVED_BLOCKNUM_TRANSLATION].refc == 1);

    {
        int64_t i;
        struct translation *t = &bt->checkpointed;

        for (i = 0; i < t->length_of_array; i++) {
            struct block_translation_pair *pair = &t->block_translation[i];
            if (pair->size > 0 && !translation_prevents_freeing(&bt->inprogress, make_blocknum(i), pair)) {
                assert(!translation_prevents_freeing(&bt->current, make_blocknum(i), pair));
                PRNTF("free", i, pair->size, pair->u.diskoff, bt);
                block_allocator_put_block(bt->block_allocator, pair->u.diskoff);
            }
        }
        sb_free_sized(bt->checkpointed.block_translation, bt->checkpointed.length_of_array * sizeof(struct block_translation_pair));
        bt->checkpointed = bt->inprogress;
        bt->checkpointed.type = TRANSLATION_CHECKPOINTED;
        memset(&bt->inprogress, 0, sizeof(bt->inprogress));
        //be noted the bt->checkpointed.log_block_log is the old inprogress lbl, aka, the frozen blb.
        block_table_log_block_log_put_blocks(bt->block_allocator, bt->checkpointed.log_block_log);
        // dealias the current lbl
        dealias_log_block_log(&bt->current.log_block_log, &bt->checkpointed.log_block_log);

        bt->checkpointed.log_block_log.deinit();
    }
end:
    unlock_for_blocktable(bt);
}

__attribute__((nonnull,const))
static inline bool
is_valid_blocknum(struct translation *t, BLOCKNUM b)
{
    //Sanity check: Verify invariant
    paranoid_invariant(t->length_of_array >= t->smallest_never_used_blocknum.b);
    return b.b >= 0 && b.b < t->smallest_never_used_blocknum.b;
}

static inline void
verify_valid_blocknum(struct translation *t, BLOCKNUM b)
{
    paranoid_invariant(is_valid_blocknum(t, b));
}

__attribute__((nonnull,const))
static inline bool
is_valid_freeable_blocknum(struct translation *t, BLOCKNUM b)
{
    //Sanity check: Verify invariant
    paranoid_invariant(t->length_of_array >= t->smallest_never_used_blocknum.b);
    return b.b >= RESERVED_BLOCKNUMS && b.b < t->smallest_never_used_blocknum.b;
}

//Can be freed
static inline void
verify_valid_freeable_blocknum (struct translation *t, BLOCKNUM b)
{
    paranoid_invariant(is_valid_freeable_blocknum(t, b));
}

void
toku_ft_lock(FT ft)
{
    BLOCK_TABLE bt = ft->blocktable;
    lock_for_blocktable(bt);
}

void
toku_ft_unlock(FT ft)
{
    BLOCK_TABLE bt = ft->blocktable;
    assert_blocktable_locked(bt);
    unlock_for_blocktable(bt);
}

// Also used only in brt-serialize-test.
void
toku_block_free(BLOCK_TABLE bt, uint64_t offset)
{
    lock_for_blocktable(bt);
    PRNTF("freeSOMETHINGunknown", 0L, 0L, offset, bt);
    block_allocator_put_block(bt->block_allocator, offset);
    unlock_for_blocktable(bt);
}

static void
blocknum_realloc_on_disk_internal(BLOCK_TABLE bt, BLOCKNUM b,
                                  DISKOFF size, DISKOFF *offset,
                                  FT ft, bool for_checkpoint)
{
    assert_blocktable_locked(bt);
    ft_set_dirty(ft, for_checkpoint);

    struct translation *t = &bt->current;
    struct block_translation_pair old_pair = t->block_translation[b.b];
    PRNTF("old", b.b, old_pair.size, old_pair.u.diskoff, bt);
    // free the old block if it is not still in use by the checkpoint in progress or the previous checkpoint
    bool cannot_free = (bool)
        ((!for_checkpoint && translation_prevents_freeing(&bt->inprogress, b, &old_pair)) ||
         translation_prevents_freeing(&bt->checkpointed, b, &old_pair));
    if (!cannot_free && old_pair.u.diskoff != diskoff_unused) {
        PRNTF("Freed", b.b, old_pair.size, old_pair.u.diskoff, bt);
        // block_allocator_free_block(bt->block_allocator, old_pair.u.diskoff);
        block_allocator_put_block(bt->block_allocator, old_pair.u.diskoff);
    }

    uint64_t allocator_offset = diskoff_unused;
    if (size > 0) {
        // allocate a new block if the size is greater than 0.
        // if the size is just 0, offset will be set to diskoff_unused
        block_allocator_alloc_and_get_block(bt->block_allocator, size, &allocator_offset);
    }
    t->block_translation[b.b].size = size;
    t->block_translation[b.b].u.diskoff = allocator_offset;
    *offset = allocator_offset;

    PRNTF("New", b.b, t->block_translation[b.b].sizer, t->block_translation[b.b].u.diskoff, bt);
    // update in-progress btt if appropriate (if called because Pending bit is set).
    if (for_checkpoint) {
        paranoid_invariant(b.b < bt->inprogress.length_of_array);
        bt->inprogress.block_translation[b.b] = t->block_translation[b.b];
    }
}

static void
ensure_safe_write_unlocked(BLOCK_TABLE bt, int fd,
                           DISKOFF block_size, DISKOFF block_offset)
{
    // Requires: holding bt->mutex
    uint64_t size_needed = block_size + block_offset;
    if (size_needed > bt->safe_file_size) {
        update_safe_file_size(fd, bt);
        if (block_size == 0) {
            assert(block_offset == diskoff_unused);
        } else if (block_size != 0 && bt->safe_file_size < size_needed) {
            printf("%s:safe_file_size=%lu, size_needed=%lu\n", __func__, bt->safe_file_size, size_needed);
            // YZJ: consider return ENOSPC in the future
            assert(false);
        }
    }
}

void
toku_blocknum_realloc_on_disk(BLOCK_TABLE bt, BLOCKNUM b,
                              DISKOFF size, DISKOFF *offset,
                              FT ft, int fd, bool for_checkpoint)
{
    lock_for_blocktable(bt);
    struct translation *t = &bt->current;
    verify_valid_freeable_blocknum(t, b);
    blocknum_realloc_on_disk_internal(bt, b, size, offset, ft, for_checkpoint);

    ensure_safe_write_unlocked(bt, fd, size, *offset);
    unlock_for_blocktable(bt);
}

__attribute__((nonnull,const))
static inline bool
pair_is_unallocated(struct block_translation_pair *pair)
{
    return pair->size == 0 && pair->u.diskoff == diskoff_unused;
}

static void blocknum_alloc_translation_on_disk_unlocked(BLOCK_TABLE bt)
// Effect: figure out where to put the inprogress btt on disk, allocate space for it there.
//   The space must be 4096-byte aligned (both the starting address and the size).
//   As a result, the allcoated space may be a little bit bigger (up to the next 4096-byte boundary) than the actual btt.
{
    assert_blocktable_locked(bt);

    struct translation *t = &bt->inprogress;
    paranoid_invariant_notnull(t->block_translation);
    BLOCKNUM b = make_blocknum(RESERVED_BLOCKNUM_TRANSLATION);
    //Each inprogress is allocated only once
    paranoid_invariant(pair_is_unallocated(&t->block_translation[b.b]));

    //Allocate a new block
    uint64_t size = calculate_size_on_disk(t);
    uint64_t offset;
    block_allocator_alloc_and_get_block(bt->block_allocator, size, &offset);
    PRNTF("blokAllokator", 1L, size, offset, bt);
    t->block_translation[b.b].u.diskoff = offset;
    t->block_translation[b.b].size      = size;
    t->block_translation[b.b].refc      = 1;
}

void toku_serialize_translation_to_wbuf(BLOCK_TABLE bt, int fd, struct wbuf *w,
                                        int64_t *address, int64_t *size)
// Effect: Fills wbuf (which starts uninitialized) with bt
//   A clean shutdown runs checkpoint start so that current and inprogress are copies.
//   The resulting wbuf buffer is guaranteed to be be 4096-byte aligned and the total length is a multiple of 4096 (so we pad with zeros at the end if needd)
//   The address is guaranteed to be 4096-byte aligned, but the size is not guaranteed.
//   It *is* guaranteed that we can read up to the next 4096-byte boundary, however
{
    lock_for_blocktable(bt);
    struct translation *t = &bt->inprogress;

    BLOCKNUM b = make_blocknum(RESERVED_BLOCKNUM_TRANSLATION);
    // The allocated block must be 4096-byte aligned to make O_DIRECT happy.
    blocknum_alloc_translation_on_disk_unlocked(bt);
    uint64_t size_translation = calculate_size_on_disk(t);
    uint64_t size_aligned     = roundup_to_multiple(BLOCK_ALIGNMENT, size_translation);
    assert((int64_t)size_translation == t->block_translation[b.b].size);
    {
        // Init wbuf
        // printf("%s:%d writing translation table of size_translation %" PRIu64 " at %" PRId64 "\n",
        //        __FILE__, __LINE__, size_translation, t->block_translation[b.b].u.diskoff);
        char *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, size_aligned, buf);
        memset(buf + size_translation, 0, size_aligned - size_translation);
        wbuf_init(w, buf, size_aligned);
    }
    wbuf_BLOCKNUM(w, t->smallest_never_used_blocknum);
    wbuf_BLOCKNUM(w, t->blocknum_freelist_head);

    int64_t i;
    for (i=0; i<t->smallest_never_used_blocknum.b; i++) {
        if (0)
            printf("%s:%d %" PRId64 ",%" PRId64 "\n", __FILE__, __LINE__, t->block_translation[i].u.diskoff, t->block_translation[i].size);
        wbuf_DISKOFF(w, t->block_translation[i].u.diskoff);
        wbuf_DISKOFF(w, t->block_translation[i].size);
        wbuf_ulonglong(w, t->block_translation[i].refc);
    }
    uint32_t checksum = x1764_finish(&w->checksum);
    wbuf_int(w, checksum);
    *address = t->block_translation[b.b].u.diskoff;
    *size    = size_translation;
    assert((*address) % BLOCK_ALIGNMENT == 0);

    ensure_safe_write_unlocked(bt, fd, size_aligned, *address);
    unlock_for_blocktable(bt);
}


// Perhaps rename: purpose is get disk address of a block, given its blocknum (blockid?)
static void
translate_blocknum_to_offset_size_unlocked(BLOCK_TABLE bt, BLOCKNUM b,
                                           DISKOFF *offset, DISKOFF *size)
{
    struct translation *t = &bt->current;
    verify_valid_blocknum(t, b);
    if (offset)
        *offset = t->block_translation[b.b].u.diskoff;
    if (size)
        *size = t->block_translation[b.b].size;
}

// Perhaps rename: purpose is get disk address of a block, given its blocknum (blockid?)
void
toku_translate_blocknum_to_offset_size(BLOCK_TABLE bt, BLOCKNUM b,
                                       DISKOFF *offset, DISKOFF *size)
{
    lock_for_blocktable(bt);
    translate_blocknum_to_offset_size_unlocked(bt, b, offset, size);
    unlock_for_blocktable(bt);
}

//Only called by toku_allocate_blocknum
static void
maybe_expand_translation(struct translation *t)
{
// Effect: expand the array to maintain size invariant
// given that one more never-used blocknum will soon be used.
    if (t->length_of_array <= t->smallest_never_used_blocknum.b) {
        //expansion is necessary
        uint64_t new_length = t->smallest_never_used_blocknum.b * 2;
        XREALLOC_N(t->length_of_array, new_length, t->block_translation);
        uint64_t i;
        for (i = t->length_of_array; i < new_length; i++) {
            t->block_translation[i].u.next_free_blocknum = freelist_null;
            t->block_translation[i].size                 = size_is_free;
            t->block_translation[i].refc                 = 0;
        }
        t->length_of_array = new_length;
    }
}

static void
toku_allocate_blocknum_unlocked(BLOCK_TABLE bt, BLOCKNUM *res, FT ft)
{
    assert_blocktable_locked(bt);
    BLOCKNUM result;
    struct translation *t = &bt->current;
    if (t->blocknum_freelist_head.b == freelist_null.b) {
        // no previously used blocknums are available
        // use a never used blocknum
        maybe_expand_translation(t); //Ensure a never used blocknums is available
        result = t->smallest_never_used_blocknum;
        t->smallest_never_used_blocknum.b++;
    } else {  // reuse a previously used blocknum
        result = t->blocknum_freelist_head;
        BLOCKNUM next = t->block_translation[result.b].u.next_free_blocknum;
        t->blocknum_freelist_head = next;
    }
    //Verify the blocknum is free
    paranoid_invariant(t->block_translation[result.b].size == size_is_free);
    //blocknum is not free anymore
    t->block_translation[result.b].u.diskoff = diskoff_unused;
    t->block_translation[result.b].size    = 0;
    t->block_translation[result.b].refc    = 1;
    verify_valid_freeable_blocknum(t, result);
    *res = result;
    ft_set_dirty(ft, false);
}

void
toku_allocate_blocknum(BLOCK_TABLE bt, BLOCKNUM *res, FT ft)
{
    lock_for_blocktable(bt);
    toku_allocate_blocknum_unlocked(bt, res, ft);
    unlock_for_blocktable(bt);
}

static void
free_blocknum_in_translation(struct translation *t, BLOCKNUM b)
{
    verify_valid_freeable_blocknum(t, b);
    paranoid_invariant(t->block_translation[b.b].size != size_is_free);

    PRNTF("free_blocknum", b.b, t->block_translation[b.b].size, t->block_translation[b.b].u.diskoff, bt);
    t->block_translation[b.b].size                 = size_is_free;
    t->block_translation[b.b].refc                 = 0;
    t->block_translation[b.b].u.next_free_blocknum = t->blocknum_freelist_head;
    t->blocknum_freelist_head                      = b;
}

static inline int
dec_refc_blocknum_in_translation(struct translation *t, BLOCKNUM b)
{
    assert(t->block_translation[b.b].refc > 0);
    int r = --t->block_translation[b.b].refc;
    if (r == 0)
        free_blocknum_in_translation(t, b);
    return r;
}

// return the resulting refc
int
toku_blocknum_dec_refc(BLOCK_TABLE bt, BLOCKNUM *bp, FT ft, bool for_checkpoint)
{
    int r;

    lock_for_blocktable(bt);

    BLOCKNUM b = *bp;
    bp->b = 0; //Remove caller's reference.

    struct block_translation_pair old_pair = bt->current.block_translation[b.b];
    r = dec_refc_blocknum_in_translation(&bt->current, b);
    if (for_checkpoint) {
        assert(ft->checkpoint_header->type == FT_CHECKPOINT_INPROGRESS);
        dec_refc_blocknum_in_translation(&bt->inprogress, b);
    }

    //If the size is 0, no disk block has ever been assigned to this blocknum.
    if (old_pair.size > 0) {
        //Free the old block if it is not still in use by the checkpoint in progress or the previous checkpoint
        if (!translation_prevents_freeing(&bt->current,      b, &old_pair) &&
            !translation_prevents_freeing(&bt->inprogress,   b, &old_pair) &&
            !translation_prevents_freeing(&bt->checkpointed, b, &old_pair))
        {
            PRNTF("free_blocknum_free", b.b, old_pair.size, old_pair.u.diskoff, bt);
            block_allocator_put_block(bt->block_allocator, old_pair.u.diskoff);
        }
    } else {
        assert(old_pair.u.diskoff = diskoff_unused);
    }

    ft_set_dirty(ft, for_checkpoint);

    unlock_for_blocktable(bt);

    return r;
}

// dec refc if the reference count is larger than 1
int toku_blocknum_maybe_dec_refc(BLOCK_TABLE bt, BLOCKNUM b, FT ft)
{
    int r;

    lock_for_blocktable(bt);

    struct block_translation_pair old_pair = bt->current.block_translation[b.b];
    assert(bt->current.block_translation[b.b].refc > 0);
    if (bt->current.block_translation[b.b].refc > 1) {
        bt->current.block_translation[b.b].refc--;
        r = 0;
    } else {
        r = -EINVAL;
    }

    ft_set_dirty(ft, false);

    unlock_for_blocktable(bt);

    return r;
}

void
toku_blocknum_inc_refc(BLOCK_TABLE bt, BLOCKNUM b, FT ft)
{
    lock_for_blocktable(bt);

    assert(bt->current.block_translation[b.b].refc > 0);
    bt->current.block_translation[b.b].refc += 1;

    ft_set_dirty(ft, false);

    unlock_for_blocktable(bt);
}

bool toku_blocknum_multi_copy(BLOCK_TABLE bt, BLOCKNUM b)
{
    bool ret = false;

    lock_for_blocktable(bt);

    assert(bt->current.block_translation[b.b].refc > 0);
    ret = (bt->current.block_translation[b.b].refc > 1);

    unlock_for_blocktable(bt);

    return ret;
}

// if the old refc == 1, we can keep the blocknum, return false
// if the old refc > 1, we need to break the cow, dec refc and return true
bool
toku_blocknum_maybe_break_cow(BLOCK_TABLE bt, BLOCKNUM b, BLOCKNUM *res, FT ft)
{
    bool ret = false;

    lock_for_blocktable(bt);

    assert(bt->current.block_translation[b.b].refc > 0);
    if (bt->current.block_translation[b.b].refc > 1) {
        bt->current.block_translation[b.b].refc -= 1;
        // this func set ft dirty
        toku_allocate_blocknum_unlocked(bt, res, ft);
        ret = true;
    }

    unlock_for_blocktable(bt);

    return ret;
}

//Verify there are no free blocks.
void
toku_block_verify_no_free_blocknums(BLOCK_TABLE UU(bt))
{
    paranoid_invariant(bt->current.blocknum_freelist_head.b == freelist_null.b);
}

// Frees blocknums that have a size of 0 and unused diskoff
// Currently used for eliminating unused cached rollback log nodes
void
toku_free_unused_blocknums(BLOCK_TABLE bt, BLOCKNUM root)
{
    lock_for_blocktable(bt);
    int64_t smallest = bt->current.smallest_never_used_blocknum.b;
    for (int64_t i = RESERVED_BLOCKNUMS; i < smallest; i++) {
        if (i == root.b) {
            continue;
        }
        BLOCKNUM b = make_blocknum(i);
        if (bt->current.block_translation[b.b].size == 0) {
            invariant(bt->current.block_translation[b.b].u.diskoff == diskoff_unused);
            free_blocknum_in_translation(&bt->current, b);
        }
    }
    unlock_for_blocktable(bt);
}

__attribute__((nonnull,const,unused))
static inline bool
no_data_blocks_except_root(BLOCK_TABLE bt, BLOCKNUM root)
{
    bool ok = true;
    lock_for_blocktable(bt);
    int64_t smallest = bt->current.smallest_never_used_blocknum.b;
    if (root.b < RESERVED_BLOCKNUMS) {
        ok = false;
        goto cleanup;
    }
    int64_t i;
    for (i = RESERVED_BLOCKNUMS; i < smallest; i++) {
        if (i == root.b) {
            continue;
        }
        BLOCKNUM b = make_blocknum(i);
        if (bt->current.block_translation[b.b].size != size_is_free) {
            ok = false;
            goto cleanup;
        }
    }
cleanup:
    unlock_for_blocktable(bt);
    return ok;
}

//Verify there are no data blocks except root.
// TODO(leif): This actually takes a lock, but I don't want to fix all the callers right now.
void
toku_block_verify_no_data_blocks_except_root(BLOCK_TABLE UU(bt), BLOCKNUM UU(root))
{
    paranoid_invariant(no_data_blocks_except_root(bt, root));
}

__attribute__((nonnull,const,unused))
static inline bool
blocknum_allocated(BLOCK_TABLE bt, BLOCKNUM b)
{
    lock_for_blocktable(bt);
    struct translation *t = &bt->current;
    verify_valid_blocknum(t, b);
    bool ok = (t->block_translation[b.b].size != size_is_free);
    unlock_for_blocktable(bt);
    return ok;
}

//Verify a blocknum is currently allocated.
void
toku_verify_blocknum_allocated(BLOCK_TABLE UU(bt), BLOCKNUM UU(b))
{
    paranoid_invariant(blocknum_allocated(bt, b));
}

//Only used by toku_dump_translation table (debug info)
static void
dump_translation(FILE *f, struct translation *t)
{
    if (t->block_translation) {
        BLOCKNUM b = make_blocknum(RESERVED_BLOCKNUM_TRANSLATION);
        fprintf(f, " length_of_array[%" PRId64 "]", t->length_of_array);
        fprintf(f, " smallest_never_used_blocknum[%" PRId64 "]", t->smallest_never_used_blocknum.b);
        fprintf(f, " blocknum_free_list_head[%" PRId64 "]", t->blocknum_freelist_head.b);
        fprintf(f, " size_on_disk[%" PRId64 "]", t->block_translation[b.b].size);
        fprintf(f, " location_on_disk[%" PRId64 "]\n", t->block_translation[b.b].u.diskoff);
        int64_t i;
        for (i=0; i<t->length_of_array; i++) {
            fprintf(f, " %" PRId64 ": %" PRId64 " %" PRId64 " %" PRId64 "\n",
                    i, t->block_translation[i].u.diskoff,
                    t->block_translation[i].size, t->block_translation[i].refc);
        }
        fprintf(f, "\n");
    }
    else {
        fprintf(f, " does not exist\n");
    }
}

//Only used by toku_ft_dump which is only for debugging purposes
// "pretty" just means we use tabs so we can parse output easier later
void
toku_dump_translation_table_pretty(FILE *f, BLOCK_TABLE bt)
{
    lock_for_blocktable(bt);
    struct translation *t = &bt->checkpointed;
    assert(t->block_translation != nullptr);
    for (int64_t i = 0; i < t->length_of_array; ++i) {
        fprintf(f, "%" PRId64 "\t%" PRId64 "\t%" PRId64 "\t%" PRId64 "\n",
                i, t->block_translation[i].u.diskoff,
                t->block_translation[i].size, t->block_translation[i].refc);
    }
    unlock_for_blocktable(bt);
}

//Only used by toku_ft_dump which is only for debugging purposes
void
toku_dump_translation_table(FILE *f, BLOCK_TABLE bt)
{
    lock_for_blocktable(bt);
    fprintf(f, "Current block translation:");
    dump_translation(f, &bt->current);
    fprintf(f, "Checkpoint in progress block translation:");
    dump_translation(f, &bt->inprogress);
    fprintf(f, "Checkpointed block translation:");
    dump_translation(f, &bt->checkpointed);
    unlock_for_blocktable(bt);
}

//Only used by ftdump
void
toku_blocknum_dump_translation(BLOCK_TABLE bt, BLOCKNUM b)
{
    lock_for_blocktable(bt);

    struct translation *t = &bt->current;
    if (b.b < t->length_of_array) {
        struct block_translation_pair *bx = &t->block_translation[b.b];
        printf("%" PRId64 ": %" PRId64 " %" PRId64 " %" PRId64 "\n",
               b.b, bx->u.diskoff, bx->size, bx->refc);
    }

    unlock_for_blocktable(bt);
}


// Must not call this function when anything else is using the blocktable.
// No one may use the blocktable afterwards.
void
toku_blocktable_destroy(BLOCK_TABLE *btp)
{
    BLOCK_TABLE bt = *btp;
    *btp = NULL;

    if (bt->current.block_translation)
    {
        bt->current.log_block_log.deinit();
        sb_free_sized(bt->current.block_translation, bt->current.length_of_array * sizeof(struct block_translation_pair));
    }

    if (bt->inprogress.block_translation)
    {
        bt->current.log_block_log.deinit();
        sb_free_sized(bt->inprogress.block_translation, bt->inprogress.length_of_array * sizeof(struct block_translation_pair));
    }
    if (bt->checkpointed.block_translation) {
        sb_free_sized(bt->checkpointed.block_translation, bt->checkpointed.length_of_array * sizeof(struct block_translation_pair));
    }

    destroy_block_allocator(&bt->block_allocator);
    blocktable_lock_destroy(bt);
    nb_mutex_destroy(&bt->safe_file_size_lock);
    toku_free(bt);
}


// Effect: Fill it in, including the translation table, which is uninitialized
static BLOCK_TABLE
blocktable_create_internal(void)
{
    BLOCK_TABLE XCALLOC(bt);
    blocktable_lock_init(bt);
    nb_mutex_init(&bt->safe_file_size_lock);

    // There are two headers, so we reserve space for two.
    uint64_t reserve_per_header = BLOCK_ALLOCATOR_HEADER_RESERVE;

    // Must reserve in multiples of BLOCK_ALLOCATOR_ALIGHNMENT
    // Round up the per-header usage if necessary.
    // We want each header aligned.
    uint64_t remainder = BLOCK_ALLOCATOR_HEADER_RESERVE %
                         BLOCK_ALLOCATOR_ALIGNMENT;
    if (remainder != 0) {
        reserve_per_header += (BLOCK_ALLOCATOR_ALIGNMENT - remainder);
    }
    assert(2 * reserve_per_header == BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE);
    create_block_allocator(&bt->block_allocator,
                           BLOCK_ALLOCATOR_TOTAL_HEADER_RESERVE,
                           BLOCK_ALLOCATOR_ALIGNMENT);
    return bt;
}

// t: destination into which to create a default translation
static void
translation_default(struct translation *t)
{
    t->type                         = TRANSLATION_CHECKPOINTED;
    t->smallest_never_used_blocknum = make_blocknum(RESERVED_BLOCKNUMS);
    t->length_of_array              = t->smallest_never_used_blocknum.b;
    t->blocknum_freelist_head       = freelist_null;
    t->block_translation            = (struct block_translation_pair *)
        sb_malloc_sized(t->length_of_array * sizeof(struct block_translation_pair), true);
    int64_t i;
    for (i = 0; i < t->length_of_array; i++) {
        t->block_translation[i].u.diskoff = diskoff_unused;
        t->block_translation[i].size      = 0;
        t->block_translation[i].refc      = 1;
    }
    t->log_block_log.init();
}


static int
translation_deserialize_from_buffer(
    // destination into which to deserialize
    struct translation *t,
    // Location of translation_buffer
    DISKOFF location_on_disk,
    uint64_t size_on_disk,
    // buffer with serialized translation
    unsigned char *translation_buffer)
{
    int r = 0;
    assert(location_on_disk != 0);
    t->type = TRANSLATION_CHECKPOINTED;
    {
        // check the checksum
        uint32_t x1764 = x1764_memory(translation_buffer, size_on_disk - 4);
        uint64_t offset = size_on_disk - 4;
        //printf("%s:%d read from %ld (x1764 offset=%ld) size=%ld\n",
        //       __FILE__, __LINE__, block_translation_address_on_disk, offset,
        //       block_translation_size_on_disk);
        uint32_t stored_x1764 = toku_dtoh32(*(int*)(translation_buffer + offset));
        if (x1764 != stored_x1764) {
            fprintf(stderr,
                    "Translation table checksum failure: calc=0x%08x read=0x%08x\n",
                    x1764, stored_x1764);
            r = TOKUDB_BAD_CHECKSUM;
            goto exit;
        }
    }
    struct rbuf rt;
    rt.buf   = translation_buffer;
    rt.ndone = 0;
    rt.size  = size_on_disk - 4;//4==checksum

    t->smallest_never_used_blocknum = rbuf_blocknum(&rt);

    t->length_of_array              = t->smallest_never_used_blocknum.b;
    assert(t->smallest_never_used_blocknum.b >= RESERVED_BLOCKNUMS);
    t->blocknum_freelist_head       = rbuf_blocknum(&rt);
    t->block_translation            = (struct block_translation_pair *)
    sb_malloc_sized(t->length_of_array * sizeof(struct block_translation_pair), true);

    int64_t i;
    for (i = 0; i < t->length_of_array; i++) {
        t->block_translation[i].u.diskoff = rbuf_diskoff(&rt);
        t->block_translation[i].size      = rbuf_diskoff(&rt);
        t->block_translation[i].refc      = rbuf_ulonglong(&rt);
        PRNTF("ReadIn", i, t->block_translation[i].size, t->block_translation[i].u.diskoff, NULL);
    }
    assert(calculate_size_on_disk(t)                                     == size_on_disk);
    assert(t->block_translation[RESERVED_BLOCKNUM_TRANSLATION].size      == (int64_t)size_on_disk);
    assert(t->block_translation[RESERVED_BLOCKNUM_TRANSLATION].u.diskoff == location_on_disk);
exit:
    return r;
}

// We just initialized a translation, inform block allocator to reserve space
//   for each blocknum in use
static void
blocktable_note_translation(BLOCK_ALLOCATOR allocator, struct translation *t)
{
    // This is where the space for them will be reserved (in addition to normal
    //   blocks). See RESERVED_BLOCKNUMS

    // Previously this added blocks one at a time, Now we make an array and
    //   pass it in so it can be sorted and merged. See #3218
    struct block_allocator_blockpair *
        XMALLOC_N(t->smallest_never_used_blocknum.b, pairs);
    uint64_t n_pairs = 0;
    for (int64_t i = 0; i < t->smallest_never_used_blocknum.b; i++) {
        struct block_translation_pair pair = t->block_translation[i];
        if (pair.size > 0) {
            paranoid_invariant(pair.u.diskoff != diskoff_unused);
            int cur_pair = n_pairs++;
            pairs[cur_pair] = (struct block_allocator_blockpair)
                              {
                                    .offset = (uint64_t)pair.u.diskoff,
                                    .size   = (uint64_t)pair.size,
                                    .nref   = 1
                              };
        }
    }
    block_allocator_alloc_blocks_at(allocator, n_pairs, pairs);
    toku_free(pairs);
}

// Fill in the checkpointed translation from buffer, and copy checkpointed to current.
// The one read from disk is the last known checkpointed one, so we are keeping it in
// place and then setting current (which is never stored on disk) for current use.
// The translation_buffer has translation only, we create the rest of the block_table.
int
toku_blocktable_create_from_buffer(int fd,
                                   BLOCK_TABLE *btp,
                                   // Location of translation_buffer
                                   DISKOFF location_on_disk,
                                   DISKOFF size_on_disk,
                                   unsigned char *translation_buffer)
{
    BLOCK_TABLE bt = blocktable_create_internal();
    int r = translation_deserialize_from_buffer(&bt->checkpointed,
                                                location_on_disk,
                                                size_on_disk,
                                                translation_buffer);
    if (r != 0) {
        goto exit;
    }
    blocktable_note_translation(bt->block_allocator, &bt->checkpointed);
    // we just filled in checkpointed, now copy it to current.
    copy_translation(&bt->current, &bt->checkpointed, TRANSLATION_CURRENT);

    int64_t file_size;
    r = toku_os_get_file_size(fd, &file_size);
    lazy_assert_zero(r);
    invariant(file_size >= 0);
    bt->safe_file_size = file_size;

    *btp = bt;
exit:
    return r;
}

void
toku_blocktable_create_new(BLOCK_TABLE *btp)
{
    BLOCK_TABLE bt = blocktable_create_internal();
    // create default btt (empty except for reserved blocknums)
    translation_default(&bt->checkpointed);
    blocktable_note_translation(bt->block_allocator, &bt->checkpointed);
    // we just created a default checkpointed, now copy it to current.
    copy_translation(&bt->current, &bt->checkpointed, TRANSLATION_CURRENT);

    *btp = bt;
}

int
toku_blocktable_iterate(BLOCK_TABLE bt, enum translation_type type,
                        BLOCKTABLE_CALLBACK f, void *extra,
                        bool data_only, bool used_only)
{
    struct translation *src;

    int r = 0;
    switch (type) {
        case TRANSLATION_CURRENT:
            src = &bt->current;
            break;
        case TRANSLATION_INPROGRESS:
            src = &bt->inprogress;
            break;
        case TRANSLATION_CHECKPOINTED:
            src = &bt->checkpointed;
            break;
        default:
            r = EINVAL;
            break;
    }
    struct translation fakecurrent;
    struct translation *t = &fakecurrent;
    if (r == 0) {
        lock_for_blocktable(bt);
        copy_translation(t, src, TRANSLATION_DEBUG);
        t->block_translation[RESERVED_BLOCKNUM_TRANSLATION] =
            src->block_translation[RESERVED_BLOCKNUM_TRANSLATION];
        unlock_for_blocktable(bt);
        int64_t i;
        for (i = 0; i < t->smallest_never_used_blocknum.b; i++) {
            struct block_translation_pair pair = t->block_translation[i];
            if (data_only && i < RESERVED_BLOCKNUMS)
                continue;
            if (used_only && pair.size <= 0)
                continue;
            r = f(make_blocknum(i), pair.size, pair.u.diskoff, extra);
            if (r != 0)
                break;
        }
        sb_free_sized(t->block_translation, t->length_of_array * sizeof(struct block_translation_pair));
    }
    return r;
}

typedef struct {
    int64_t used_space;
    int64_t total_space;
} frag_extra;

static int
frag_helper(BLOCKNUM UU(b), int64_t size, int64_t address, void *extra)
{
    frag_extra *info = (frag_extra *) extra;

    if (size + address > info->total_space)
        info->total_space = size + address;
    info->used_space += size;
    return 0;
}

void
toku_blocktable_internal_fragmentation(BLOCK_TABLE bt,
                                       int64_t *total_sizep,
                                       int64_t *used_sizep)
{
    frag_extra info = {0, 0};
    int r = toku_blocktable_iterate(bt, TRANSLATION_CHECKPOINTED,
                                    frag_helper, &info, false, true);
    assert_zero(r);

    if (total_sizep)
        *total_sizep = info.total_space;
    if (used_sizep)
        *used_sizep  = info.used_space;
}

static void
toku_realloc_descriptor_on_disk_unlocked(BLOCK_TABLE bt,
                                         DISKOFF size, DISKOFF *offset, FT ft)
{
    assert_blocktable_locked(bt);
    BLOCKNUM b = make_blocknum(RESERVED_BLOCKNUM_DESCRIPTOR);
    blocknum_realloc_on_disk_internal(bt, b, size, offset, ft, false);
}

void
toku_realloc_descriptor_on_disk(BLOCK_TABLE bt, DISKOFF size, DISKOFF *offset,
                                FT ft, int fd)
{
    lock_for_blocktable(bt);
    toku_realloc_descriptor_on_disk_unlocked(bt, size, offset, ft);

    ensure_safe_write_unlocked(bt, fd, size, *offset);
    unlock_for_blocktable(bt);
}

void
toku_get_descriptor_offset_size(BLOCK_TABLE bt, DISKOFF *offset, DISKOFF *size)
{
    lock_for_blocktable(bt);
    BLOCKNUM b = make_blocknum(RESERVED_BLOCKNUM_DESCRIPTOR);
    translate_blocknum_to_offset_size_unlocked(bt, b, offset, size);
    unlock_for_blocktable(bt);
}

void
toku_block_table_get_fragmentation_unlocked(BLOCK_TABLE bt,
                                            TOKU_DB_FRAGMENTATION report)
{
    //Requires:  blocktable lock is held.
    //Requires:  report->file_size_bytes is already filled in.

    //Count the headers.
    report->data_bytes                   = BLOCK_ALLOCATOR_HEADER_RESERVE;
    report->data_blocks                  = 1;
    report->checkpoint_bytes_additional  = BLOCK_ALLOCATOR_HEADER_RESERVE;
    report->checkpoint_blocks_additional = 1;

    struct translation *current = &bt->current;
    int64_t i;
    for (i = 0; i < current->length_of_array; i++) {
        struct block_translation_pair *pair = &current->block_translation[i];
        if (pair->size > 0) {
            report->data_bytes += pair->size;
            report->data_blocks++;
        }
    }
    struct translation *checkpointed = &bt->checkpointed;
    for (i = 0; i < checkpointed->length_of_array; i++) {
        struct block_translation_pair *pair = &checkpointed->block_translation[i];
        if (pair->size > 0 &&
            !(i < current->length_of_array &&
                current->block_translation[i].size > 0 &&
                current->block_translation[i].u.diskoff == pair->u.diskoff)
           )
        {
                report->checkpoint_bytes_additional += pair->size;
                report->checkpoint_blocks_additional++;
        }
    }
    struct translation *inprogress = &bt->inprogress;
    for (i = 0; i < inprogress->length_of_array; i++) {
        struct block_translation_pair *pair = &inprogress->block_translation[i];
        if (pair->size > 0 &&
            !(i < current->length_of_array &&
                current->block_translation[i].size > 0 &&
                current->block_translation[i].u.diskoff == pair->u.diskoff) &&
            !(i < checkpointed->length_of_array &&
                checkpointed->block_translation[i].size > 0 &&
                checkpointed->block_translation[i].u.diskoff == pair->u.diskoff)
           )
        {
                report->checkpoint_bytes_additional += pair->size;
                report->checkpoint_blocks_additional++;
        }
    }

    block_allocator_get_unused_statistics(bt->block_allocator, report);
}

void
toku_blocktable_get_info64(BLOCK_TABLE bt, struct ftinfo64 *s)
{
    lock_for_blocktable(bt);

    struct translation *current = &bt->current;
    s->num_blocks_allocated = current->length_of_array;
    s->num_blocks_in_use = 0;
    s->size_allocated = 0;
    s->size_in_use = 0;

    for (int64_t i = 0; i < current->length_of_array; ++i) {
        struct block_translation_pair *block = &current->block_translation[i];
        if (block->size != size_is_free) {
            ++s->num_blocks_in_use;
            s->size_in_use += block->size;
            if (block->u.diskoff != diskoff_unused) {
                uint64_t limit = block->u.diskoff + block->size;
                if (limit > s->size_allocated) {
                    s->size_allocated = limit;
                }
            }
        }
    }

    unlock_for_blocktable(bt);
}

int
toku_blocktable_iterate_translation_tables(BLOCK_TABLE bt, uint64_t checkpoint_count,
                                           int (*iter)(uint64_t checkpoint_count,
                                                       int64_t total_num_rows,
                                                       int64_t blocknum,
                                                       int64_t diskoff,
                                                       int64_t size,
                                                       void *extra),
                                           void *iter_extra)
{
    int error = 0;
    lock_for_blocktable(bt);

    int64_t total_num_rows = bt->current.length_of_array + bt->checkpointed.length_of_array;
    for (int64_t i = 0; error == 0 && i < bt->current.length_of_array; ++i) {
        struct block_translation_pair *block = &bt->current.block_translation[i];
        error = iter(checkpoint_count, total_num_rows, i, block->u.diskoff, block->size, iter_extra);
    }
    for (int64_t i = 0; error == 0 && i < bt->checkpointed.length_of_array; ++i) {
        struct block_translation_pair *block = &bt->checkpointed.block_translation[i];
        error = iter(checkpoint_count - 1, total_num_rows, i, block->u.diskoff, block->size, iter_extra);
    }

    unlock_for_blocktable(bt);
    return error;
}

