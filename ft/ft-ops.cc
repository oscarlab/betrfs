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

/*

Managing the tree shape:  How insertion, deletion, and querying work

When we insert a message into the FT_HANDLE, here's what happens.

to insert a message at the root

    --------------- find the root node
    - capture the next msn of the root node and assign it to the message
    - split the root if it needs to be split
    - insert the message into the root buffer
    - if the root is too full, then toku_ft_flush_some_child() of the root on a flusher thread

flusher functions use an advice struct with provides some functions to
call that tell it what to do based on the context of the flush. see ft-flusher.h

to flush some child, given a parent and some advice
    - pick the child using advice->pick_child()
    - remove that childs buffer from the parent
    - flush the buffer to the child
    - if the child has stable reactivity and
      advice->should_recursively_flush() is true, then
      toku_ft_flush_some_child() of the child
    - otherwise split the child if it needs to be split
    - otherwise maybe merge the child if it needs to be merged

flusher threads:

    flusher threads are created on demand as the result of internal nodes
    becoming gorged by insertions. this allows flushing to be done somewhere
    other than the client thread. these work items are enqueued onto
    the cachetable kibbutz and are done in a first in first out order.

cleaner threads:

    the cleaner thread wakes up every so often (say, 1 second) and chooses
    a small number (say, 5) of nodes as candidates for a flush. the one
    with the largest cache pressure is chosen to be flushed. cache pressure
    is a function of the size of the node in the cachetable plus the work done.
    the cleaner thread need not actually do a flush when awoken, so only
    nodes that have sufficient cache pressure are flushed.

checkpointing:

    the checkpoint thread wakes up every minute to checkpoint dirty nodes
    to disk. at the time of this writing, nodes during checkpoint are
    locked and cannot be queried or flushed to. a design in which nodes
    are copied before checkpoint is being considered as a way to reduce
    the performance variability caused by a checkpoint locking too
    many nodes and preventing other threads from traversing down the tree,
    for a query or otherwise.

To shrink a file: Let X be the size of the reachable data.
    We define an acceptable bloat constant of C.  For example we set C=2 if we are willing to allow the file to be as much as 2X in size.
    The goal is to find the smallest amount of stuff we can move to get the file down to size CX.
    That seems like a difficult problem, so we use the following heuristics:
       If we can relocate the last block to an lower location, then do so immediately.        (The file gets smaller right away, so even though the new location
         may even not be in the first CX bytes, we are making the file smaller.)
       Otherwise all of the earlier blocks are smaller than the last block (of size L).         So find the smallest region that has L free bytes in it.
         (This can be computed in one pass)
         Move the first allocated block in that region to some location not in the interior of the region.
               (Outside of the region is OK, and reallocating the block at the edge of the region is OK).
            This has the effect of creating a smaller region with at least L free bytes in it.
         Go back to the top (because by now some other block may have been allocated or freed).
    Claim: if there are no other allocations going on concurrently, then this algorithm will shrink the file reasonably efficiently.  By this I mean that
       each block of shrinkage does the smallest amount of work possible.  That doesn't mean that the work overall is minimized.
    Note: If there are other allocations and deallocations going on concurrently, we might never get enough space to move the last block.  But it takes a lot
      of allocations and deallocations to make that happen, and it's probably reasonable for the file not to shrink in this case.

To split or merge a child of a node:
Split_or_merge (node, childnum) {
  If the child needs to be split (it's a leaf with too much stuff or a nonleaf with too much fanout)
    fetch the node and the child into main memory.
    split the child, producing two nodes A and B, and also a pivot.   Don't worry if the resulting child is still too big or too small.         Fix it on the next pass.
    fixup node to point at the two new children.  Don't worry about the node getting too much fanout.
    return;
  If the child needs to be merged (it's a leaf with too little stuff (less than 1/4 full) or a nonleaf with too little fanout (less than 1/4)
    fetch node, the child  and a sibling of the child into main memory.
    move all messages from the node to the two children (so that the FIFOs are empty)
    If the two siblings together fit into one node then
      merge the two siblings.
      fixup the node to point at one child
    Otherwise
      load balance the content of the two nodes
    Don't worry about the resulting children having too many messages or otherwise being too big or too small.        Fix it on the next pass.
  }
}

Here's how querying works:

lookups:
    - As of Dr. No, we don't do any tree shaping on lookup.
    - We don't promote eagerly or use aggressive promotion or passive-aggressive
    promotion.        We just push messages down according to the traditional FT_HANDLE
    algorithm on insertions.
    - when a node is brought into memory, we apply ancestor messages above it.

basement nodes, bulk fetch,  and partial fetch:
    - leaf nodes are comprised of N basement nodes, each of nominal size. when
    a query hits a leaf node. it may require one or more basement nodes to be in memory.
    - for point queries, we do not read the entire node into memory. instead,
      we only read in the required basement node
    - for range queries, cursors may return cursor continue in their callback
      to take a the shortcut path until the end of the basement node.
    - for range queries, cursors may prelock a range of keys (with or without a txn).
      the fractal tree will prefetch nodes aggressively until the end of the range.
    - without a prelocked range, range queries behave like successive point queries.

*/

#include "checkpoint.h"
#include "ft.h"
#include "ft-cachetable-wrappers.h"
#include "log-internal.h"
#include "ft-flusher.h"
#include "ft-internal.h"
#include "ft_layout_version.h"
#include "key.h"
#include "sub_block.h"
#include "txn_manager.h"
#include "leafentry.h"
#include "xids.h"
#include "ft_msg.h"
#include "ule.h"
#include <toku_race_tools.h>

#include <portability/toku_atomic.h>

#include <util/mempool.h>
#include <util/status.h>
#include <util/rwlock.h>
#include <util/sort.h>

#include <stdint.h>
TOKULOGGER global_logger = nullptr;
static const uint32_t this_version = FT_LAYOUT_VERSION;

/* Status is intended for display to humans to help understand system behavior.
 * It does not need to be perfectly thread-safe.
 */
static FT_STATUS_S ft_status;

#define STATUS_INIT(k,c,t,l,inc) TOKUDB_STATUS_INIT(ft_status, k, c, t, "brt: " l, inc)

static toku_mutex_t ft_open_close_lock;

extern "C" void toku_dump_stack(void);
static void
status_init(void)
{
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.
    STATUS_INIT(FT_UPDATES,                                DICTIONARY_UPDATES, PARCOUNT, "dictionary updates", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_UPDATES_BROADCAST,                      DICTIONARY_BROADCAST_UPDATES, PARCOUNT, "dictionary broadcast updates", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DESCRIPTOR_SET,                         DESCRIPTOR_SET, PARCOUNT, "descriptor set", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_MSN_DISCARDS,                           MESSAGES_IGNORED_BY_LEAF_DUE_TO_MSN, PARCOUNT, "messages ignored by leaf due to msn", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOTAL_RETRIES,                          nullptr, PARCOUNT, "total search retries due to TRY_AGAIN", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_SEARCH_TRIES_GT_HEIGHT,                 nullptr, PARCOUNT, "searches requiring more tries than the height of the tree", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_SEARCH_TRIES_GT_HEIGHTPLUS3,            nullptr, PARCOUNT, "searches requiring more tries than the height of the tree plus three", TOKU_ENGINE_STATUS);
    STATUS_INIT(FT_CREATE_LEAF,                            LEAF_NODES_CREATED, PARCOUNT, "leaf nodes created", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_CREATE_NONLEAF,                         NONLEAF_NODES_CREATED, PARCOUNT, "nonleaf nodes created", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DESTROY_LEAF,                           LEAF_NODES_DESTROYED, PARCOUNT, "leaf nodes destroyed", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DESTROY_NONLEAF,                        NONLEAF_NODES_DESTROYED, PARCOUNT, "nonleaf nodes destroyed", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_MSG_BYTES_IN,                           MESSAGES_INJECTED_AT_ROOT_BYTES, PARCOUNT, "bytes of messages injected at root (all trees)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_MSG_BYTES_OUT,                          MESSAGES_FLUSHED_FROM_H1_TO_LEAVES_BYTES, PARCOUNT, "bytes of messages flushed from h1 nodes to leaves", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_MSG_BYTES_CURR,                         MESSAGES_IN_TREES_ESTIMATE_BYTES, PARCOUNT, "bytes of messages currently in trees (estimate)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_MSG_NUM,                                MESSAGES_INJECTED_AT_ROOT, PARCOUNT, "messages injected at root", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_MSG_NUM_BROADCAST,                      BROADCASE_MESSAGES_INJECTED_AT_ROOT, PARCOUNT, "broadcast messages injected at root", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_NORMAL,      BASEMENTS_DECOMPRESSED_TARGET_QUERY, PARCOUNT, "basements decompressed as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_AGGRESSIVE,  BASEMENTS_DECOMPRESSED_PRELOCKED_RANGE, PARCOUNT, "basements decompressed for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_PREFETCH,    BASEMENTS_DECOMPRESSED_PREFETCH, PARCOUNT, "basements decompressed for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_WRITE,       BASEMENTS_DECOMPRESSED_FOR_WRITE, PARCOUNT, "basements decompressed for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_NORMAL,     BUFFERS_DECOMPRESSED_TARGET_QUERY, PARCOUNT, "buffers decompressed as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_AGGRESSIVE, BUFFERS_DECOMPRESSED_PRELOCKED_RANGE, PARCOUNT, "buffers decompressed for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_PREFETCH,   BUFFERS_DECOMPRESSED_PREFETCH, PARCOUNT, "buffers decompressed for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_WRITE,      BUFFERS_DECOMPRESSED_FOR_WRITE, PARCOUNT, "buffers decompressed for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Eviction statistics:
    STATUS_INIT(FT_FULL_EVICTIONS_LEAF,                    LEAF_NODE_FULL_EVICTIONS, PARCOUNT, "leaf node full evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_FULL_EVICTIONS_LEAF_BYTES,              LEAF_NODE_FULL_EVICTIONS_BYTES, PARCOUNT, "leaf node full evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_FULL_EVICTIONS_NONLEAF,                 NONLEAF_NODE_FULL_EVICTIONS, PARCOUNT, "nonleaf node full evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_FULL_EVICTIONS_NONLEAF_BYTES,           NONLEAF_NODE_FULL_EVICTIONS_BYTES, PARCOUNT, "nonleaf node full evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PARTIAL_EVICTIONS_LEAF,                 LEAF_NODE_PARTIAL_EVICTIONS, PARCOUNT, "leaf node partial evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PARTIAL_EVICTIONS_LEAF_BYTES,           LEAF_NODE_PARTIAL_EVICTIONS_BYTES, PARCOUNT, "leaf node partial evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PARTIAL_EVICTIONS_NONLEAF,              NONLEAF_NODE_PARTIAL_EVICTIONS, PARCOUNT, "nonleaf node partial evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PARTIAL_EVICTIONS_NONLEAF_BYTES,        NONLEAF_NODE_PARTIAL_EVICTIONS_BYTES, PARCOUNT, "nonleaf node partial evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Disk read statistics:
    //
    // Pivots: For queries, prefetching, or writing.
    STATUS_INIT(FT_NUM_PIVOTS_FETCHED_QUERY,               PIVOTS_FETCHED_FOR_QUERY, PARCOUNT, "pivots fetched for query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_PIVOTS_FETCHED_QUERY,             PIVOTS_FETCHED_FOR_QUERY_BYTES, PARCOUNT, "pivots fetched for query (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_PIVOTS_FETCHED_QUERY,          PIVOTS_FETCHED_FOR_QUERY_SECONDS, TOKUTIME, "pivots fetched for query (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_PIVOTS_FETCHED_PREFETCH,            PIVOTS_FETCHED_FOR_PREFETCH, PARCOUNT, "pivots fetched for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_PIVOTS_FETCHED_PREFETCH,          PIVOTS_FETCHED_FOR_PREFETCH_BYTES, PARCOUNT, "pivots fetched for prefetch (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_PIVOTS_FETCHED_PREFETCH,       PIVOTS_FETCHED_FOR_PREFETCH_SECONDS, TOKUTIME, "pivots fetched for prefetch (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_PIVOTS_FETCHED_WRITE,               PIVOTS_FETCHED_FOR_WRITE, PARCOUNT, "pivots fetched for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_PIVOTS_FETCHED_WRITE,             PIVOTS_FETCHED_FOR_WRITE_BYTES, PARCOUNT, "pivots fetched for write (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_PIVOTS_FETCHED_WRITE,          PIVOTS_FETCHED_FOR_WRITE_SECONDS, TOKUTIME, "pivots fetched for write (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    // Basements: For queries, aggressive fetching in prelocked range, prefetching, or writing.
    STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_NORMAL,           BASEMENTS_FETCHED_TARGET_QUERY, PARCOUNT, "basements fetched as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_NORMAL,         BASEMENTS_FETCHED_TARGET_QUERY_BYTES, PARCOUNT, "basements fetched as a target of a query (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_NORMAL,      BASEMENTS_FETCHED_TARGET_QUERY_SECONDS, TOKUTIME, "basements fetched as a target of a query (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_AGGRESSIVE,       BASEMENTS_FETCHED_PRELOCKED_RANGE, PARCOUNT, "basements fetched for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_AGGRESSIVE,     BASEMENTS_FETCHED_PRELOCKED_RANGE_BYTES, PARCOUNT, "basements fetched for prelocked range (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_AGGRESSIVE,  BASEMENTS_FETCHED_PRELOCKED_RANGE_SECONDS, TOKUTIME, "basements fetched for prelocked range (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_PREFETCH,         BASEMENTS_FETCHED_PREFETCH, PARCOUNT, "basements fetched for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_PREFETCH,       BASEMENTS_FETCHED_PREFETCH_BYTES, PARCOUNT, "basements fetched for prefetch (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_PREFETCH,    BASEMENTS_FETCHED_PREFETCH_SECONDS, TOKUTIME, "basements fetched for prefetch (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_WRITE,            BASEMENTS_FETCHED_FOR_WRITE, PARCOUNT, "basements fetched for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_WRITE,          BASEMENTS_FETCHED_FOR_WRITE_BYTES, PARCOUNT, "basements fetched for write (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_WRITE,       BASEMENTS_FETCHED_FOR_WRITE_SECONDS, TOKUTIME, "basements fetched for write (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    // Buffers: For queries, aggressive fetching in prelocked range, prefetching, or writing.
    STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_NORMAL,          BUFFERS_FETCHED_TARGET_QUERY, PARCOUNT, "buffers fetched as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_NORMAL,        BUFFERS_FETCHED_TARGET_QUERY_BYTES, PARCOUNT, "buffers fetched as a target of a query (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_NORMAL,     BUFFERS_FETCHED_TARGET_QUERY_SECONDS, TOKUTIME, "buffers fetched as a target of a query (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_AGGRESSIVE,      BUFFERS_FETCHED_PRELOCKED_RANGE, PARCOUNT, "buffers fetched for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_AGGRESSIVE,    BUFFERS_FETCHED_PRELOCKED_RANGE_BYTES, PARCOUNT, "buffers fetched for prelocked range (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_AGGRESSIVE, BUFFERS_FETCHED_PRELOCKED_RANGE_SECONDS, TOKUTIME, "buffers fetched for prelocked range (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_PREFETCH,        BUFFERS_FETCHED_PREFETCH, PARCOUNT, "buffers fetched for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_PREFETCH,      BUFFERS_FETCHED_PREFETCH_BYTES, PARCOUNT, "buffers fetched for prefetch (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_PREFETCH,   BUFFERS_FETCHED_PREFETCH_SECONDS, TOKUTIME, "buffers fetched for prefetch (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_WRITE,           BUFFERS_FETCHED_FOR_WRITE, PARCOUNT, "buffers fetched for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_WRITE,         BUFFERS_FETCHED_FOR_WRITE_BYTES, PARCOUNT, "buffers fetched for write (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_WRITE,      BUFFERS_FETCHED_FOR_WRITE_SECONDS, TOKUTIME, "buffers fetched for write (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Disk write statistics.
    //
    // Leaf/Nonleaf: Not for checkpoint
    STATUS_INIT(FT_DISK_FLUSH_LEAF,                                         LEAF_NODES_FLUSHED_NOT_CHECKPOINT, PARCOUNT, "leaf nodes flushed to disk (not for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_LEAF_BYTES,                                   LEAF_NODES_FLUSHED_NOT_CHECKPOINT_BYTES, PARCOUNT, "leaf nodes flushed to disk (not for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES,                      LEAF_NODES_FLUSHED_NOT_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "leaf nodes flushed to disk (not for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_LEAF_TOKUTIME,                                LEAF_NODES_FLUSHED_NOT_CHECKPOINT_SECONDS, TOKUTIME, "leaf nodes flushed to disk (not for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF,                                      NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT, PARCOUNT, "nonleaf nodes flushed to disk (not for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_BYTES,                                NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (not for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES,                   NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (not for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_TOKUTIME,                             NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT_SECONDS, TOKUTIME, "nonleaf nodes flushed to disk (not for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    // Leaf/Nonleaf: For checkpoint
    STATUS_INIT(FT_DISK_FLUSH_LEAF_FOR_CHECKPOINT,                          LEAF_NODES_FLUSHED_CHECKPOINT, PARCOUNT, "leaf nodes flushed to disk (for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_LEAF_BYTES_FOR_CHECKPOINT,                    LEAF_NODES_FLUSHED_CHECKPOINT_BYTES, PARCOUNT, "leaf nodes flushed to disk (for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT,       LEAF_NODES_FLUSHED_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "leaf nodes flushed to disk (for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_LEAF_TOKUTIME_FOR_CHECKPOINT,                 LEAF_NODES_FLUSHED_CHECKPOINT_SECONDS, TOKUTIME, "leaf nodes flushed to disk (for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_FOR_CHECKPOINT,                       NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT, PARCOUNT, "nonleaf nodes flushed to disk (for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_BYTES_FOR_CHECKPOINT,                 NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT,    NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_DISK_FLUSH_NONLEAF_TOKUTIME_FOR_CHECKPOINT,              NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT_SECONDS, TOKUTIME, "nonleaf nodes flushed to disk (for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // CPU time statistics for [de]serialization and [de]compression.
    STATUS_INIT(FT_LEAF_COMPRESS_TOKUTIME,                                  LEAF_COMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "leaf compression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_LEAF_SERIALIZE_TOKUTIME,                                 LEAF_SERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "leaf serialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_LEAF_DECOMPRESS_TOKUTIME,                                LEAF_DECOMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "leaf decompression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_LEAF_DESERIALIZE_TOKUTIME,                               LEAF_DESERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "leaf deserialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NONLEAF_COMPRESS_TOKUTIME,                               NONLEAF_COMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf compression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NONLEAF_SERIALIZE_TOKUTIME,                              NONLEAF_SERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf serialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NONLEAF_DECOMPRESS_TOKUTIME,                             NONLEAF_DECOMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf decompression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_NONLEAF_DESERIALIZE_TOKUTIME,                            NONLEAF_DESERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf deserialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Promotion statistics.
    STATUS_INIT(FT_PRO_NUM_ROOT_SPLIT,                     PROMOTION_ROOTS_SPLIT, PARCOUNT, "promotion: roots split", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_ROOT_H0_INJECT,                 PROMOTION_LEAF_ROOTS_INJECTED_INTO, PARCOUNT, "promotion: leaf roots injected into", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_ROOT_H1_INJECT,                 PROMOTION_H1_ROOTS_INJECTED_INTO, PARCOUNT, "promotion: h1 roots injected into", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_0,                 PROMOTION_INJECTIONS_AT_DEPTH_0, PARCOUNT, "promotion: injections at depth 0", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_1,                 PROMOTION_INJECTIONS_AT_DEPTH_1, PARCOUNT, "promotion: injections at depth 1", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_2,                 PROMOTION_INJECTIONS_AT_DEPTH_2, PARCOUNT, "promotion: injections at depth 2", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_3,                 PROMOTION_INJECTIONS_AT_DEPTH_3, PARCOUNT, "promotion: injections at depth 3", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_GT3,               PROMOTION_INJECTIONS_LOWER_THAN_DEPTH_3, PARCOUNT, "promotion: injections lower than depth 3", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_STOP_NONEMPTY_BUF,              PROMOTION_STOPPED_NONEMPTY_BUFFER, PARCOUNT, "promotion: stopped because of a nonempty buffer", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_STOP_H1,                        PROMOTION_STOPPED_AT_HEIGHT_1, PARCOUNT, "promotion: stopped at height 1", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_STOP_LOCK_CHILD,                PROMOTION_STOPPED_CHILD_LOCKED_OR_NOT_IN_MEMORY, PARCOUNT, "promotion: stopped because the child was locked or not at all in memory", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_STOP_CHILD_INMEM,               PROMOTION_STOPPED_CHILD_NOT_FULLY_IN_MEMORY, PARCOUNT, "promotion: stopped because the child was not fully in memory", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(FT_PRO_NUM_DIDNT_WANT_PROMOTE,             PROMOTION_STOPPED_AFTER_LOCKING_CHILD, PARCOUNT, "promotion: stopped anyway, after locking the child", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

STATUS_INIT(FT_DISK_IO_OTHER,                      	   ALL_OTHER_DISK_IO_BYTES, PARCOUNT, "all other disk io, including the descriptors, translation table, ft header, roll log writing", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
STATUS_INIT(FT_DISK_IO_DESCRIPTOR,                         DESCRIPTOR_DISK_IO_BYTES, PARCOUNT, "descriptor bytes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
STATUS_INIT(FT_DISK_IO_TT,                      	   TRANSLATION_TABLE_DISK_IO_BYTES, PARCOUNT, "translation table bytes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
STATUS_INIT(FT_DISK_IO_HEADER,                      	   FT_HEADER_IO_BYTES, PARCOUNT, "ft header bytes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
STATUS_INIT(FT_DISK_IO_PREALLOC,                      	   PREALLOC_DISK_IO_BYTES, PARCOUNT, "preallocation bytes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
STATUS_INIT(FT_DISK_IO_ROLLBACK,                      	   ROLLBACK_DISK_IO_BYTES, PARCOUNT, "rollback log bytes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    ft_status.initialized = true;
}
static void status_destroy(void) {
    for (int i = 0; i < FT_STATUS_NUM_ROWS; ++i) {
        if (ft_status.status[i].type == PARCOUNT) {
            destroy_partitioned_counter(ft_status.status[i].value.parcount);
        }
    }
}
#undef STATUS_INIT

void
toku_ft_get_status(FT_STATUS s) {
    *s = ft_status;
}

#define STATUS_INC(x, d)                                                            \
    do {                                                                            \
        if (ft_status.status[x].type == PARCOUNT) {                                 \
            increment_partitioned_counter(ft_status.status[x].value.parcount, d);   \
        } else {                                                                    \
            toku_sync_fetch_and_add(&ft_status.status[x].value.num, d);             \
        }                                                                           \
    } while (0)

void status_inc(ft_status_entry x, int d) {
    STATUS_INC(x,d);
}
bool is_entire_node_in_memory(FTNODE node) {
    for (int i = 0; i < node->n_children; i++) {
        if(BP_STATE(node,i) != PT_AVAIL) {
	    //printf("height=%d\n", node->height);
            return false;
        }
    }
    return true;
}

void
toku_assert_entire_node_in_memory(FTNODE UU() node) {
    paranoid_invariant(is_entire_node_in_memory(node));
}

uint32_t
get_leaf_num_entries(FTNODE node) {
    uint32_t result = 0;
    int i;
    toku_assert_entire_node_in_memory(node);
    for ( i = 0; i < node->n_children; i++) {
        result += BLB_DATA(node, i)->omt_size();
    }
    return result;
}

static enum reactivity
get_leaf_reactivity (FTNODE node, uint32_t nodesize) {
    enum reactivity re = RE_STABLE;
    toku_assert_entire_node_in_memory(node);
    paranoid_invariant(node->height==0);
    unsigned int size = toku_serialize_ftnode_size(node);
    if (size > nodesize && get_leaf_num_entries(node) > 1) {
        re = RE_FISSIBLE;
    }
    else if ((size*4) < nodesize && !BLB_SEQINSERT(node, node->n_children-1)) {
        re = RE_FUSIBLE;
    }
    return re;
}

enum reactivity
get_nonleaf_reactivity (FTNODE node) {
    paranoid_invariant(node->height>0);
    int n_children = node->n_children;
    if (n_children > TREE_FANOUT) return RE_FISSIBLE;
    if (n_children*4 < TREE_FANOUT) return RE_FUSIBLE;
    return RE_STABLE;
}

enum reactivity
get_node_reactivity (FTNODE node, uint32_t nodesize) {
    toku_assert_entire_node_in_memory(node);
    if (node->height==0)
        return get_leaf_reactivity(node, nodesize);
    else
        return get_nonleaf_reactivity(node);
}

unsigned int
toku_bnc_nbytesinbuf(NONLEAF_CHILDINFO bnc)
{
    return toku_fifo_buffer_size_in_use(bnc->buffer);
}

// return true if the size of the buffers plus the amount of work done is large enough.   (But return false if there is nothing to be flushed (the buffers empty)).
bool
toku_ft_nonleaf_is_gorged (FTNODE node, uint32_t nodesize) {
    uint64_t size = toku_serialize_ftnode_size(node);

    bool buffers_are_empty = true;
    toku_assert_entire_node_in_memory(node);
    //
    // the nonleaf node is gorged if the following holds true:
    //  - the buffers are non-empty
    //  - the total workdone by the buffers PLUS the size of the buffers
    //     is greater than nodesize (which as of Maxwell should be
    //     4MB)
    //
    paranoid_invariant(node->height > 0);
    for (int child = 0; child < node->n_children; ++child) {
        size += BP_WORKDONE(node, child);
    }
    for (int child = 0; child < node->n_children; ++child) {
        if (toku_bnc_nbytesinbuf(BNC(node, child)) > 0) {
            buffers_are_empty = false;
            break;
        }
    }
    return ((size > nodesize)
            &&
            (!buffers_are_empty));
}

static void ft_verify_flags(FT UU(ft), FTNODE UU(node)) {
    paranoid_invariant(ft->h->flags == node->flags);
}

int toku_ft_debug_mode = 0;

uint32_t compute_child_fullhash (CACHEFILE cf, FTNODE node, int childnum) {
    paranoid_invariant(node->height>0);
    paranoid_invariant(childnum<node->n_children);
    return toku_cachetable_hash(cf, BP_BLOCKNUM(node, childnum));
}

int
toku_bnc_n_entries(NONLEAF_CHILDINFO bnc)
{
    return toku_fifo_n_entries(bnc->buffer);
}

struct store_fifo_offset_extra {
    int32_t *offsets;
    int i;
};

int store_fifo_offset(const int32_t &offset, const uint32_t UU(idx), struct store_fifo_offset_extra *const extra) __attribute__((nonnull(3)));

int store_fifo_offset(const int32_t &offset, const uint32_t UU(idx), struct store_fifo_offset_extra *const extra)
{
    extra->offsets[extra->i] = offset;
    extra->i++;
    return 0;
}
/**
 * Given pointers to offsets within a FIFO where we can find messages,
 * figure out the MSN of each message, and compare those MSNs.  Returns 1,
 * 0, or -1 if a is larger than, equal to, or smaller than b.
 */
int fifo_offset_msn_cmp(FIFO &fifo, const int32_t &ao, const int32_t &bo);
int fifo_offset_msn_cmp(FIFO &fifo, const int32_t &ao, const int32_t &bo)
{
    const struct fifo_entry *a = toku_fifo_get_entry(fifo, ao);
    const struct fifo_entry *b = toku_fifo_get_entry(fifo, bo);
    if (a->msn.msn > b->msn.msn) {
        return +1;
    }
    if (a->msn.msn < b->msn.msn) {
        return -1;
    }
    return 0;
}


// how much memory does this child buffer consume?
long
toku_bnc_memory_size(NONLEAF_CHILDINFO bnc)
{
    return (sizeof(*bnc) +
            toku_fifo_memory_footprint(bnc->buffer) +
            bnc->fresh_message_tree.memory_size() +
            bnc->stale_message_tree.memory_size() +
            bnc->broadcast_list.memory_size() +
            bnc->goto_list.memory_size());
}

// how much memory in this child buffer holds useful data?
// originally created solely for use by test program(s).
long
toku_bnc_memory_used(NONLEAF_CHILDINFO bnc)
{
    return (sizeof(*bnc) +
            toku_fifo_memory_size_in_use(bnc->buffer) +
            bnc->fresh_message_tree.memory_size() +
            bnc->stale_message_tree.memory_size() +
            bnc->broadcast_list.memory_size() +
            bnc->goto_list.memory_size());
}

static long
get_avail_internal_node_partition_size(FTNODE node, int i)
{
    paranoid_invariant(node->height > 0);
    return toku_bnc_memory_size(BNC(node, i));
}


static long
ftnode_cachepressure_size(FTNODE node)
{
    long retval = 0;
    bool totally_empty = true;
    if (node->height == 0) {
        goto exit;
    }
    else {
        for (int i = 0; i < node->n_children; i++) {
            if (BP_STATE(node,i) == PT_INVALID || BP_STATE(node,i) == PT_ON_DISK) {
                continue;
            }
            else if (BP_STATE(node,i) == PT_COMPRESSED) {
                SUB_BLOCK sb = BSB(node, i);
                totally_empty = false;
                retval += sb->compressed_size;
            }
            else if (BP_STATE(node,i) == PT_AVAIL) {
                totally_empty = totally_empty && (toku_bnc_n_entries(BNC(node, i)) == 0);
                retval += get_avail_internal_node_partition_size(node, i);
                retval += BP_WORKDONE(node, i);
            }
            else {
                abort();
            }
        }
    }
exit:
    if (totally_empty) {
        return 0;
    }
    return retval;
}

static long
ftnode_memory_size (FTNODE node)
// Effect: Estimate how much main memory a node requires.
{
    long retval = 0;
    int n_children = node->n_children;
    retval += sizeof(*node);
    retval += (n_children)*(sizeof(node->bp[0]));
    retval += node->totalchildkeylens;

    // now calculate the sizes of the partitions
    for (int i = 0; i < n_children; i++) {
        if (BP_STATE(node,i) == PT_INVALID || BP_STATE(node,i) == PT_ON_DISK) {
            continue;
        }
        else if (BP_STATE(node,i) == PT_COMPRESSED) {
            SUB_BLOCK sb = BSB(node, i);
            retval += sizeof(*sb);
            retval += sb->compressed_size;
        }
        else if (BP_STATE(node,i) == PT_AVAIL) {
            if (node->height > 0) {
                retval += get_avail_internal_node_partition_size(node, i);
            }
            else {
                BASEMENTNODE bn = BLB(node, i);
                retval += sizeof(*bn);
                retval += BLB_DATA(node, i)->get_memory_size();
            }
        }
        else {
            abort();
        }
    }

    // shadows
    retval += node->shadow_version.size;
    if (node->shadow_next) {
        assert_zero(node->height);
        retval += ftnode_memory_size(node->shadow_next);
    }

    return retval;
}

PAIR_ATTR make_ftnode_pair_attr(FTNODE node) {
    long size = ftnode_memory_size(node);
    long cachepressure_size = ftnode_cachepressure_size(node);
    PAIR_ATTR result={
        .size = size,
        .nonleaf_size = (node->height > 0) ? size : 0,
        .leaf_size = (node->height > 0) ? 0 : size,
        .rollback_size = 0,
        .cache_pressure_size = cachepressure_size,
        .is_valid = true
    };
    return result;
}

PAIR_ATTR make_invalid_pair_attr(void) {
    PAIR_ATTR result={
        .size = 0,
        .nonleaf_size = 0,
        .leaf_size = 0,
        .rollback_size = 0,
        .cache_pressure_size = 0,
        .is_valid = false
    };
    return result;
}


// assign unique dictionary id
static uint64_t dict_id_serial = 1;
static DICTIONARY_ID
next_dict_id(void) {
    uint64_t i = toku_sync_fetch_and_add(&dict_id_serial, 1);
    assert(i);        // guarantee unique dictionary id by asserting 64-bit counter never wraps
    DICTIONARY_ID d = {.dictid = i};
    return d;
}

//
// Given a BFE and a childnum, returns whether the query that constructed the bfe


//
bool
toku_bfe_wants_child_available (struct ftnode_fetch_extra* bfe, int childnum)
{
    return bfe->type == ftnode_fetch_all ||
        (bfe->child_to_read == childnum &&
         (bfe->type == ftnode_fetch_subset || bfe->type == ftnode_fetch_keymatch));
}

int
toku_bfe_leftmost_child_wanted(struct ftnode_fetch_extra *bfe, FTNODE node)
{
    paranoid_invariant(bfe->type == ftnode_fetch_subset || bfe->type == ftnode_fetch_prefetch || bfe->type == ftnode_fetch_keymatch);
    if (bfe->left_is_neg_infty) {
        return 0;
    } else if (bfe->range_lock_left_key.data == nullptr) {
        return -1;
    } else {
        return toku_ftnode_which_child(node, &bfe->range_lock_left_key, &bfe->h->cmp_descriptor, bfe->h->key_ops.keycmp);
    }
}

int
toku_bfe_rightmost_child_wanted(struct ftnode_fetch_extra *bfe, FTNODE node)
{
    paranoid_invariant(bfe->type == ftnode_fetch_subset || bfe->type == ftnode_fetch_prefetch || bfe->type == ftnode_fetch_keymatch);
    if (bfe->right_is_pos_infty) {
        return node->n_children - 1;
    } else if (bfe->range_lock_right_key.data == nullptr) {
        return -1;
    } else {
        return toku_ftnode_which_child(node, &bfe->range_lock_right_key, &bfe->h->cmp_descriptor, bfe->h->key_ops.keycmp);
    }
}

static int
ft_cursor_rightmost_child_wanted(FT_CURSOR cursor, FT_HANDLE brt, FTNODE node)
{
    if (cursor->right_is_pos_infty) {
        return node->n_children - 1;
    } else if (cursor->range_lock_right_key.data == nullptr) {
        return -1;
    } else {
        return toku_ftnode_which_child(node, &cursor->range_lock_right_key, &brt->ft->cmp_descriptor, brt->ft->key_ops.keycmp);
    }
}

STAT64INFO_S
toku_get_and_clear_basement_stats(FTNODE leafnode) {
    invariant(leafnode->height == 0);
    STAT64INFO_S deltas = ZEROSTATS;
    for (int i = 0; i < leafnode->n_children; i++) {
        BASEMENTNODE bn = BLB(leafnode, i);
        invariant(BP_STATE(leafnode,i) == PT_AVAIL);
        deltas.numrows  += bn->stat64_delta.numrows;
        deltas.numbytes += bn->stat64_delta.numbytes;
        bn->stat64_delta = ZEROSTATS;
    }
    return deltas;
}
static void
ht_0_for_chkpt(uint64_t uncompressed_bytes_flushed, uint64_t bytes_written,
               tokutime_t write_time) {
    STATUS_INC(FT_DISK_FLUSH_LEAF_FOR_CHECKPOINT, 1);
    STATUS_INC(FT_DISK_FLUSH_LEAF_BYTES_FOR_CHECKPOINT, bytes_written);
    STATUS_INC(FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT, uncompressed_bytes_flushed);
    STATUS_INC(FT_DISK_FLUSH_LEAF_TOKUTIME_FOR_CHECKPOINT, write_time);
}

static void
ht_0_not_chkpt(uint64_t uncompressed_bytes_flushed, uint64_t bytes_written,
               tokutime_t write_time) {
    STATUS_INC(FT_DISK_FLUSH_LEAF, 1);
    STATUS_INC(FT_DISK_FLUSH_LEAF_BYTES, bytes_written);
    STATUS_INC(FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES, uncompressed_bytes_flushed);
    STATUS_INC(FT_DISK_FLUSH_LEAF_TOKUTIME, write_time);
}

static void
ht_pos_for_chkpt(uint64_t uncompressed_bytes_flushed, uint64_t bytes_written,
                 tokutime_t write_time) {
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_FOR_CHECKPOINT, 1);
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_BYTES_FOR_CHECKPOINT, bytes_written);
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT, uncompressed_bytes_flushed);
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_TOKUTIME_FOR_CHECKPOINT, write_time);
}

static void
ht_pos_not_chkpt(uint64_t uncompressed_bytes_flushed, uint64_t bytes_written,
                 tokutime_t write_time) {
    STATUS_INC(FT_DISK_FLUSH_NONLEAF, 1);
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_BYTES, bytes_written);
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES, uncompressed_bytes_flushed);
    STATUS_INC(FT_DISK_FLUSH_NONLEAF_TOKUTIME, write_time);
}

void toku_ft_status_update_flush_reason(FTNODE node,
        uint64_t uncompressed_bytes_flushed, uint64_t bytes_written,
        tokutime_t write_time, bool for_checkpoint) {
    if (node->height == 0) {
        if (for_checkpoint) {
            ht_0_for_chkpt(uncompressed_bytes_flushed,
                           bytes_written, write_time);
        }
        else {
            ht_0_not_chkpt(uncompressed_bytes_flushed,
                           bytes_written, write_time);
        }
    }
    else {
        if (for_checkpoint) {
            ht_pos_for_chkpt(uncompressed_bytes_flushed,
                             bytes_written, write_time);
        }
        else {
            ht_pos_not_chkpt(uncompressed_bytes_flushed,
                             bytes_written, write_time);
        }
    }
}

void toku_ft_status_update_io_reason(ft_status_entry entry, uint64_t bytes_written) {
    STATUS_INC(entry, bytes_written);
    STATUS_INC(FT_DISK_IO_OTHER, bytes_written);
}

static void ftnode_update_disk_stats(
    FTNODE ftnode,
    FT ft,
    bool for_checkpoint
    )
{
    STAT64INFO_S deltas = ZEROSTATS;
    // capture deltas before rebalancing basements for serialization
    deltas = toku_get_and_clear_basement_stats(ftnode);
    // locking not necessary here with respect to checkpointing
    // in Clayface (because of the pending lock and cachetable lock
    // in toku_cachetable_begin_checkpoint)
    // essentially, if we are dealing with a for_checkpoint
    // parameter in a function that is called by the flush_callback,
    // then the cachetable needs to ensure that this is called in a safe
    // manner that does not interfere with the beginning
    // of a checkpoint, which it does with the cachetable lock
    // and pending lock
    toku_ft_update_stats(&ft->h->on_disk_stats, deltas);
    if (for_checkpoint) {
        toku_ft_update_stats(&ft->checkpoint_header->on_disk_stats, deltas);
    }
}


static void
ftnode_clone_partitions(FTNODE node, FTNODE cloned_node, bool move_ubi)
{
    for (int i = 0; i < node->n_children; i++) {
        BP_BLOCKNUM(cloned_node, i) = BP_BLOCKNUM(node, i);

        if (BP_LIFT(node, i).size) {
            toku_clone_dbt(&BP_LIFT(cloned_node, i), BP_LIFT(node, i));
        } else {
            toku_init_dbt(&BP_LIFT(cloned_node, i));
        }
        if (BP_NOT_LIFTED(node, i).size) {
            toku_clone_dbt(&BP_NOT_LIFTED(cloned_node, i), BP_NOT_LIFTED(node, i));
        } else {
            toku_init_dbt(&BP_NOT_LIFTED(cloned_node, i));
        }

        assert(BP_STATE(node, i) == PT_AVAIL);
        BP_STATE(cloned_node, i) = PT_AVAIL;
        BP_WORKDONE(cloned_node, i) = BP_WORKDONE(node, i);
        if (node->height == 0) {
            set_BLB(cloned_node, i, toku_clone_bn(BLB(node,i), move_ubi));
            BP_PREFETCH_PFN(cloned_node,i)    = NULL;
            BP_PREFETCH_PFN_CNT(cloned_node,i) = 0;
            BP_PREFETCH_FLAG(cloned_node,i) = false;
            BP_PREFETCH_BUF(cloned_node,i) = NULL;
        }
        else {
            set_BNC(cloned_node, i, toku_clone_nl(BNC(node,i), move_ubi));
        }
    }
}

void toku_ftnode_checkpoint_complete_callback(void *UU(value_data)) {
    return;
}

void ftnode_promise_to_bind_msgs(FTNODE node) {
    assert(has_unbound_msgs(node));
    for (int i=0; i<node->n_children; i++) {
        toku_list * unbound_msgs;
        if (node->height > 0) {
            unbound_msgs = &BNC(node,i)->unbound_inserts;
        } else {
            unbound_msgs = &BLB(node, i)->unbound_inserts;
        }
        struct toku_list * list = unbound_msgs->next;
        while(list!=unbound_msgs) {
            struct ubi_entry *entry = toku_list_struct(list, struct ubi_entry, node_list);
            paranoid_invariant(entry->state == UBI_UNBOUND || entry->state == UBI_QUEUED);
            entry->state = UBI_BINDING;
            list = list->next;
        }
    }
}


void ftnode_reset_unbound_counter(FTNODE node) {
    node->unbound_insert_count = 0;
    for(int i =0 ; i< node->n_children; i++) {
        if(node->height >0 ){
            BNC(node,i)->unbound_insert_count = 0;
        } else {
            BLB(node,i)->unbound_insert_count = 0;
        }
    }
}

#ifdef DEBUG_SEQ_IO
static void toku_assert_all_entries_in_node_are_bound(FTNODE UU(node)){
	for (int i=0; i<node->n_children; i++) {
    		toku_list * unbound_msgs;
    		if (node->height > 0) {
			unbound_msgs = &BNC(node,i)->unbound_inserts;
    		} else {
			unbound_msgs = &BLB(node, i)->unbound_inserts;
    		}
    	struct toku_list * list = unbound_msgs->next;
   	 while(list!=unbound_msgs) {
		struct ubi_entry * entry = toku_list_struct(list, struct ubi_entry, node_list);
		paranoid_invariant(entry->state == UBI_BOUND);
		list = list->next;
    	}
   }
}
#else
#define toku_assert_all_entries_in_node_are_bound(node)
#endif

void ftnode_remove_unbound_insert_list_and_reset_count(FTNODE node) {
    toku_assert_entire_node_in_memory(node);
    toku_assert_all_entries_in_node_are_bound(node);
    for(int i=0; i<node->n_children; i++) {
        if(node->height > 0 ) {
            toku_list_init(&BNC(node,i)->unbound_inserts);
	    BNC(node,i) -> unbound_insert_count = 0;
        } else {
            toku_list_init(&BLB(node, i)->unbound_inserts);
	    BLB(node, i) -> unbound_insert_count = 0;
        }
    }
    node->unbound_insert_count = 0;
}
void ftnode_flip_unbound_msgs_type(FTNODE node) {
    assert(node->unbound_insert_count>0);
    toku_assert_entire_node_in_memory(node);
    if(node->height >0) {
        for(int childnum = 0; childnum < node->n_children; childnum++) {
            NONLEAF_CHILDINFO bnc = BNC(node, childnum);
            toku_fifo_iterate_flip_msg_type(bnc->buffer, FT_UNBOUND_INSERT, FT_INSERT);
        }
    } else { //uxr fix, flip the type in the uxr so gc would work.
        for(int i =0; i < node->n_children; i++) {
            BASEMENTNODE bn = BLB(node, i);
            for (uint32_t j =0 ; j<bn->data_buffer.omt_size(); j++) {
                void* keyp = NULL;
                uint32_t keylen = 0;
                LEAFENTRY leaf_entry;
                LEAFENTRY new_leaf_entry;
                new_leaf_entry = NULL;

                int r = bn->data_buffer.fetch_klpair(j, &leaf_entry, &keylen, &keyp);
                assert_zero(r);
                paranoid_invariant(leaf_entry);
                if(leaf_entry->type < LE_MVCC || leaf_entry->type >= LE_MVCC_END) {
                    continue;
                } else {
                    toku_le_flip_uxr_type(leaf_entry, &bn->data_buffer, j, keyp, keylen, &new_leaf_entry);
                }
            }
        }
    }
}
# if 1
static void printf_slice_key_1(struct toku_db_key_operations * key_ops, DBT * key) {
	key_ops->keyprint(key, false); //use printf
}
#endif
//static void printf_slice_key_2(struct toku_db_key_operations * key_ops, DBT * key) {
//	key_ops->keyprint(key, true);  //use trace_printk
//}

# if 0
static void printf_slice_key(DBT * UU(key)) {
#if 1
	bool is_meta;
	if(!key||key->data==nullptr) {
		toku_trace_printk(" null \n");
		return;
	}
	//char * path = get_path_from_ftfs_key_dbt(key);
	//uint64_t blocknum = get_blocknum_if_ftfs_data_key(key, &is_meta);
	if(is_meta)
		// printf(" meta key: [%s]\n", path);
		toku_trace_printk(" meta key: [%s]\n", path);
	else
		//printf(" data key: [%s:%" PRIu64 "]\n", path, blocknum);
		toku_trace_printk(" data key: [%s:%" PRIu64 "]\n", path, blocknum);

#else
	//only for unit test debugging
	if(!key||key->data==nullptr) {
		printf(" null \n");
		return;
	}
	char * buf = (char *)toku_malloc(key->size+1);
	memset(buf, 0, key->size+1);
	memcpy(buf, key->data, key->size);
	printf("key: [%s]\n", buf);
#endif
}
#endif
#if 0
#define VERIFY_ASSERTION(predicate, i, string) ({                                                                              \
    if(!(predicate)) {                                                                                                         \
        toku_trace_printk("%s:%d: Looking at child %d out of %d children of block %" PRId64 ": %s two keys:\n", __FILE__, __LINE__, i, node->n_children, node->thisnodename.b, string); \
	printf_slice_key(&node->childkeys[i]);\
	printf_slice_key(&node->childkeys[i+1]);\
        assert(predicate);                    \
    }})

static int
compare_pairs (FT ft, const DBT *a, const DBT *b) {
    FAKE_DB(db, &ft->cmp_descriptor);
    int cmp = ft->compare_fun(&db, a, b);
    return cmp;
}
#endif

void toku_ftnode_clone_callback(
    void* value_data,
    void** cloned_value_data,
    long* clone_size,
    PAIR_ATTR* new_attr,
    bool for_checkpoint,
    void* write_extraargs
    )
{
    FTNODE node = static_cast<FTNODE>(value_data);
    toku_assert_entire_node_in_memory(node);
    FT ft = static_cast<FT>(write_extraargs);
    FTNODE XCALLOC(cloned_node);

    if (node->height == 0) {
        // set header stats, must be done before rebalancing
        ftnode_update_disk_stats(node, ft, for_checkpoint);
        // rebalance the leaf node
        rebalance_ftnode_leaf(ft, node, ft->h->basementnodesize);
        //toku_trace_printk("after rebalancing node [%" PRIu64 "]\n", node->thisnodename.b);
    }

    //toku_ft_node_unbound_inserts_validation(node);
    cloned_node->oldest_referenced_xid_known = node->oldest_referenced_xid_known;
    cloned_node->max_msn_applied_to_node_on_disk = node->max_msn_applied_to_node_on_disk;
    cloned_node->flags = node->flags;
    cloned_node->thisnodename = node->thisnodename;
    cloned_node->layout_version = node->layout_version;
    cloned_node->layout_version_original = node->layout_version_original;
    cloned_node->layout_version_read_from_disk = node->layout_version_read_from_disk;
    cloned_node->build_id = node->build_id;
    cloned_node->height = node->height;
    cloned_node->dirty = node->dirty;
    cloned_node->fullhash = node->fullhash;
    cloned_node->n_children = node->n_children;
    cloned_node->totalchildkeylens = node->totalchildkeylens;

    assert(node->n_children > 0);
    if (node->n_children > 1) {
        XMALLOC_N(node->n_children - 1, cloned_node->childkeys);
    } else {
        cloned_node->childkeys = NULL;
    }
    XMALLOC_N(node->n_children, cloned_node->bp);
    // clone pivots
    if (node->bound_l.size) {
        toku_clone_dbt(&cloned_node->bound_l, node->bound_l);
    } else {
        toku_init_dbt(&cloned_node->bound_l);
    }
    if (node->bound_r.size) {
        toku_clone_dbt(&cloned_node->bound_r, node->bound_r);
    } else {
        toku_init_dbt(&cloned_node->bound_r);
    }
    for (int i = 0; i < node->n_children - 1; i++) {
        toku_clone_dbt(&cloned_node->childkeys[i], node->childkeys[i]);
    }
    toku_init_dbt(&cloned_node->shadow_version);
    cloned_node->shadow_next = NULL;

    // clone partition
    ftnode_clone_partitions(node, cloned_node, true);

    cloned_node->unbound_insert_count = node->unbound_insert_count;
    if (node->unbound_insert_count > 0) {
        ftnode_flip_unbound_msgs_type(node);
        ftnode_reset_unbound_counter(node);
    }
    // clear dirty bit
    node->dirty = 0;
    cloned_node->dirty = 0;
    node->layout_version_read_from_disk = FT_LAYOUT_VERSION;
    // set new pair attr if necessary
    if (node->height == 0) {
        *new_attr = make_ftnode_pair_attr(node);
    } else {
        new_attr->is_valid = false;
    }
    *clone_size = ftnode_memory_size(cloned_node);
    *cloned_value_data = cloned_node;
    cloned_node->ct_pair = node->ct_pair;
    toku_ft_node_unbound_inserts_validation(node);
    //toku_ft_node_unbound_inserts_validation(cloned_node);
}

static void ft_leaf_run_gc(FTNODE node, FT ft);

static void
note_diskoff_size_and_bound(FTNODE node, DISKOFF bound_offset, DISKOFF bound_size)
{
    assert(has_unbound_msgs(node));

    toku_list *unbound_msgs;
    for(int i = 0; i < node->n_children; i++) {
        if (node->height > 0) {
            unbound_msgs = &BNC(node, i)->unbound_inserts;
        } else {
            unbound_msgs = &BLB(node, i)->unbound_inserts;
        }
        struct toku_list *list = unbound_msgs->next;
        while (list != unbound_msgs) {
            struct ubi_entry *entry = toku_list_struct(list, struct ubi_entry, node_list);
            if (entry->state != UBI_BINDING && entry->state != UBI_QUEUED)
                assert(0);
            paranoid_invariant(entry->state == UBI_BINDING || entry->state == UBI_QUEUED);
            entry->state = UBI_BOUND;
            entry->diskoff = bound_offset;
            entry->size = bound_size;
            list = list->next;
        }
    }
}

void toku_ftnode_flush_callback(
    CACHEFILE cachefile,
    int fd,
    BLOCKNUM nodename,
    void *ftnode_v,
    void **disk_data,
    void *extraargs,
    PAIR_ATTR UU(size),
    PAIR_ATTR *new_size,
    bool write_me,
    bool keep_me,
    bool for_checkpoint,
    bool is_clone
    )
{
    FT h = (FT)extraargs;
    TOKULOGGER logger = nullptr;
    FTNODE ftnode = (FTNODE) ftnode_v;
    bool is_unbound = has_unbound_msgs(ftnode);

    FTNODE_DISK_DATA* ndd = (FTNODE_DISK_DATA*)disk_data;
    assert(ftnode->thisnodename.b==nodename.b);
    int height = ftnode->height;

    DISKOFF bound_size = 0;
    DISKOFF bound_offset = 0;
    if (write_me) {
        toku_assert_entire_node_in_memory(ftnode);
        if (height == 0) {
            ft_leaf_run_gc(ftnode, h);
        }
        if (height == 0 && !is_clone) {
            ftnode_update_disk_stats(ftnode, h, for_checkpoint);
        }

        bool is_blocking;
        if (ftfs_is_hdd()) {
            is_blocking = true; /* for hdd: we use the old to write node */
        } else if (!for_checkpoint) {
            is_blocking = false; /* for evictor and partial checkpoint, for_checkpoint is off. We async write the ftnode to reduce dio write latency. Integrity can be guaranteed later */
        } else {
            is_blocking = true; /* for real checkpointer: there is a fsync very soon, we need to wait for data to be durable any way. The benefit to async write is marginal. */
        }

	int r = toku_serialize_ftnode_to(fd, ftnode->thisnodename, ftnode, ndd, !is_clone, h, for_checkpoint, &bound_size, &bound_offset, is_blocking);
	if(is_unbound) {
		//grab the fat link lock--do i have to ? FIXME
		logger = toku_cachefile_logger(cachefile);
		toku_mutex_lock(&logger->ubi_lock);

		// ref count of physical block + 1
		block_table_log_get_block(h->blocktable, bound_offset);

		//update the logger ubi hashtable with diskoff and size
		note_diskoff_size_and_bound(ftnode, bound_offset, bound_size);
        	//FIXME remove the unbound msg list from the node??

                ftnode_remove_unbound_insert_list_and_reset_count(ftnode);
                //FIXME Bill, who would free unbound_msg_entry that were just bound from the in/out list in logger? will you do it upon log flush?

		toku_mutex_unlock(&logger->ubi_lock);
	}
        //debugging code
	toku_ft_node_empty_unbound_inserts_validation(ftnode);

	assert_zero(r);
        ftnode->layout_version_read_from_disk = FT_LAYOUT_VERSION;
    }
    if (!keep_me) {
        if (!is_clone) {
            long node_size = ftnode_memory_size(ftnode);
            if (ftnode->height == 0) {
                STATUS_INC(FT_FULL_EVICTIONS_LEAF, 1);
                STATUS_INC(FT_FULL_EVICTIONS_LEAF_BYTES, node_size);
            } else {
                STATUS_INC(FT_FULL_EVICTIONS_NONLEAF, 1);
                STATUS_INC(FT_FULL_EVICTIONS_NONLEAF_BYTES, node_size);
            }
            toku_free(*disk_data);
        }
        else {
            if (ftnode->height == 0) {
                for (int i = 0; i < ftnode->n_children; i++) {
                    if (BP_STATE(ftnode,i) == PT_AVAIL) {
                        BASEMENTNODE bn = BLB(ftnode, i);
                        toku_ft_decrease_stats(&h->in_memory_stats, bn->stat64_delta);
                    }
                }
            }
        }
        toku_ftnode_free(&ftnode);
    }
    else {
        *new_size = make_ftnode_pair_attr(ftnode);
    }
}
#if 0
void toku_ftnode_flush_callback(
    CACHEFILE UU(cachefile),
    int fd,
    BLOCKNUM nodename,
    void *ftnode_v,
    void** disk_data,
    void *extraargs,
    PAIR_ATTR size __attribute__((unused)),
    PAIR_ATTR* new_size,
    bool write_me,
    bool keep_me,
    bool for_checkpoint,
    bool is_clone
    )
{
    FT h = (FT) extraargs;
    FTNODE ftnode = (FTNODE) ftnode_v;
    FTNODE_DISK_DATA* ndd = (FTNODE_DISK_DATA*)disk_data;
    assert(ftnode->thisnodename.b==nodename.b);
    int height = ftnode->height;
    if (write_me) {
        toku_assert_entire_node_in_memory(ftnode);
        if (height == 0) {
            ft_leaf_run_gc(ftnode, h);
        }
        if (height == 0 && !is_clone) {
            ftnode_update_disk_stats(ftnode, h, for_checkpoint);
        }
        //debugging code
        toku_ft_node_empty_unbound_inserts_validation(ftnode);
    }
    if (!keep_me) {
        if (!is_clone) {
            long node_size = ftnode_memory_size(ftnode);
            if (ftnode->height == 0) {
                STATUS_INC(FT_FULL_EVICTIONS_LEAF, 1);
                STATUS_INC(FT_FULL_EVICTIONS_LEAF_BYTES, node_size);
            } else {
                STATUS_INC(FT_FULL_EVICTIONS_NONLEAF, 1);
                STATUS_INC(FT_FULL_EVICTIONS_NONLEAF_BYTES, node_size);
            }
            toku_free(*disk_data);
        }
        else {
            if (ftnode->height == 0) {
                for (int i = 0; i < ftnode->n_children; i++) {
                    if (BP_STATE(ftnode,i) == PT_AVAIL) {
                        BASEMENTNODE bn = BLB(ftnode, i);
                        toku_ft_decrease_stats(&h->in_memory_stats, bn->stat64_delta);
                    }
                }
            }
        }
        toku_ftnode_free(&ftnode);
    }
    else {
        *new_size = make_ftnode_pair_attr(ftnode);
    }
}
#endif

void
toku_ft_status_update_pivot_fetch_reason(struct ftnode_fetch_extra *bfe)
{
    if (bfe->type == ftnode_fetch_prefetch) {
        STATUS_INC(FT_NUM_PIVOTS_FETCHED_PREFETCH, 1);
        STATUS_INC(FT_BYTES_PIVOTS_FETCHED_PREFETCH, bfe->bytes_read);
        STATUS_INC(FT_TOKUTIME_PIVOTS_FETCHED_PREFETCH, bfe->io_time);
    } else if (bfe->type == ftnode_fetch_all) {
        STATUS_INC(FT_NUM_PIVOTS_FETCHED_WRITE, 1);
        STATUS_INC(FT_BYTES_PIVOTS_FETCHED_WRITE, bfe->bytes_read);
        STATUS_INC(FT_TOKUTIME_PIVOTS_FETCHED_WRITE, bfe->io_time);
    } else if (bfe->type == ftnode_fetch_subset || bfe->type == ftnode_fetch_keymatch) {
        STATUS_INC(FT_NUM_PIVOTS_FETCHED_QUERY, 1);
        STATUS_INC(FT_BYTES_PIVOTS_FETCHED_QUERY, bfe->bytes_read);
        STATUS_INC(FT_TOKUTIME_PIVOTS_FETCHED_QUERY, bfe->io_time);
    }
}

FTNODE ft_create_shadow(FTNODE node, DBT *version)
{
    assert_zero(node->height);
    FTNODE XCALLOC(shadow);
    shadow->oldest_referenced_xid_known = node->oldest_referenced_xid_known;
    shadow->max_msn_applied_to_node_on_disk = node->max_msn_applied_to_node_on_disk;
    shadow->flags = node->flags;
    shadow->thisnodename = node->thisnodename;
    shadow->layout_version = node->layout_version;
    shadow->layout_version_original = node->layout_version_original;
    shadow->layout_version_read_from_disk = node->layout_version_read_from_disk;
    shadow->build_id = node->build_id;
    shadow->height = node->height;
    shadow->dirty = 0;
    shadow->fullhash = node->fullhash;
    shadow->n_children = node->n_children;
    shadow->totalchildkeylens = node->totalchildkeylens;

    assert(node->n_children > 0);
    if (node->n_children > 1) {
        XMALLOC_N(node->n_children - 1, shadow->childkeys);
    } else {
        shadow->childkeys = NULL;
    }
    XMALLOC_N(node->n_children, shadow->bp);
    // clone pivots
    if (node->bound_l.size) {
        toku_clone_dbt(&shadow->bound_l, node->bound_l);
    } else {
        toku_init_dbt(&shadow->bound_l);
    }
    if (node->bound_r.size) {
        toku_clone_dbt(&shadow->bound_r, node->bound_r);
    } else {
        toku_init_dbt(&shadow->bound_r);
    }
    for (int i = 0; i < node->n_children - 1; i++) {
        toku_clone_dbt(&shadow->childkeys[i], node->childkeys[i]);
    }
    if (version->size) {
        toku_clone_dbt(&shadow->shadow_version, *version);
    } else {
        toku_init_dbt(&shadow->shadow_version);
    }
    // init BNs
    for (int i = 0; i < node->n_children; ++i) {
        BP_BLOCKNUM(shadow, i) = BP_BLOCKNUM(node, i);

        assert_zero(BP_LIFT(node, i).size);
        toku_init_dbt(&BP_LIFT(shadow, i));
        assert_zero(BP_NOT_LIFTED(node, i).size);
        toku_init_dbt(&BP_NOT_LIFTED(shadow, i));

        BP_STATE(shadow, i) = PT_ON_DISK;
        set_BNULL(shadow, i);
    }

    // add to the shadow list
    shadow->shadow_next = node->shadow_next;
    node->shadow_next = shadow;

    return shadow;
}

void ft_update_shadow(FTNODE node, FTNODE shadow, bool *node_changed)
{
    assert(node->n_children == shadow->n_children);
    assert_zero(node->height);
    assert_zero(shadow->height);

    for (int i = 0; i < node->n_children; i++) {
        if (BP_STATE(node, i) == PT_AVAIL && BP_STATE(shadow, i) != PT_AVAIL) {
            BP_STATE(shadow, i) = PT_AVAIL;
            set_BLB(shadow, i, toku_clone_bn(BLB(node, i), false));
            *node_changed = true;
        }
    }
}

FTNODE toku_ft_get_shadow(FTNODE node, DBT *version)
{
    assert_zero(node->height);

    for (FTNODE curr = node->shadow_next; curr; curr = curr->shadow_next) {
        if (version->size == curr->shadow_version.size &&
            !memcmp(version->data, curr->shadow_version.data, version->size))
        {
            return curr;
        }
    }

    return NULL;
}

int toku_ftnode_fetch_callback (CACHEFILE UU(cachefile), PAIR p, int fd, BLOCKNUM nodename, uint32_t fullhash,
                                 void **ftnode_pv,  void** disk_data, PAIR_ATTR *sizep, int *dirtyp, void *extraargs) {
    assert(extraargs);
    assert(*ftnode_pv == NULL);
    FTNODE_DISK_DATA* ndd = (FTNODE_DISK_DATA*)disk_data;
    struct ftnode_fetch_extra *bfe = (struct ftnode_fetch_extra *)extraargs;
    FTNODE *node=(FTNODE*)ftnode_pv;
    // deserialize the node, must pass the bfe in because we cannot
    // evaluate what piece of the the node is necessary until we get it at
    // least partially into memory
    int r = toku_deserialize_ftnode_from(fd, nodename, fullhash, node, ndd, bfe);
    if (r != 0) {
        if (r == TOKUDB_BAD_CHECKSUM) {
            fprintf(stderr,
                    "Checksum failure while reading node in file %s.\n",
                    toku_cachefile_fname_in_env(cachefile));
        } else {
            fprintf(stderr, "Error deserializing node, errno = %d", r);
        }
        // make absolutely sure we crash before doing anything else.
        abort();
    }

    if (r == 0) {
        *sizep = make_ftnode_pair_attr(*node);
        (*node)->ct_pair = p;
        *dirtyp = (*node)->dirty;  // deserialize could mark the node as dirty (presumably for upgrade)
    }
    return r;
}

void toku_ftnode_pe_est_callback(
    void* ftnode_pv,
    void* disk_data,
    long* bytes_freed_estimate,
    enum partial_eviction_cost *cost,
    void* UU(write_extraargs)
    )
{
    paranoid_invariant(ftnode_pv != NULL);
    long bytes_to_free = 0;
    FTNODE node = static_cast<FTNODE>(ftnode_pv);
    if (node->dirty || node->height == 0 ||
        node->layout_version_read_from_disk < FT_FIRST_LAYOUT_VERSION_WITH_BASEMENT_NODES) {
        *bytes_freed_estimate = 0;
        *cost = PE_CHEAP;
        goto exit;
    }

    //
    // we are dealing with a clean internal node
    //
    *cost = PE_EXPENSIVE;
    // now lets get an estimate for how much data we can free up
    // we estimate the compressed size of data to be how large
    // the compressed data is on disk
    for (int i = 0; i < node->n_children; i++) {
        if (BP_STATE(node,i) == PT_AVAIL && BP_SHOULD_EVICT(node,i)) {
            // calculate how much data would be freed if
            // we compress this node and add it to
            // bytes_to_free

            // first get an estimate for how much space will be taken
            // after compression, it is simply the size of compressed
            // data on disk plus the size of the struct that holds it
            FTNODE_DISK_DATA ndd = (FTNODE_DISK_DATA) disk_data;
            uint32_t compressed_data_size = BP_SIZE(ndd, i);
            compressed_data_size += sizeof(struct sub_block);

            // now get the space taken now
            uint32_t decompressed_data_size = get_avail_internal_node_partition_size(node,i);
            bytes_to_free += (decompressed_data_size - compressed_data_size);
        }
    }

    *bytes_freed_estimate = bytes_to_free;
exit:
    return;
}

static void
compress_internal_node_partition(FTNODE node, int i, enum toku_compression_method compression_method)
{
    // if we should evict, compress the
    // message buffer into a sub_block
    assert(BP_STATE(node, i) == PT_AVAIL);
    assert(node->height > 0);
    SUB_BLOCK XMALLOC(sb);
    sub_block_init(sb);
    toku_create_compressed_partition_from_available(node, i, compression_method, sb);

    // now free the old partition and replace it with this
    destroy_nonleaf_childinfo(BNC(node,i));
    set_BSB(node, i, sb);
    BP_STATE(node,i) = PT_COMPRESSED;
}

void toku_evict_bn_from_memory(FTNODE node, int childnum, FT h) {
    // free the basement node
    assert(!node->dirty);
    BASEMENTNODE bn = BLB(node, childnum);
    assert_zero(bn->unbound_insert_count);
    toku_ft_decrease_stats(&h->in_memory_stats, bn->stat64_delta);
    destroy_basement_node(bn);
    set_BNULL(node, childnum);
    BP_STATE(node, childnum) = PT_ON_DISK;
}

BASEMENTNODE toku_detach_bn(FTNODE node, int childnum) {
    assert(BP_STATE(node, childnum) == PT_AVAIL);
    BASEMENTNODE bn = BLB(node, childnum);
    set_BNULL(node, childnum);
    BP_STATE(node, childnum) = PT_ON_DISK;
    return bn;
}

// callback for partially evicting a node
int toku_ftnode_pe_callback (void *ftnode_pv, PAIR_ATTR UU(old_attr), PAIR_ATTR* new_attr, void* extraargs) {
    FTNODE node = (FTNODE)ftnode_pv;
    FT ft = (FT) extraargs;
    // Don't partially evict dirty nodes
    if (node->dirty) {
        goto exit;
    }
    // Don't partially evict nodes whose partitions can't be read back
    // from disk individually
    if (node->layout_version_read_from_disk < FT_FIRST_LAYOUT_VERSION_WITH_BASEMENT_NODES) {
        goto exit;
    }
    //
    // partial eviction for nonleaf nodes
    //
    if (node->height > 0) {
        for (int i = 0; i < node->n_children; i++) {
            if (BP_STATE(node,i) == PT_AVAIL) {
                if (BP_SHOULD_EVICT(node,i)) {
                    long size_before = ftnode_memory_size(node);
                    compress_internal_node_partition(node, i, ft->h->compression_method);
                    long delta = size_before - ftnode_memory_size(node);
                    STATUS_INC(FT_PARTIAL_EVICTIONS_NONLEAF, 1);
                    STATUS_INC(FT_PARTIAL_EVICTIONS_NONLEAF_BYTES, delta);
                }
                else {
                    BP_SWEEP_CLOCK(node,i);
                }
            }
            else {
                continue;
            }
        }
    }
    //
    // partial eviction strategy for basement nodes:
    //  if the bn is compressed, evict it
    //  else: check if it requires eviction, if it does, evict it, if not, sweep the clock count
    //
    else {
        for (int i = 0; i < node->n_children; i++) {
            // Get rid of compressed stuff no matter what.
            if (BP_STATE(node,i) == PT_COMPRESSED) {
                long size_before = ftnode_memory_size(node);
                SUB_BLOCK sb = BSB(node, i);
                toku_free(sb->compressed_ptr);
                toku_free(sb);
                set_BNULL(node, i);
                BP_STATE(node,i) = PT_ON_DISK;
                long delta = size_before - ftnode_memory_size(node);
                STATUS_INC(FT_PARTIAL_EVICTIONS_LEAF, 1);
                STATUS_INC(FT_PARTIAL_EVICTIONS_LEAF_BYTES, delta);
            }
            else if (BP_STATE(node,i) == PT_AVAIL) {
                if (BP_SHOULD_EVICT(node,i)) {
                    long size_before = ftnode_memory_size(node);
                    toku_evict_bn_from_memory(node, i, ft);
                    long delta = size_before - ftnode_memory_size(node);
                    STATUS_INC(FT_PARTIAL_EVICTIONS_LEAF, 1);
                    STATUS_INC(FT_PARTIAL_EVICTIONS_LEAF_BYTES, delta);
                }
                else {
                    BP_SWEEP_CLOCK(node,i);
                }
            }
            else if (BP_STATE(node,i) == PT_ON_DISK) {
                continue;
            }
            else {
                abort();
            }
        }
    }

exit:
    *new_attr = make_ftnode_pair_attr(node);
    return 0;
}

// We touch the clock while holding a read lock.
// DRD reports a race but we want to ignore it.
// Using a valgrind suppressions file is better than the DRD_IGNORE_VAR macro because it's more targeted.
// We need a function to have something a drd suppression can reference
// see src/tests/drd.suppressions (unsafe_touch_clock)
static void unsafe_touch_clock(FTNODE node, int i) {
    BP_TOUCH_CLOCK(node, i);
}

// Callback that states if a partial fetch of the node is necessary
// Currently, this function is responsible for the following things:
//  - reporting to the cachetable whether a partial fetch is required (as required by the contract of the callback)
//  - A couple of things that are NOT required by the callback, but we do for efficiency and simplicity reasons:
//   - for queries, set the value of bfe->child_to_read so that the query that called this can proceed with the query
//      as opposed to having to evaluate toku_ft_search_which_child again. This is done to make the in-memory query faster
//   - touch the necessary partition's clock. The reason we do it here is so that there is one central place it is done, and not done
//      by all the various callers
//
bool toku_ftnode_pf_req_callback(void* ftnode_pv, void* read_extraargs) {
    // placeholder for now
    bool retval = false;
    FTNODE node = (FTNODE) ftnode_pv;
    struct ftnode_fetch_extra *bfe = (struct ftnode_fetch_extra *) read_extraargs;
    //
    // The three types of fetches that the brt layer may request are:
    //  - ftnode_fetch_none: no partitions are necessary (example use: stat64)
    //  - ftnode_fetch_subset: some subset is necessary (example use: toku_ft_search)
    //  - ftnode_fetch_all: entire node is necessary (example use: flush, split, merge)
    // The code below checks if the necessary partitions are already in memory,
    // and if they are, return false, and if not, return true
    //
    if (bfe->type == ftnode_fetch_none) {
        retval = false;
    }
    else if (bfe->type == ftnode_fetch_all) {
        retval = false;
        for (int i = 0; i < node->n_children; i++) {
            unsafe_touch_clock(node,i);
            // if we find a partition that is not available,
            // then a partial fetch is required because
            // the entire node must be made available
            if (BP_STATE(node,i) != PT_AVAIL) {
                retval = true;
            }
        }
    }
    else if (bfe->type == ftnode_fetch_subset) {
        // we do not take into account prefetching yet
        // as of now, if we need a subset, the only thing
        // we can possibly require is a single basement node
        // we find out what basement node the query cares about
        // and check if it is available
        paranoid_invariant(bfe->h->key_ops.keycmp);
        paranoid_invariant(bfe->search);
        bfe->child_to_read = toku_ft_search_which_child(
            bfe->h,
            node,
            bfe->search
            );
        unsafe_touch_clock(node,bfe->child_to_read);
        // child we want to read is not available, must set retval to true
        retval = (BP_STATE(node, bfe->child_to_read) != PT_AVAIL);
    }
    else if (bfe->type == ftnode_fetch_prefetch) {
        // makes no sense to have prefetching disabled
        // and still call this function
        paranoid_invariant(!bfe->disable_prefetching);
        int lc;
        int rc;
        if (ftfs_is_hdd()) {
            lc = toku_bfe_leftmost_child_wanted(bfe, node);
            rc = toku_bfe_rightmost_child_wanted(bfe, node);
            for (int i = lc; i <= rc; ++i) {
                if (BP_STATE(node, i) != PT_AVAIL) {
                    retval = true;
                }
            }
        } else {
            retval = true;
        }
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
                unsafe_touch_clock(node,bfe->child_to_read);
                // child we want to read is not available, must set retval to true
                retval = (BP_STATE(node, bfe->child_to_read) != PT_AVAIL);
            }
        }
    } else {
        // we have a bug. The type should be known
        abort();
    }
    return retval;
}

// kernel stack size is small, this might cause problems
static void
ft_status_update_partial_fetch_reason_is_leaf(
    struct ftnode_fetch_extra* bfe,
    int childnum,
    enum pt_state state) {
    if (bfe->type == ftnode_fetch_prefetch) {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_BASEMENTS_DECOMPRESSED_PREFETCH, 1);
        } else {
            STATUS_INC(FT_NUM_BASEMENTS_FETCHED_PREFETCH, 1);
            STATUS_INC(FT_BYTES_BASEMENTS_FETCHED_PREFETCH, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_BASEMENTS_FETCHED_PREFETCH, bfe->io_time);
        }
    } else if (bfe->type == ftnode_fetch_all) {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_BASEMENTS_DECOMPRESSED_WRITE, 1);
        } else {
            STATUS_INC(FT_NUM_BASEMENTS_FETCHED_WRITE, 1);
            STATUS_INC(FT_BYTES_BASEMENTS_FETCHED_WRITE, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_BASEMENTS_FETCHED_WRITE, bfe->io_time);
        }
    } else if (childnum == bfe->child_to_read) {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_BASEMENTS_DECOMPRESSED_NORMAL, 1);
        } else {
            STATUS_INC(FT_NUM_BASEMENTS_FETCHED_NORMAL, 1);
            STATUS_INC(FT_BYTES_BASEMENTS_FETCHED_NORMAL, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_BASEMENTS_FETCHED_NORMAL, bfe->io_time);
        }
    } else {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_BASEMENTS_DECOMPRESSED_AGGRESSIVE, 1);
        } else {
            STATUS_INC(FT_NUM_BASEMENTS_FETCHED_AGGRESSIVE, 1);
            STATUS_INC(FT_BYTES_BASEMENTS_FETCHED_AGGRESSIVE, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_BASEMENTS_FETCHED_AGGRESSIVE, bfe->io_time);
        }
    }
}

// kernel stack size is small, this might cause problems
static void
ft_status_update_partial_fetch_reason_not_leaf(
    struct ftnode_fetch_extra* bfe,
    int childnum,
    enum pt_state state) {
    if (bfe->type == ftnode_fetch_prefetch) {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_MSG_BUFFER_DECOMPRESSED_PREFETCH, 1);
        } else {
            STATUS_INC(FT_NUM_MSG_BUFFER_FETCHED_PREFETCH, 1);
            STATUS_INC(FT_BYTES_MSG_BUFFER_FETCHED_PREFETCH, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_MSG_BUFFER_FETCHED_PREFETCH, bfe->io_time);
        }
    } else if (bfe->type == ftnode_fetch_all) {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_MSG_BUFFER_DECOMPRESSED_WRITE, 1);
        } else {
            STATUS_INC(FT_NUM_MSG_BUFFER_FETCHED_WRITE, 1);
            STATUS_INC(FT_BYTES_MSG_BUFFER_FETCHED_WRITE, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_MSG_BUFFER_FETCHED_WRITE, bfe->io_time);
        }
    } else if (childnum == bfe->child_to_read) {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_MSG_BUFFER_DECOMPRESSED_NORMAL, 1);
        } else {
            STATUS_INC(FT_NUM_MSG_BUFFER_FETCHED_NORMAL, 1);
            STATUS_INC(FT_BYTES_MSG_BUFFER_FETCHED_NORMAL, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_MSG_BUFFER_FETCHED_NORMAL, bfe->io_time);
        }
    } else {
        if (state == PT_COMPRESSED) {
            STATUS_INC(FT_NUM_MSG_BUFFER_DECOMPRESSED_AGGRESSIVE, 1);
        } else {
            STATUS_INC(FT_NUM_MSG_BUFFER_FETCHED_AGGRESSIVE, 1);
            STATUS_INC(FT_BYTES_MSG_BUFFER_FETCHED_AGGRESSIVE, bfe->bytes_read);
            STATUS_INC(FT_TOKUTIME_MSG_BUFFER_FETCHED_AGGRESSIVE, bfe->io_time);
        }
    }
}

static void
ft_status_update_partial_fetch_reason(
    struct ftnode_fetch_extra* bfe,
    int childnum,
    enum pt_state state,
    bool is_leaf
    )
{
    invariant(state == PT_COMPRESSED || state == PT_ON_DISK);
    if (is_leaf) {
        ft_status_update_partial_fetch_reason_is_leaf(bfe, childnum, state);
    }
    else {
        ft_status_update_partial_fetch_reason_not_leaf(bfe, childnum, state);
    }
}

void toku_ft_status_update_serialize_times(FTNODE node, tokutime_t serialize_time, tokutime_t compress_time) {
    if (node->height == 0) {
        STATUS_INC(FT_LEAF_SERIALIZE_TOKUTIME, serialize_time);
        STATUS_INC(FT_LEAF_COMPRESS_TOKUTIME, compress_time);
    } else {
        STATUS_INC(FT_NONLEAF_SERIALIZE_TOKUTIME, serialize_time);
        STATUS_INC(FT_NONLEAF_COMPRESS_TOKUTIME, compress_time);
    }
}

void toku_ft_status_update_deserialize_times(FTNODE node, tokutime_t deserialize_time, tokutime_t decompress_time) {
    if (node->height == 0) {
        STATUS_INC(FT_LEAF_DESERIALIZE_TOKUTIME, deserialize_time);
        STATUS_INC(FT_LEAF_DECOMPRESS_TOKUTIME, decompress_time);
    } else {
        STATUS_INC(FT_NONLEAF_DESERIALIZE_TOKUTIME, deserialize_time);
        STATUS_INC(FT_NONLEAF_DECOMPRESS_TOKUTIME, decompress_time);
    }
}

// callback for partially reading a node
// could have just used toku_ftnode_fetch_callback, but wanted to separate the two cases to separate functions
int
toku_ftnode_pf_callback(void *ftnode_pv, void *disk_data, void *read_extraargs,
                        int fd, PAIR_ATTR* sizep)
{
    int r = 0;
    FTNODE node = (FTNODE)ftnode_pv;
    FTNODE_DISK_DATA ndd = (FTNODE_DISK_DATA)disk_data;
    struct ftnode_fetch_extra *bfe = (struct ftnode_fetch_extra *)read_extraargs;
    // there must be a reason this is being called.
    // If we get a garbage type or the type is ftnode_fetch_none,
    // then something went wrong
    assert((bfe->type == ftnode_fetch_subset)   ||
           (bfe->type == ftnode_fetch_all)      ||
           (bfe->type == ftnode_fetch_prefetch) ||
           (bfe->type == ftnode_fetch_keymatch));
    // determine the range to prefetch
    int lc, rc;
    if (!bfe->disable_prefetching && bfe->type == ftnode_fetch_subset)
    {
        lc = toku_bfe_leftmost_child_wanted(bfe, node);
        rc = toku_bfe_rightmost_child_wanted(bfe, node);
    } else if (!bfe->disable_prefetching && bfe->type == ftnode_fetch_prefetch) {
        if (ftfs_is_hdd()) {
            lc = toku_bfe_leftmost_child_wanted(bfe, node);
            rc = toku_bfe_rightmost_child_wanted(bfe, node);
        } else {
            lc = 0;
            rc = node->n_children - 1;
        }
    } else {
        lc = -1;
        rc = -1;
    }

    for (int i = 0; i < node->n_children; i++) {
        if (BP_STATE(node, i) == PT_AVAIL) {
            continue;
        }
        if ((lc <= i && i <= rc) || toku_bfe_wants_child_available(bfe, i)) {
            enum pt_state state = BP_STATE(node, i);
            if (state == PT_COMPRESSED) {
                r = toku_deserialize_bp_from_compressed(node, i, bfe);
            } else {
                invariant(state == PT_ON_DISK);
                r = toku_deserialize_bp_from_disk(node, ndd, i, fd, bfe);

                int child_to_prefetch = i + 1;
                int nr_prefetch = 2;
                while (child_to_prefetch <= node->n_children-1 && nr_prefetch > 0) {
                    // do not use this optimizaiton for HDD
                    if (ftfs_is_hdd()) {
                       break;
                    }
                    // do not use this optimizaiton for random read
                    if (bfe->is_seqread == false) {
                       break;
                    }
                    if (BP_STATE(node, child_to_prefetch) == PT_ON_DISK && BP_PREFETCH_FLAG(node, child_to_prefetch) == false) {
                       nr_prefetch--;
                    } else {
                       child_to_prefetch++;
                       continue;
                    }
                    // Get the disk address first
                    BP_PREFETCH_FLAG(node, child_to_prefetch) = true;
                    //BP_PREFETCH_AVAIL(node, child_to_prefetch) = false;
                    DISKOFF node_offset, node_size;
                    toku_translate_blocknum_to_offset_size(bfe->h->blocktable, node->thisnodename, &node_offset, &node_size);
                    uint32_t curr_size = BP_SIZE(ndd, child_to_prefetch);
                    uint32_t curr_offset = BP_START(ndd, child_to_prefetch);
                    uint32_t pad_at_beginning = (node_offset+curr_offset)%BLOCK_ALIGNMENT;
                    uint32_t padded_size = roundup_to_multiple(BLOCK_ALIGNMENT, pad_at_beginning + curr_size);
                    int nr_pages = padded_size / BLOCK_ALIGNMENT;
                    if (BP_PREFETCH_PFN(node, child_to_prefetch) != NULL) {
                        printf("%s: pfns=%p\n", __func__, BP_PREFETCH_PFN(node, child_to_prefetch));
                        printf("%s: flag=%d\n", __func__, BP_PREFETCH_FLAG(node, child_to_prefetch));
                        printf("%s: cnt=%d\n", __func__, BP_PREFETCH_PFN_CNT(node, child_to_prefetch));
                        printf("%s: buf=%p\n", __func__, BP_PREFETCH_BUF(node, child_to_prefetch));
                        assert(false);
                    }
                    unsigned long *pfns;
                    XMALLOC_N(nr_pages, pfns);
                    unsigned char *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, padded_size, buf);

                    BP_PREFETCH_PFN(node, child_to_prefetch) = pfns;
                    BP_PREFETCH_PFN_CNT(node, child_to_prefetch) = nr_pages;
                    BP_PREFETCH_BUF(node, child_to_prefetch) = buf;
                    for (int p = 0; p < nr_pages; p++) {
                        unsigned long addr = (unsigned long)buf;
                        pfns[p] = get_kernfs_pfn(addr + p * BLOCK_ALIGNMENT);
                        sb_lock_read_page(pfns[p]);
                    }
                    sb_read_pages(fd, pfns, nr_pages, node_offset+curr_offset-pad_at_beginning);
                    //printf("%s: prefetch is done; i=%d, child_to_prefetch=%d\n", __func__, i, child_to_prefetch);
                    child_to_prefetch++;
                }
            }
            ft_status_update_partial_fetch_reason(bfe, i, state, (node->height == 0));
        }
        if (r != 0) {
            if (r == TOKUDB_BAD_CHECKSUM) {
                fprintf(stderr,
                        "Checksum failure while reading node partition in file %s.\n",
                        toku_cachefile_fname_in_env(bfe->h->cf));
            }
            abort();
        }
    }

    *sizep = make_ftnode_pair_attr(node);

    return 0;
}

struct cmd_leafval_heaviside_extra {
    ft_compare_func compare_fun;
    DESCRIPTOR desc;
    DBT const * const key;
};

//TODO: #1125 optimize
static int
toku_cmd_leafval_heaviside(DBT const &kdbt, const struct cmd_leafval_heaviside_extra &be) {
    FAKE_DB(db, be.desc);
    DBT const * const key = be.key;
    return be.compare_fun(&db, &kdbt, key);
}

static int
ft_compare_pivot(DESCRIPTOR desc, ft_compare_func cmp, const DBT *key, const DBT *pivot)
{
    int r;
    FAKE_DB(db, desc);
    r = cmp(&db, key, pivot);
    return r;
}


// destroys the internals of the ftnode, but it does not free the values
// that are stored
// this is common functionality for toku_ftnode_free and rebalance_ftnode_leaf
// MUST NOT do anything besides free the structures that have been allocated
void toku_destroy_ftnode_internals(FTNODE node)
{
    if (node->n_children > 1) {
        if (node->childkeys == NULL){
            printf("node->childkeys == NULL while n_children is %d\n", node->n_children);
            assert(false);
        }
        for (int i = 0; i < node->n_children - 1; i++) {
            toku_destroy_dbt(&node->childkeys[i]);
        }
        toku_free(node->childkeys);
    }
    if (node->n_children > 0) {
        for (int i=0; i < node->n_children; i++) {
            if (node->height == 0) {
                if (BP_PREFETCH_FLAG(node,i)) {
                    toku_free(BP_PREFETCH_PFN(node, i));
                    toku_free(BP_PREFETCH_BUF(node, i));
                    BP_PREFETCH_FLAG(node,i)=false;
                }
            }

            if (BP_STATE(node,i) == PT_AVAIL) {
                if (node->height > 0) {
                    destroy_nonleaf_childinfo(BNC(node, i));
                } else {
                    destroy_basement_node(BLB(node, i));
                }
            } else if (BP_STATE(node,i) == PT_COMPRESSED) {
                SUB_BLOCK sb = BSB(node, i);
                toku_free(sb->compressed_ptr);
                toku_free(sb);
            } else {
                assert(is_BNULL(node, i));
            }
            set_BNULL(node, i);
            toku_destroy_dbt(&BP_LIFT(node, i));
            toku_destroy_dbt(&BP_NOT_LIFTED(node, i));
        }
        toku_free(node->bp);
    }
    node->childkeys = NULL;
    node->bp = NULL;
    node->unbound_insert_count = 0;
    node->n_children = 0;
}

void toku_destroy_ftnode_shadows(FTNODE node)
{
    FTNODE tmp_node = node->shadow_next, next;
    while (tmp_node != NULL) {
        next = tmp_node->shadow_next;
        toku_destroy_ftnode_internals(tmp_node);
        toku_destroy_dbt(&tmp_node->bound_l);
        toku_destroy_dbt(&tmp_node->bound_r);
        toku_destroy_dbt(&tmp_node->shadow_version);
        toku_free(tmp_node);
        tmp_node = next;
    }
    node->shadow_next = NULL;
}

/* Frees a node, including all the stuff in the hash table. */
void toku_ftnode_free(FTNODE *nodep) {
    FTNODE node = *nodep;
    if (node->height == 0) {
        STATUS_INC(FT_DESTROY_LEAF, 1);
        if (node->shadow_next) {
          toku_destroy_ftnode_shadows(*nodep);
        }
    } else {
        STATUS_INC(FT_DESTROY_NONLEAF, 1);
        assert_zero((*nodep)->shadow_next);
    }
    toku_destroy_ftnode_internals(node);
    // free here because toku_destroy_ftnode_internals is also called in
    //   rebalance_ftnode_leaf
    toku_cleanup_dbt(&node->bound_l);
    toku_cleanup_dbt(&node->bound_r);
    if (node->shadow_next){
      toku_cleanup_dbt(&node->shadow_version);
    }
    toku_free(node);
    *nodep = nullptr;
}

void
toku_initialize_empty_ftnode (FTNODE n, BLOCKNUM nodename, int height, int num_children, int layout_version, unsigned int flags)
// Effect: Fill in N as an empty ftnode.
{
    paranoid_invariant(layout_version != 0);
    paranoid_invariant(height >= 0);

    if (height == 0) {
        STATUS_INC(FT_CREATE_LEAF, 1);
    } else {
        STATUS_INC(FT_CREATE_NONLEAF, 1);
    }

    n->max_msn_applied_to_node_on_disk = ZERO_MSN;    // correct value for root node, harmless for others
    n->flags = flags;
    n->thisnodename = nodename;
    n->layout_version = layout_version;
    n->layout_version_original = layout_version;
    n->layout_version_read_from_disk = layout_version;
    n->height = height;
    n->totalchildkeylens = 0;
    n->childkeys = 0;
    n->bp = 0;
    n->n_children = num_children;
    n->oldest_referenced_xid_known = TXNID_NONE;
    n->unbound_insert_count = 0;
    if (num_children > 0) {
        if (num_children > 1) {
            XCALLOC_N(num_children - 1, n->childkeys);
        } else {
            n->childkeys = NULL;
        }
        XMALLOC_N(num_children, n->bp);
        for (int i = 0; i < num_children; i++) {
            BP_BLOCKNUM(n, i).b = 0;
            BP_STATE(n, i) = PT_INVALID;
            BP_WORKDONE(n, i) = 0;
            BP_INIT_TOUCHED_CLOCK(n, i);
            set_BNULL(n,i);
            toku_init_dbt(&BP_LIFT(n, i));
            toku_init_dbt(&BP_NOT_LIFTED(n, i));
            if (height > 0) {
                set_BNC(n, i, toku_create_empty_nl());
                BP_PREFETCH_FLAG(n,i) = false;
            } else {
                set_BLB(n, i, toku_create_empty_bn());
                BP_PREFETCH_PFN(n,i)    = NULL;
                BP_PREFETCH_PFN_CNT(n,i) = 0;
                BP_PREFETCH_FLAG(n,i) = false;
                BP_PREFETCH_BUF(n,i) = NULL;
            }
        }
    } else {
        n->childkeys = NULL;
        n->bp = NULL;
    }
    toku_init_dbt(&n->bound_l);
    toku_init_dbt(&n->bound_r);
    toku_init_dbt(&n->shadow_version);
    n->shadow_next = NULL;
    n->dirty = 1;  // special case exception, it's okay to mark as dirty because the basements are empty
}

static void
ft_init_new_root(FT ft, FTNODE oldroot, FTNODE *newrootp)
// Effect:  Create a new root node whose two children are the split of oldroot.
//  oldroot is unpinned in the process.
//  Leave the new root pinned.
{
    FTNODE newroot;

    BLOCKNUM old_blocknum = oldroot->thisnodename;
    uint32_t old_fullhash = oldroot->fullhash;
    PAIR old_pair = oldroot->ct_pair;

    int new_height = oldroot->height+1;
    uint32_t new_fullhash;
    BLOCKNUM new_blocknum;
    PAIR new_pair = NULL;

    cachetable_put_empty_node_with_dep_nodes(
        ft,
        1,
        &oldroot,
        &new_blocknum,
        &new_fullhash,
        &newroot
        );
    new_pair = newroot->ct_pair;

    assert(newroot);
    assert(new_height > 0);
    toku_initialize_empty_ftnode (
        newroot,
        new_blocknum,
        new_height,
        1,
        ft->h->layout_version,
        ft->h->flags
        );
    MSN msna = oldroot->max_msn_applied_to_node_on_disk;
    newroot->max_msn_applied_to_node_on_disk = msna;
    BP_STATE(newroot,0) = PT_AVAIL;
    newroot->dirty = 1;

    // now do the "switcheroo"
    BP_BLOCKNUM(newroot,0) = new_blocknum;
    newroot->thisnodename = old_blocknum;
    newroot->fullhash = old_fullhash;
    newroot->ct_pair = old_pair;

    oldroot->thisnodename = new_blocknum;
    oldroot->fullhash = new_fullhash;
    oldroot->ct_pair = new_pair;

    toku_cachetable_swap_pair_values(old_pair, new_pair);

    toku_ft_split_child(
        ft,
        newroot,
        0, // childnum to split
        oldroot,
        SPLIT_EVENLY
        );

    // ft_split_child released locks on newroot
    // and oldroot, so now we repin and
    // return to caller
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, ft);
    toku_pin_ftnode_off_client_thread(
        ft,
        old_blocknum,
        old_fullhash,
        &bfe,
        PL_WRITE_EXPENSIVE, // may_modify_node
        0,
        NULL,
        newrootp
        );
}

static void
ft_init_new_root_no_split_no_unlock(FT ft, FTNODE oldroot, FTNODE *newrootp)
{
    FTNODE newroot;

    BLOCKNUM old_blocknum = oldroot->thisnodename;
    uint32_t old_fullhash = oldroot->fullhash;
    PAIR old_pair = oldroot->ct_pair;

    int new_height = oldroot->height + 1;
    uint32_t new_fullhash;
    BLOCKNUM new_blocknum;
    PAIR new_pair = NULL;

    cachetable_put_empty_node_with_dep_nodes(
        ft, 1, &oldroot, &new_blocknum, &new_fullhash, &newroot);
    new_pair = newroot->ct_pair;

    assert(newroot);
    assert(new_height > 0);
    toku_initialize_empty_ftnode(
        newroot, new_blocknum, new_height, 1,
        ft->h->layout_version, ft->h->flags);
    newroot->max_msn_applied_to_node_on_disk =
        oldroot->max_msn_applied_to_node_on_disk;
    BP_STATE(newroot, 0) = PT_AVAIL;
    newroot->dirty = 1;

    BP_BLOCKNUM(newroot, 0) = new_blocknum;
    newroot->thisnodename = old_blocknum;
    newroot->fullhash = old_fullhash;
    newroot->ct_pair = old_pair;

    oldroot->thisnodename = new_blocknum;
    oldroot->fullhash = new_fullhash;
    oldroot->ct_pair = new_pair;

    toku_cachetable_swap_pair_values(old_pair, new_pair);

    *newrootp = newroot;
}

static void
init_childinfo(FTNODE node, int childnum, FTNODE child) {
    BP_BLOCKNUM(node,childnum) = child->thisnodename;
    BP_STATE(node,childnum) = PT_AVAIL;
    BP_WORKDONE(node, childnum) = 0;
    toku_init_dbt(&BP_LIFT(node, childnum));
    toku_init_dbt(&BP_NOT_LIFTED(node, childnum));
    set_BNC(node, childnum, toku_create_empty_nl());
}

static void
init_childkey(FTNODE node, int childnum, const DBT *pivotkey) {
    toku_clone_dbt(&node->childkeys[childnum], *pivotkey);
    node->totalchildkeylens += pivotkey->size;
}

#ifdef DEBUG_SEQ_IO
//if it is not empty, return;
//else check
//this is a fast version of validation that only checks on empty unbound inserts.
void toku_ft_node_empty_unbound_inserts_validation(FTNODE UU(node)) {
	if(node->unbound_insert_count > 0) return;
	else {
		for(int i = 0; i < node->n_children; i++) {
			if(node->height>0) {
				if(!toku_list_empty(&BNC(node,i)->unbound_inserts) || BNC(node, i)->unbound_insert_count>0)
					printf("\nnode %p failed zero validation at %dth bnc child\n", node, i);
				paranoid_invariant(toku_list_empty(&BNC(node, i) -> unbound_inserts));
				paranoid_invariant(BNC(node, i)->unbound_insert_count == 0);
			} else {
				if(!toku_list_empty(&BLB(node,i)->unbound_inserts) || BLB(node, i)->unbound_insert_count>0)
					printf("\nnode %p failed zero validation at %dth blb child\n", node, i);
				paranoid_invariant(toku_list_empty(&BLB(node, i) -> unbound_inserts));
				paranoid_invariant(BLB(node, i)->unbound_insert_count == 0);
		}
	    }
	}
}

static void verify_unbound_list_count(toku_list * head, int count, FTNODE UU(node)) {
	toku_list * item = head->next;
	int i = 0;
	while(item != head) {
		i++;
		item = item->next;
	}
	if(count!=i)
	   printf("%s,%d:count =%d, i = %d\n", __func__, __LINE__, count, i);
	paranoid_invariant(count == i);
}

static void
basement_node_gc_all_les(BASEMENTNODE bn,
                         const xid_omt_t &snapshot_xids,
                         const rx_omt_t &referenced_xids,
                         const xid_omt_t &live_root_txns,
                         TXNID oldest_referenced_xid_known,
                         STAT64INFO_S * delta);
static int
//count the unbound ule in a basement nodes
ft_basement_node_count_unbound_ules(BASEMENTNODE bn) ;

void toku_ft_node_unbound_inserts_validation(FTNODE UU(node), FT_MSG UU(msg), int UU(line)) {
//	printf("\n%s:%d, going to validate node:%p\n", __func__, __LINE__, node);
	int childnum = node->n_children;
	int n_unbound_insert_node = node->unbound_insert_count;
	int sum_child_ubi = 0;

        xid_omt_t snapshot_txnids;
        rx_omt_t referenced_xids;
        xid_omt_t live_root_txns;
	if(global_logger) {
        	toku_txn_manager_clone_state_for_gc(
            		global_logger->txn_manager,
            		&snapshot_txnids,
            		&referenced_xids,
            		&live_root_txns
            		);
	}

	for(int i = 0; i < childnum; i++) {
		toku_list * head;
		if(BP_STATE(node,i) != PT_AVAIL)
			continue;

		if(node->height>0) {
			head = &BNC(node, i) -> unbound_inserts;
			int n_unbound_insert_count_bnc = BNC(node, i)->unbound_insert_count;
			verify_unbound_list_count(head, n_unbound_insert_count_bnc, node);
			sum_child_ubi += n_unbound_insert_count_bnc;
		} else {
			BASEMENTNODE bn = BLB(node, i);
			int ule_unbound_count = ft_basement_node_count_unbound_ules(bn);
 			if(ule_unbound_count != bn->unbound_insert_count){
				 printf("\n unmatch num of unbound in the bn!!%d ules unbound but bn count is %d\n", ule_unbound_count, bn->unbound_insert_count);
				//printf("\n dumping the buggy bn:\n");
				//toku_print_bn(bn);
				if(msg)
					printf("the fatal msg type is %d\n", ft_msg_get_type(msg));
				if(line)
					printf("invalidation called from line %d\n", line);
			}
			assert(ule_unbound_count == bn->unbound_insert_count);
			# if 0
			if(bn->data_buffer.omt_size() < bn->unbound_insert_count){
				printf("before the gc, omt_size = %d, unbound count = %d", bn->data_buffer.omt_size(), bn->unbound_insert_count);
				if(msg)
					printf("the fatal msg type is %d\n", ft_msg_get_type(msg));
				if(line)
					printf("invalidation called from line %d\n", line);
				toku_dump_stack();
			}

			assert(bn->data_buffer.omt_size() >= bn->unbound_insert_count);
			#endif
        		#if 0
			STAT64INFO_S delta;
        		delta.numrows = 0;
        		delta.numbytes = 0;
			#endif
			head = &BLB(node, i) -> unbound_inserts;
			#if 0
			if(global_logger)
				basement_node_gc_all_les(bn, snapshot_txnids, referenced_xids, live_root_txns, node->oldest_referenced_xid_known, &delta);
			if(bn->data_buffer.omt_size() < bn->unbound_insert_count){
				printf("omt_size = %d, unbound count = %d\n", bn->data_buffer.omt_size(), bn->unbound_insert_count);
				if(msg)
					printf("the fatal msg type is %d\n", ft_msg_get_type(msg));

				if(line)
					printf("invalidation called from line %d\n", line);
				toku_dump_stack();
			}
			assert(bn->data_buffer.omt_size() >= bn->unbound_insert_count);
			#endif
			int n_unbound_insert_count_blb = BLB(node, i)->unbound_insert_count;
			verify_unbound_list_count(head, n_unbound_insert_count_blb, node);
			sum_child_ubi += n_unbound_insert_count_blb;
		}
	}

	if(n_unbound_insert_node != sum_child_ubi) {
		printf("%s:%d, n_unbound_insert_node=%d, sum_child_ubi=%d\n", __func__, __LINE__,n_unbound_insert_node, sum_child_ubi);
	}
	paranoid_invariant(n_unbound_insert_node == sum_child_ubi);
}
#else /* DEBUG_SEQ_IO */
void toku_ft_node_unbound_inserts_validation(FTNODE UU(node), FT_MSG UU(msg), int UU(line))
{
    return;
}
void toku_ft_node_empty_unbound_inserts_validation(FTNODE UU(node))
{
    return;
}
#endif /* DEBUG_SEQ_IO */

// Used only by test programs: append a child node to a parent node
void
toku_ft_nonleaf_append_child(FTNODE node, FTNODE child, const DBT *pivotkey) {
    int childnum = node->n_children;
    node->n_children++;
    XREALLOC_N(node->n_children-1, node->n_children, node->bp);
    init_childinfo(node, childnum, child);
    XREALLOC_N(node->n_children-2, node->n_children-1, node->childkeys);
    if (pivotkey) {
        invariant(childnum > 0);
        init_childkey(node, childnum-1, pivotkey);
    }
    node->dirty = 1;
}

void
toku_ft_bn_apply_cmd_once (
    BASEMENTNODE bn,
    struct ubi_entry *UU(ubi_entry),
    const FT_MSG cmd,
    uint32_t idx,
    LEAFENTRY le,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info,
    uint64_t *workdone,
    STAT64INFO stats_to_update
    )
// Effect: Apply cmd to leafentry (msn is ignored)
//         Calculate work done by message on leafentry and add it to caller's workdone counter.
//   idx is the location where it goes
//   le is old leafentry
{
    size_t newsize=0, oldsize=0, workdone_this_le=0;
    LEAFENTRY new_le=0;
    int64_t numbytes_delta = 0;  // how many bytes of user data (not including overhead) were added or deleted from this row
    int64_t numrows_delta = 0;   // will be +1 or -1 or 0 (if row was added or deleted or not)
    uint32_t key_storage_size = ft_msg_get_keylen(cmd) + sizeof(uint32_t);
    if (le) {
        oldsize = leafentry_memsize(le) + key_storage_size;
    }

    if (ft_msg_get_type(cmd) == FT_UNBOUND_INSERT && ubi_entry) {
        bn->unbound_insert_count++;
        toku_list_push(&bn->unbound_inserts, &ubi_entry->node_list);
    }

    // toku_le_apply_msg() may call mempool_malloc_from_omt() to allocate more space.
    // That means le is guaranteed to not cause a sigsegv but it may point to a mempool that is
    // no longer in use.  We'll have to release the old mempool later.
    toku_le_apply_msg(
        cmd,
        le,
        &bn->data_buffer,
        idx,
        oldest_referenced_xid,
        gc_info,
        &new_le,
        &numbytes_delta
        );

    newsize = new_le ? (leafentry_memsize(new_le) +  + key_storage_size) : 0;
    if (le && new_le) {
        workdone_this_le = (oldsize > newsize ? oldsize : newsize);  // work done is max of le size before and after message application

    } else {           // we did not just replace a row, so ...
        if (le) {
            //            ... we just deleted a row ...
            workdone_this_le = oldsize;
            numrows_delta = -1;
        }
        if (new_le) {
            //            ... or we just added a row
            workdone_this_le = newsize;
            numrows_delta = 1;
        }
    }
    if (workdone) {  // test programs may call with NULL
        *workdone += workdone_this_le;
    }

    // now update stat64 statistics
    bn->stat64_delta.numrows  += numrows_delta;
    bn->stat64_delta.numbytes += numbytes_delta;
    // the only reason stats_to_update may be null is for tests
    if (stats_to_update) {
        stats_to_update->numrows += numrows_delta;
        stats_to_update->numbytes += numbytes_delta;
    }

}

static const uint32_t setval_tag = 0xee0ccb99; // this was gotten by doing "cat /dev/random|head -c4|od -x" to get a random number.  We want to make sure that the user actually passes us the setval_extra_s that we passed in.
struct setval_extra_s {
    uint32_t  tag;
    bool did_set_val;
    int         setval_r;    // any error code that setval_fun wants to return goes here.
    // need arguments for toku_ft_bn_apply_cmd_once
    BASEMENTNODE bn;
    MSN msn;              // captured from original message, not currently used
    XIDS xids;
    const DBT *key;
    uint32_t idx;
    LEAFENTRY le;
    TXNID oldest_referenced_xid;
    GC_INFO gc_info;
    uint64_t * workdone;  // set by toku_ft_bn_apply_cmd_once()
    STAT64INFO stats_to_update;
};

/*
 * If new_val == NULL, we send a delete message instead of an insert.
 * This happens here instead of in do_delete() for consistency.
 * setval_fun() is called from handlerton, passing in svextra_v
 * from setval_extra_s input arg to brt->update_fun().
 */
static void setval_fun (const DBT *new_val, void *svextra_v) {
    struct setval_extra_s *CAST_FROM_VOIDP(svextra, svextra_v);
    paranoid_invariant(svextra->tag==setval_tag);
    paranoid_invariant(!svextra->did_set_val);
    svextra->did_set_val = true;

    {
        // can't leave scope until toku_ft_bn_apply_cmd_once if
        // this is a delete
        DBT val;
        FT_MSG_S msg;
        ft_msg_init(&msg, FT_NONE, svextra->msn, svextra->xids,
                        svextra->key, NULL);
        if (new_val) {
            msg.type = FT_INSERT;
            msg.val = new_val;
        } else {
            msg.type = FT_DELETE_ANY;
            toku_init_dbt(&val);
            msg.val = &val;
        }

        // SOSP TODO: this cannot be called for an FT_UNBOUND_INSERT...
        toku_ft_bn_apply_cmd_once(svextra->bn, nullptr, &msg,
                                  svextra->idx, svextra->le,
                                  svextra->oldest_referenced_xid, svextra->gc_info,
                                  svextra->workdone, svextra->stats_to_update);
        svextra->setval_r = 0;
    }
}

// We are already past the msn filter (in toku_ft_bn_apply_cmd(), which calls do_update()),
// so capturing the msn in the setval_extra_s is not strictly required.         The alternative
// would be to put a dummy msn in the messages created by setval_fun(), but preserving
// the original msn seems cleaner and it preserves accountability at a lower layer.
static int do_update(ft_update_func update_fun, DESCRIPTOR desc, BASEMENTNODE bn, FT_MSG cmd, uint32_t idx,
                     LEAFENTRY le,
                     void* keydata,
                     uint32_t keylen,
                     TXNID oldest_referenced_xid,
                     GC_INFO gc_info,
                     uint64_t * workdone,
                     STAT64INFO stats_to_update) {
    LEAFENTRY le_for_update;
    DBT key;
    const DBT *keyp;
    const DBT *update_function_extra;
    DBT vdbt;
    const DBT *vdbtp;

    // the location of data depends whether this is a regular or
    // broadcast update
    if (cmd->type == FT_UPDATE) {
        // key is passed in with command (should be same as from le)
        // update function extra is passed in with command
        STATUS_INC(FT_UPDATES, 1);
        keyp = cmd->key;
        update_function_extra = cmd->val;
    } else if (cmd->type == FT_UPDATE_BROADCAST_ALL) {
        // key is not passed in with broadcast, it comes from le
        // update function extra is passed in with command
        paranoid_invariant(le);  // for broadcast updates, we just hit all leafentries
                     // so this cannot be null
        paranoid_invariant(keydata);
        paranoid_invariant(keylen);
        paranoid_invariant(cmd->key->size == 0);
        STATUS_INC(FT_UPDATES_BROADCAST, 1);
        keyp = toku_fill_dbt(&key, keydata, keylen);
        update_function_extra = cmd->val;
    } else {
        abort();
    }

    if (le && !le_latest_is_del(le)) {
        // if the latest val exists, use it, and we'll use the leafentry later
        uint32_t vallen;
        void *valp = le_latest_val_and_len(le, &vallen);
        vdbtp = toku_fill_dbt(&vdbt, valp, vallen);
    } else {
        // otherwise, the val and leafentry are both going to be null
        vdbtp = NULL;
    }
    le_for_update = le;

    struct setval_extra_s setval_extra = {setval_tag, false, 0, bn, cmd->msn, cmd->xids,
                                          keyp, idx, le_for_update, oldest_referenced_xid, gc_info, workdone, stats_to_update};
    // call handlerton's brt->update_fun(), which passes setval_extra to setval_fun()
    FAKE_DB(db, desc);
    int r = update_fun(
        &db,
        keyp,
        vdbtp,
        update_function_extra,
        setval_fun, &setval_extra
        );

    if (r == 0) { r = setval_extra.setval_r; }
    return r;
}


static bool le_needs_broadcast_msg_applied(FT_MSG msg, LEAFENTRY le) {
    enum ft_msg_type type = ft_msg_get_type(msg);
    XIDS xids = ft_msg_get_xids(msg);
    paranoid_invariant(ft_msg_type_applies_multiple(type));
    switch (type) {
    case FT_OPTIMIZE_FOR_UPGRADE:
    case FT_COMMIT_BROADCAST_ALL:
    case FT_OPTIMIZE:
        return !le_is_clean(le);
    case FT_COMMIT_BROADCAST_TXN:
    case FT_ABORT_BROADCAST_TXN:
        return le_has_xids(le, xids);
    default:
        // in case we miss any in the future
        return true;
    }
}
static void find_idx_for_msg(
    ft_compare_func compare_fun,
    BASEMENTNODE bn,
    FT_MSG msg,
    DESCRIPTOR desc,
    bool use_max, // for multicast messages
    uint32_t* idx, // output, position where we are to apply the message
    LEAFENTRY* old_le, // output, old leafentry we are to overwrite, if NULL, means we are to insert at idx
    void** key,
    uint32_t* keylen // output, keylen
    )
{
    int r;
    struct cmd_leafval_heaviside_extra be = {compare_fun, desc, use_max?msg->max_key:msg->key};
    bool is_insert = (msg->type== FT_INSERT || msg->type == FT_INSERT_NO_OVERWRITE);
    paranoid_invariant(msg->type != FT_UNBOUND_INSERT);
    unsigned int doing_seqinsert = bn->seqinsert;
    bn->seqinsert = 0;
    if (is_insert && doing_seqinsert) {
        *idx = bn->data_buffer.omt_size();
        DBT kdbt;
        r = bn->data_buffer.fetch_le_key_and_len((*idx)-1, &kdbt.size, &kdbt.data);
        if (r != 0) goto fz;
        int c = toku_cmd_leafval_heaviside(kdbt, be);
        if (c >= 0) goto fz;
        r = DB_NOTFOUND;
    } else {
    fz:
        r = bn->data_buffer.find_zero<decltype(be), toku_cmd_leafval_heaviside>(
            be,
            old_le,
            key,
            keylen,
            idx
            );
    }
    if (r==DB_NOTFOUND) {
        *old_le = nullptr;
    } else {
        assert_zero(r);
    }

    // if the insertion point is within a window of the right edge of
    // the leaf then it is sequential
    // window = min(32, number of leaf entries/16)
    if (is_insert) {
        uint32_t s = bn->data_buffer.omt_size();
        uint32_t w = s / 16;
        if (w == 0) w = 1;
        if (w > 32) w = 32;

        // within the window?
        if (s - *idx <= w)
            bn->seqinsert = doing_seqinsert + 1;
    }
}
// Should be renamed as something like "apply_cmd_to_basement()."
void
toku_ft_bn_apply_cmd (
    FT ft, // SOSP TODO: will be used to update unbound_cmd_count for nodes
    DESCRIPTOR desc,
    struct ubi_entry *ubi_entry,
    BASEMENTNODE bn,
    FT_MSG cmd,
    TXNID oldest_referenced_xid_known,
    GC_INFO gc_info,
    uint64_t *workdone,
    STAT64INFO stats_to_update
    )
// Effect:
//   Put a cmd into a leaf.
//   Calculate work done by message on leafnode and add it to caller's workdone counter.
// The leaf could end up "too big" or "too small".  The caller must fix that up.
{
    LEAFENTRY storeddata;
    void* key = NULL;
    uint32_t keylen = 0;

    uint32_t omt_size;
    int r;
    struct cmd_leafval_heaviside_extra be = {ft->key_ops.keycmp, desc, cmd->key};

    unsigned int doing_seqinsert = bn->seqinsert;
    bn->seqinsert = 0;

    switch (cmd->type) {
    case FT_INSERT_NO_OVERWRITE:
    case FT_INSERT:
    case FT_UNBOUND_INSERT: {
        uint32_t idx;
        if (doing_seqinsert) {
            idx = bn->data_buffer.omt_size();
            DBT kdbt;
            r = bn->data_buffer.fetch_le_key_and_len(idx-1, &kdbt.size, &kdbt.data);
            if (r != 0) goto fz;
            int cmp = toku_cmd_leafval_heaviside(kdbt, be);
            if (cmp >= 0) goto fz;
            r = DB_NOTFOUND;
        } else {
        fz:
            r = bn->data_buffer.find_zero<decltype(be), toku_cmd_leafval_heaviside>(
                be,
                &storeddata,
                &key,
                &keylen,
                &idx
                );
        }
        if (r==DB_NOTFOUND) {
            storeddata = 0;
        } else {
            assert_zero(r);
        }
        toku_ft_bn_apply_cmd_once(bn, ubi_entry, cmd, idx, storeddata, oldest_referenced_xid_known, gc_info, workdone, stats_to_update);

        // if the insertion point is within a window of the right edge of
        // the leaf then it is sequential
        // window = min(32, number of leaf entries/16)
        {
            uint32_t s = bn->data_buffer.omt_size();
            uint32_t w = s / 16;
            if (w == 0) w = 1;
            if (w > 32) w = 32;

            // within the window?
            if (s - idx <= w)
                bn->seqinsert = doing_seqinsert + 1;
        }
        break;
    }
    case FT_DELETE_ANY:
    case FT_ABORT_ANY:
    case FT_COMMIT_ANY: {
        uint32_t idx;
        // Apply to all the matches

        r = bn->data_buffer.find_zero<decltype(be), toku_cmd_leafval_heaviside>(
            be,
            &storeddata,
            &key,
            &keylen,
            &idx
            );
        if (r == DB_NOTFOUND) break;
        assert_zero(r);
        toku_ft_bn_apply_cmd_once(bn, nullptr, cmd, idx, storeddata, oldest_referenced_xid_known, gc_info, workdone, stats_to_update);
        break;
    }
    case FT_OPTIMIZE_FOR_UPGRADE:
        // fall through so that optimize_for_upgrade performs rest of the optimize logic
    case FT_COMMIT_BROADCAST_ALL:
    case FT_OPTIMIZE:
        // Apply to all leafentries
        omt_size = bn->data_buffer.omt_size();
        for (uint32_t idx = 0; idx < omt_size; ) {
            DBT curr_keydbt;
            void* curr_keyp = NULL;
            uint32_t curr_keylen = 0;
            r = bn->data_buffer.fetch_klpair(idx, &storeddata, &curr_keylen, &curr_keyp);
            assert_zero(r);
            toku_fill_dbt(&curr_keydbt, curr_keyp, curr_keylen);
            // because this is a broadcast message, we need
            // to fill the key in the msg that we pass into toku_ft_bn_apply_cmd_once
            cmd->key = &curr_keydbt;
            int deleted = 0;
            if (!le_is_clean(storeddata)) { //If already clean, nothing to do.
                toku_ft_bn_apply_cmd_once(bn, nullptr, cmd, idx, storeddata, oldest_referenced_xid_known, gc_info, workdone, stats_to_update);
                uint32_t new_omt_size = bn->data_buffer.omt_size();
                if (new_omt_size != omt_size) {
                    paranoid_invariant(new_omt_size+1 == omt_size);
                    //Item was deleted.
                    deleted = 1;
                }
            }
            if (deleted)
                omt_size--;
            else
                idx++;
        }
        paranoid_invariant(bn->data_buffer.omt_size() == omt_size);

        break;
    case FT_COMMIT_BROADCAST_TXN:
    case FT_ABORT_BROADCAST_TXN:
        // Apply to all leafentries if txn is represented
        omt_size = bn->data_buffer.omt_size();
        for (uint32_t idx = 0; idx < omt_size; ) {
            DBT curr_keydbt;
            void* curr_keyp = NULL;
            uint32_t curr_keylen = 0;
            r = bn->data_buffer.fetch_klpair(idx, &storeddata, &curr_keylen, &curr_keyp);
            assert_zero(r);
            toku_fill_dbt(&curr_keydbt, curr_keyp, curr_keylen);
            // because this is a broadcast message, we need
            // to fill the key in the msg that we pass into toku_ft_bn_apply_cmd_once
            cmd->key = &curr_keydbt;
            int deleted = 0;
            if (le_has_xids(storeddata, cmd->xids)) {
                toku_ft_bn_apply_cmd_once(bn, nullptr, cmd, idx, storeddata, oldest_referenced_xid_known, gc_info, workdone, stats_to_update);
                uint32_t new_omt_size = bn->data_buffer.omt_size();
                if (new_omt_size != omt_size) {
                    paranoid_invariant(new_omt_size+1 == omt_size);
                    //Item was deleted.
                    deleted = 1;
                }
            }
            if (deleted)
                omt_size--;
            else
                idx++;
        }
        paranoid_invariant(bn->data_buffer.omt_size() == omt_size);

        break;
    case FT_UPDATE: {
        uint32_t idx;
        r = bn->data_buffer.find_zero<decltype(be), toku_cmd_leafval_heaviside>(
            be,
            &storeddata,
            &key,
            &keylen,
            &idx
            );
        if (r==DB_NOTFOUND) {
            {
                //Point to msg's copy of the key so we don't worry about le being freed
                //TODO: 46 MAYBE Get rid of this when le_apply message memory is better handled
                key = cmd->key->data;
                keylen = cmd->key->size;
            }
            r = do_update(ft->update_fun, desc, bn, cmd, idx, NULL,
                          NULL, 0, oldest_referenced_xid_known,
                          gc_info, workdone, stats_to_update);
        } else if (r==0) {
            r = do_update(ft->update_fun, desc, bn, cmd, idx, storeddata,
                          key, keylen, oldest_referenced_xid_known,
                          gc_info, workdone, stats_to_update);
        } // otherwise, a worse error, just return it
        break;
    }
    case FT_UPDATE_BROADCAST_ALL: {
        // apply to all leafentries.
        uint32_t idx = 0;
        uint32_t num_leafentries_before;
        while (idx < (num_leafentries_before = bn->data_buffer.omt_size())) {
            void* curr_key = nullptr;
            uint32_t curr_keylen = 0;
            r = bn->data_buffer.fetch_klpair(idx, &storeddata, &curr_keylen, &curr_key);
            assert_zero(r);

            //TODO: 46 replace this with something better than cloning key
            // TODO: (Zardosht) This may be unnecessary now, due to how the key
            // is handled in the bndata. Investigate and determine
            char clone_mem[curr_keylen];  // only lasts one loop, alloca would overflow (end of function)
            memcpy((void*)clone_mem, curr_key, curr_keylen);
            curr_key = (void*)clone_mem;

            // This is broken below. Have a compilation error checked
            // in as a reminder
            r = do_update(ft->update_fun, desc, bn, cmd, idx, storeddata,
                          curr_key, curr_keylen, oldest_referenced_xid_known,
                          gc_info, workdone, stats_to_update);
            assert_zero(r);

            if (num_leafentries_before == bn->data_buffer.omt_size()) {
                // we didn't delete something, so increment the index.
                idx++;
            }
        }
        break;
    }
    case FT_NONE: break; // don't do anything
    case FT_DELETE_MULTICAST:
    case FT_COMMIT_MULTICAST_TXN:
    case FT_COMMIT_MULTICAST_ALL:
    case FT_ABORT_MULTICAST_TXN: {
        // Apply to all leafentries
        uint32_t num_klpairs = bn->data_buffer.omt_size();
        uint32_t start_idx = 0;
        uint32_t end_idx = num_klpairs;
        enum ft_msg_type type = ft_msg_get_type(cmd);
        if (ft_msg_type_is_multicast(type)) {
            void * max_key = ft_msg_get_max_key(cmd);
            LEAFENTRY old_le = nullptr;
            bool is_right_excl = ft_msg_is_multicast_rightexcl(cmd);
            find_idx_for_msg(ft->key_ops.keycmp, bn, cmd, desc, false,
                             &start_idx, &old_le, &key, &keylen);
            old_le = nullptr;
            find_idx_for_msg(ft->key_ops.keycmp, bn, cmd, desc, true,
                             &end_idx, &old_le, &key, &keylen);
            if (old_le == nullptr || (is_right_excl && !memcmp(key, max_key, keylen))) {
               //ugly fix for int underflow for now.
                if(end_idx == 0) return;
                end_idx--;
            }
        }
        // why are all the indexes using unsigned int? there is apparently potential underflow bug when start_idx is 0 -JYM
        for (uint32_t idx = start_idx; idx <= end_idx; ) {
            void* curr_keyp = NULL;
            uint32_t curr_keylen = 0;
            num_klpairs = bn->data_buffer.omt_size();
            r = bn->data_buffer.fetch_klpair(idx, &storeddata, &curr_keylen, &curr_keyp);
            assert_zero(r);
            int deleted = 0;
            if (le_needs_broadcast_msg_applied(cmd, storeddata)) { //If already clean, nothing to do.
                // message application code needs a key in order to determine how much
                // work was done by this message. since this is a broadcast message,
                // we have to create a new message whose key is the current le's key.
                DBT curr_keydbt;
                toku_fill_dbt(&curr_keydbt, curr_keyp, curr_keylen);
                FT_MSG_S curr_msg;
                ft_msg_init(&curr_msg, ft_msg_get_type(cmd), ft_msg_get_msn(cmd),ft_msg_get_xids(cmd), &curr_keydbt, cmd->val);
                /*ft_msg curr_msg(toku_fill_dbt(&curr_keydbt, curr_keyp, curr_keylen),
                                msg.vdbt(), msg.type(), msg.msn(), msg.xids());*/
                toku_ft_bn_apply_cmd_once(bn, nullptr, &curr_msg, idx, storeddata, oldest_referenced_xid_known, gc_info, workdone, stats_to_update);
                // at this point, we cannot trust msg.kdbt to be valid.
                uint32_t new_omt_size = bn->data_buffer.omt_size();
                if (new_omt_size != num_klpairs) {
                    paranoid_invariant(new_omt_size + 1 == num_klpairs);
                    //Item was deleted.
                    deleted = 1;
                }
            }
            if (deleted) {
                //ugly fix for int underflow for now.
                if(end_idx == 0) break;
                end_idx--;
            }
            else {
                idx++;
            }
        }

        break;
    }
    case FT_GOTO: {
        // Apply to all leafentries
        uint32_t num_klpairs = bn->data_buffer.omt_size();
        uint32_t start_idx = 0;
        uint32_t end_idx = num_klpairs;
        {
            LEAFENTRY old_le = nullptr;
            find_idx_for_msg(ft->key_ops.keycmp, bn, cmd, desc, false,
                             &start_idx, &old_le, &key, &keylen);
            old_le = nullptr;
            find_idx_for_msg(ft->key_ops.keycmp, bn, cmd, desc, true,
                             &end_idx, &old_le, &key, &keylen);
            if (old_le == nullptr) {
                // we need to back by 1
                if (start_idx == end_idx) {
                    // nothing to do
                    break;
                }
                end_idx -= 1;
            }
        }
        for (uint32_t idx = start_idx; idx <= end_idx; ) {
            void *curr_keyp = NULL;
            uint32_t curr_keylen = 0;
            num_klpairs = bn->data_buffer.omt_size();
            r = bn->data_buffer.fetch_klpair(idx, &storeddata, &curr_keylen, &curr_keyp);
            int deleted = 0;

            DBT curr_keydbt;
            toku_fill_dbt(&curr_keydbt, curr_keyp, curr_keylen);
            FT_MSG_S curr_msg;
            ft_msg_init(&curr_msg, ft_msg_get_type(cmd), ft_msg_get_msn(cmd),
                        ft_msg_get_xids(cmd), &curr_keydbt, cmd->val);
            toku_ft_bn_apply_cmd_once(bn, nullptr, &curr_msg, idx, storeddata,
                                      oldest_referenced_xid_known, gc_info,
                                      workdone, stats_to_update);
            // at this point, we cannot trust msg.kdbt to be valid.
            uint32_t new_omt_size = bn->data_buffer.omt_size();
            if (new_omt_size != num_klpairs) {
                assert(new_omt_size + 1 == num_klpairs);
                deleted = 1;
            }
            if (deleted) {
                if (end_idx == 0)
                    break;
                end_idx--;
            } else {
                idx++;
            }
        }
        break;
    }
    }
    return;
}

static inline int
key_msn_cmp(const DBT *a, const DBT *b, const MSN amsn, const MSN bmsn,
            DESCRIPTOR descriptor, ft_compare_func key_cmp)
{
    FAKE_DB(db, descriptor);
    int r = key_cmp(&db, a, b);
    if (r == 0) {
        if (amsn.msn > bmsn.msn) {
            r = +1;
        } else if (amsn.msn < bmsn.msn) {
            r = -1;
        } else {
            r = 0;
        }
    }
    return r;
}

int
toku_fifo_entry_key_msn_heaviside(const int32_t &offset, const struct toku_fifo_entry_key_msn_heaviside_extra &extra)
{
    const struct fifo_entry *query = toku_fifo_get_entry(extra.fifo, offset);
    DBT qdbt;
    const DBT *query_key = fill_dbt_for_fifo_entry(&qdbt, query);
    const DBT *target_key = extra.key;
    return key_msn_cmp(query_key, target_key, query->msn, extra.msn,
                       extra.desc, extra.cmp);
}


int
toku_fifo_entry_key_msn_cmp(const struct toku_fifo_entry_key_msn_cmp_extra &extra, const int32_t &ao, const int32_t &bo)
{
    const struct fifo_entry *a = toku_fifo_get_entry(extra.fifo, ao);
    const struct fifo_entry *b = toku_fifo_get_entry(extra.fifo, bo);
    DBT adbt, bdbt;
    const DBT *akey = fill_dbt_for_fifo_entry(&adbt, a);
    const DBT *bkey = fill_dbt_for_fifo_entry(&bdbt, b);
    return key_msn_cmp(akey, bkey, a->msn, b->msn,
                       extra.desc, extra.cmp);
}

void toku_bnc_insert_msg(NONLEAF_CHILDINFO bnc, struct ubi_entry *ubi_insert, FT_MSG msg, bool is_fresh, DESCRIPTOR desc, ft_compare_func cmp)
// Effect: Enqueue the message represented by the parameters into the
//   bnc's buffer, and put it in either the fresh or stale message tree,
//   or the broadcast list.
//
// This is only exported for tests.
{
    int32_t offset;
    int r = toku_fifo_enq(bnc->buffer, msg, is_fresh, &offset);
    void * key = ft_msg_get_key(msg);
    uint32_t keylen = ft_msg_get_keylen(msg);
    MSN msn = ft_msg_get_msn(msg);
    enum ft_msg_type type = ft_msg_get_type(msg);
    if (type == FT_UNBOUND_INSERT) {
        bnc->unbound_insert_count++;
        //SOSP TODO: Bill locking?
        toku_list_push(&bnc->unbound_inserts, &ubi_insert->node_list);
    }
    assert_zero(r);
    if (ft_msg_type_applies_once(type)) {
        DBT keydbt;
        struct toku_fifo_entry_key_msn_heaviside_extra extra = { .desc = desc, .cmp = cmp, .fifo = bnc->buffer, .key = toku_fill_dbt(&keydbt, key, keylen), .msn = msn };
        if (is_fresh) {
            r = bnc->fresh_message_tree.insert<struct toku_fifo_entry_key_msn_heaviside_extra, toku_fifo_entry_key_msn_heaviside>(offset, extra, nullptr);
            assert_zero(r);
        } else {
            r = bnc->stale_message_tree.insert<struct toku_fifo_entry_key_msn_heaviside_extra, toku_fifo_entry_key_msn_heaviside>(offset, extra, nullptr);
            assert_zero(r);
        }
    } else {
        invariant(ft_msg_type_applies_multiple(type) || ft_msg_type_does_nothing(type));
        if (FT_GOTO == type) {
            const uint32_t idx = bnc->goto_list.size();
            r = bnc->goto_list.insert_at(offset, idx);
            assert_zero(r);
        } else {
            const uint32_t idx = bnc->broadcast_list.size();
            r = bnc->broadcast_list.insert_at(offset, idx);
            assert_zero(r);
        }
    }
}

static void
ft_process_key(FT ft, DBT *lift, DBT *not_lifted, DBT *key, DBT *alloc_key)
{
    DBT tmp;
    int r;

    if (lift->size) {
        r = toku_ft_lift_key_no_alloc(ft, &tmp, key, lift);
        assert_zero(r);
        toku_copy_dbt(key, tmp);
    }

    if (not_lifted->size) {
        r = toku_ft_unlift_key(ft, &tmp, key, not_lifted);
        assert_zero(r);
        toku_cleanup_dbt(alloc_key);
        toku_copy_dbt(alloc_key, tmp);
        toku_copy_dbt(key, tmp);
    }
}

static void
ft_recover_key(FT ft, DBT *lift, DBT *not_lifted, DBT *key, DBT *alloc_key)
{
    DBT tmp;
    int r;

    if (not_lifted->size) {
        r = toku_ft_lift_key_no_alloc(ft, &tmp, key, not_lifted);
        assert_zero(r);
        toku_copy_dbt(key, tmp);
    }

    if (lift->size) {
        r = toku_ft_unlift_key(ft, &tmp, key, lift);
        assert_zero(r);
        toku_cleanup_dbt(alloc_key);
        toku_copy_dbt(alloc_key, tmp);
        toku_copy_dbt(key, tmp);
    }
}

// append a cmd to a nonleaf node's child buffer
// should be static, but used by test programs
void toku_ft_append_to_child_buffer(ft_compare_func compare_fun, DESCRIPTOR desc, struct ubi_entry *ubi_insert, FTNODE node, int childnum, FT_MSG msg, bool is_fresh) {
    paranoid_invariant(BP_STATE(node,childnum) == PT_AVAIL);
    toku_bnc_insert_msg(BNC(node, childnum), ubi_insert, msg, is_fresh, desc, compare_fun);
    node->dirty = 1;
}

static void
ft_nonleaf_cmd_once_to_child(
    FT ft,
    DESCRIPTOR desc,
    struct ubi_entry *ubi_entry,
    FTNODE node,
    int target_childnum,
    FT_MSG cmd,
    bool is_fresh)
// Previously we had passive aggressive promotion, but that causes a lot of I/O a the checkpoint.  So now we are just putting it in the buffer here.
// Also we don't worry about the node getting overfull here.  It's the caller's problem.
{
    if (cmd->type == FT_UNBOUND_INSERT) {
        assert(ubi_entry != NULL);
        ++node->unbound_insert_count;
    }

    unsigned int childnum;
    if (target_childnum >= 0) {
        childnum = target_childnum;
    } else {
        childnum = toku_ftnode_which_child(node, cmd->key, desc, ft->key_ops.keycmp);
    }

    const DBT *old_key;
    DBT new_key, alloc_key;

    if (BP_LIFT(node, childnum).size || BP_NOT_LIFTED(node, childnum).size) {
        old_key = cmd->key;
        toku_copy_dbt(&new_key, *cmd->key);
        cmd->key = &new_key;
        toku_init_dbt(&alloc_key);

        ft_process_key(ft, &BP_LIFT(node, childnum),
                       &BP_NOT_LIFTED(node, childnum), &new_key, &alloc_key);
    } else {
        old_key = NULL;
    }

    toku_ft_append_to_child_buffer(ft->key_ops.keycmp, desc, ubi_entry, node,
                                   childnum, cmd, is_fresh);

    if (old_key) {
        cmd->key = old_key;
        toku_cleanup_dbt(&alloc_key);
    }
}

/* Find the leftmost child that may contain the key.
 * If the key exists it will be in the child whose number
 * is the return value of this function.
 */
int toku_ftnode_which_child(FTNODE node, const DBT *k,
                            DESCRIPTOR desc, ft_compare_func cmp) {
    // a funny case of no pivots
    if (node->n_children <= 1) return 0;

    // check the last key to optimize seq insertions
    int n = node->n_children-1;
    int c = ft_compare_pivot(desc, cmp, k, &node->childkeys[n-1]);
    if (c > 0) return n;

    // binary search the pivots
    int lo = 0;
    int hi = n-1; // skip the last one, we checked it above
    int mi;
    while (lo < hi) {
        mi = (lo + hi) / 2;
        c = ft_compare_pivot(desc, cmp, k, &node->childkeys[mi]);
        if (c > 0) {
            lo = mi+1;
            continue;
        }
        if (c < 0) {
            hi = mi;
            continue;
        }
        return mi;
    }
    return lo;
}

// Used for HOT.
int
toku_ftnode_hot_next_child(FTNODE node,
                           const DBT *k,
                           DESCRIPTOR desc,
                           ft_compare_func cmp) {
    int low = 0;
    int hi = node->n_children - 1;
    int mi;
    while (low < hi) {
        mi = (low + hi) / 2;
        int r = ft_compare_pivot(desc, cmp, k, &node->childkeys[mi]);
        if (r > 0) {
            low = mi + 1;
        } else if (r < 0) {
            hi = mi;
        } else {
            // if they were exactly equal, then we want the sub-tree under
            // the next pivot.
            return mi + 1;
        }
    }
    invariant(low == hi);
    return low;
}

// TODO Use this function to clean up other places where bits of messages are passed around
//      such as toku_bnc_insert_msg() and the call stack above it.
static uint64_t
ft_msg_size(FT_MSG msg) {
    size_t keyval_size = msg->key->size + msg->val->size + msg->max_key->size;
    size_t xids_size = xids_get_serialize_size(msg->xids);
    return keyval_size + KEY_VALUE_OVERHEAD + FT_CMD_OVERHEAD + xids_size;
}

void get_child_bounds_for_msg_put(ft_compare_func cmp, DESCRIPTOR desc, FTNODE node, FT_MSG msg, int* start, int* end) {
    *start = 0;
    *end = node->n_children-1;
    enum ft_msg_type type = ft_msg_get_type(msg);
    const DBT * kdbt = msg->key;
    const DBT* max_kdbt = msg->max_key;
    if (ft_msg_type_is_multicast(type)) {
        *start = toku_ftnode_which_child(node, kdbt, desc, cmp);
        *end = toku_ftnode_which_child(node, max_kdbt, desc, cmp);
        paranoid_invariant(*start <= *end);
    }
}

static bool is_dbt_empty_or_size_zero(const DBT * dbt) {
	return (is_dbt_empty(dbt) || dbt->size == 0);
}

void
toku_ftnode_cow_clone(FTNODE old_node, FTNODE *new_n, BLOCKNUM new_b, uint32_t new_h)
{
    FTNODE XCALLOC(new_node);
    *new_n = new_node;

    new_node->oldest_referenced_xid_known = old_node->oldest_referenced_xid_known;
    new_node->max_msn_applied_to_node_on_disk = old_node->max_msn_applied_to_node_on_disk;
    new_node->flags = old_node->flags;
    new_node->thisnodename = new_b;
    new_node->layout_version = old_node->layout_version;
    new_node->layout_version_original = old_node->layout_version_original;
    new_node->layout_version_read_from_disk = new_node->layout_version_read_from_disk;
    new_node->build_id = old_node->build_id;
    new_node->height = old_node->height;
    new_node->dirty = 1;
    new_node->fullhash = new_h;
    new_node->n_children = old_node->n_children;
    new_node->totalchildkeylens = old_node->totalchildkeylens;

    assert(old_node->n_children > 0);
    if (old_node->n_children > 1) {
        XMALLOC_N(old_node->n_children - 1, new_node->childkeys);
    } else {
        new_node->childkeys = NULL;
    }
    XMALLOC_N(old_node->n_children, new_node->bp);

    if (old_node->bound_l.size) {
        toku_clone_dbt(&new_node->bound_l, old_node->bound_l);
    } else {
        toku_init_dbt(&new_node->bound_l);
    }
    if (old_node->bound_r.size) {
        toku_clone_dbt(&new_node->bound_r, old_node->bound_r);
    } else {
        toku_init_dbt(&new_node->bound_r);
    }
    toku_init_dbt(&new_node->shadow_version);
    new_node->shadow_next = NULL;

    for (int i = 0; i < old_node->n_children - 1; i++) {
        toku_clone_dbt(&new_node->childkeys[i], old_node->childkeys[i]);
    }

    ftnode_clone_partitions(old_node, new_node, false);
    new_node->unbound_insert_count = 0;
}

void
toku_ftnode_inc_child_refc(FT ft, FTNODE node)
{
    if (node->height == 0)
        return;
    for (int i = 0; i < node->n_children; i++) {
        BLOCKNUM blocknum = BP_BLOCKNUM(node, i);
        toku_blocknum_inc_refc(ft->blocktable, blocknum, ft);
    }
}

struct subtree_gc_args {
    FT ft;
    BLOCKNUM bnum;
};

static void ft_subtree_gc_func(void *extra);

static inline void
toku_ft_gc_subtree(FT ft, BLOCKNUM blocknum)
{
    if (!toku_blocknum_maybe_dec_refc(ft->blocktable, blocknum, ft)) {
        return;
    }

    struct subtree_gc_args *XMALLOC(args);
    args->ft = ft;
    args->bnum = blocknum;

    cachetable_rr_cf_kibbutz_enq(ft->cf, ft_subtree_gc_func, args);
}

struct goto_gc_extra {
    FT ft;
    NONLEAF_CHILDINFO bnc;
};

static int
goto_gc(const int32_t &offset, const uint32_t UU(idx), struct goto_gc_extra *extra)
{
    struct fifo_entry *entry = toku_fifo_get_entry(extra->bnc->buffer, offset);
    FT_MSG_S cmd;
    DBT k, v, m;
    fifo_entry_get_msg(&cmd, entry, &k, &v, &m);
    assert(ft_msg_get_type(&cmd) == FT_GOTO);

    if (ft_msg_goto_extra(&cmd)->blocknum.b != 0) {
        toku_ft_gc_subtree(extra->ft, ft_msg_goto_extra(&cmd)->blocknum);
    }

    return 0;
}

static void
toku_ft_gc_child(FT ft, FTNODE node, int childnum)
{
    assert(node->height > 0);

    if (BP_STATE(node, childnum) == PT_AVAIL) {
        // gc smart_pvts in the bnc
        NONLEAF_CHILDINFO bnc = BNC(node, childnum);
        struct goto_gc_extra extra = {
                .ft = ft,
                .bnc = bnc,
        };
        int r;
        if (bnc->goto_list.size() > 0) {
            r = bnc->goto_list.iterate<struct goto_gc_extra, goto_gc>(&extra);
            assert_zero(r);
        }
    }

    // send the child to bg
    toku_ft_gc_subtree(ft, BP_BLOCKNUM(node, childnum));
}

void toku_ftnode_gc_bncs_and_children(FT UU(ft), FTNODE UU(node))
{
    // call from dec_refc_and_unpin
    if (node->height > 0) {
        for (int i = 0; i < node->n_children; i++) {
            toku_ft_gc_child(ft, node, i);
        }
    }
}

static void ft_subtree_gc_func(void *extra)
{
    struct subtree_gc_args *args = (struct subtree_gc_args *)extra;

    uint32_t fullhash = toku_cachetable_hash(args->ft->cf, args->bnum);
    struct ftnode_fetch_extra bfe;
    FTNODE node;
    fill_bfe_for_full_read(&bfe, args->ft);
    toku_pin_ftnode_off_client_thread(args->ft, args->bnum, fullhash, &bfe,
                                      PL_WRITE_EXPENSIVE, 0, NULL, &node);
    toku_cachetable_dec_refc_and_unpin(args->ft, node);

    toku_free(args);
}

// add a stalk (an empty tree) for node childnum
static void
ft_clone_add_stalk(FT ft, FTNODE node, int childnum,
                   DBT *bound_l, DBT *bound_r)
{
    paranoid_invariant(node->height > 0);
    FTNODE stalk = NULL;
    // node is a nonleaf node
    {
        // create a node with one child -> stalk
        BLOCKNUM blocknum;
        uint32_t fullhash;
        cachetable_put_empty_node_with_dep_nodes(
            ft, 0, NULL, &blocknum, &fullhash, &stalk);
        toku_initialize_empty_ftnode(stalk, blocknum, node->height - 1, 1,
                                     ft->h->layout_version,
                                     ft->h->flags);

        // populate info in node->bp[childnum]
        memset(&node->bp[childnum], 0, sizeof(node->bp[0]));
        BP_BLOCKNUM(node, childnum) = blocknum;
        BP_WORKDONE(node, childnum) = 0;
        BP_STATE(node, childnum) = PT_AVAIL;
        set_BNC(node, childnum, toku_create_empty_nl());
        int r = toku_ft_lift(ft, &BP_LIFT(node, childnum), bound_l, bound_r);
        assert_zero(r);
        // puplate stalk pivots (only left and right bounds)
        r = toku_ft_lift_key(ft, &stalk->bound_l, bound_l, &BP_LIFT(node, childnum));
        assert_zero(r);
        r = toku_ft_lift_key(ft, &stalk->bound_r, bound_r, &BP_LIFT(node, childnum));
        assert_zero(r);
        stalk->totalchildkeylens = stalk->bound_l.size + stalk->bound_r.size;
    }
    if (node->height > 1) {
        // if the stalk in nonleaf, recursive creation
        ft_clone_add_stalk(ft, stalk, 0, &stalk->bound_l, &stalk->bound_r);
    } else {
        // if the stalk is a leaf, populate stalk->bp[0]
        memset(&stalk->bp[0], 0, sizeof(stalk->bp[0]));
        BP_BLOCKNUM(stalk, 0) = {0};
        BP_WORKDONE(stalk, 0) = 0;
        BP_STATE(stalk, 0) = PT_AVAIL;
        set_BLB(stalk, 0, toku_create_empty_bn());
    }
    toku_unpin_ftnode(ft, stalk);
}

// merge childnum_l and childnum_r, GC all children between them
static void
ft_clone_merge_and_gc(FT ft, FTNODE node, int childnum_l, int childnum_r)
{
    paranoid_invariant(childnum_l < childnum_r);
    paranoid_invariant(node->height > 0);

    FTNODE child_l = NULL;
    FTNODE child_r = NULL;
    ft_nonleaf_fetch_child_and_flush(ft, node, childnum_l, &child_l);
    ft_nonleaf_fetch_child_and_flush(ft, node, childnum_r, &child_r);

    // change lifted part of child_l and child_r
    int r;
    DBT *bound_l, *bound_r;
    bound_l = (childnum_l == 0) ?
              &node->bound_l : &node->childkeys[childnum_l - 1];
    bound_r = (childnum_r == node->n_children - 1) ?
              &node->bound_r : &node->childkeys[childnum_r];
    {
        DBT new_lift;
        r = toku_ft_lift(ft, &new_lift, bound_l, bound_r);
        assert_zero(r);
        r = toku_ft_node_relift(ft, child_l, &BP_LIFT(node, childnum_l), &new_lift);
        assert_zero(r);
        r = toku_ft_node_relift(ft, child_r, &BP_LIFT(node, childnum_r), &new_lift);
        assert_zero(r);
        toku_cleanup_dbt(&BP_LIFT(node, childnum_l));
        toku_copy_dbt(&BP_LIFT(node, childnum_l), new_lift);
    }

    // merge child_l and child_r, we may need to add a stalk between the
    // children of child_l and child_r to keep the boundary and lifting
    int old_n_children = child_l->n_children;
    bool add_stalk = (childnum_l + 1 != childnum_r) && (node->height > 1);
    if (add_stalk) {
        child_l->n_children += (child_r->n_children + 1);
    } else {
        child_l->n_children += child_r->n_children;
    }

    // xrealloc, keeping children of child_l
    XREALLOC_N(old_n_children, child_l->n_children, child_l->bp);
    // copy children of child_r
    if (add_stalk) {
        memcpy(child_l->bp + old_n_children + 1, child_r->bp,
               child_r->n_children * sizeof(child_r->bp[0]));
        // add a stalk to bp[old_n_children]
        ft_clone_add_stalk(ft, child_l, old_n_children,
                           &child_l->bound_r, &child_r->bound_l);
    } else {
        memcpy(child_l->bp + old_n_children, child_r->bp,
               child_r->n_children * sizeof(child_r->bp[0]));
    }
    // cleanup children in child_r
    memset(child_r->bp, 0, child_r->n_children * sizeof(child_r->bp[0]));

    // xrealloc, keeping pivots of child_l
    XREALLOC_N(old_n_children - 1, child_l->n_children - 1, child_l->childkeys);
    // pivots[old_n_children - 1] is bound_r of child_l
    toku_copy_dbt(&child_l->childkeys[old_n_children - 1], child_l->bound_r);
    // copy bound_r of child_r
    toku_copy_dbt(&child_l->bound_r, child_r->bound_r);
    child_l->totalchildkeylens += child_r->bound_r.size;
    {
        int target;
        // if add stalk, also copy bound_l of child_r
        if (add_stalk) {
            toku_copy_dbt(&child_l->childkeys[old_n_children], child_r->bound_l);
            child_l->totalchildkeylens += child_r->bound_l.size;
            toku_init_dbt(&child_r->bound_l);
            target = old_n_children + 1;
        } else {
            toku_cleanup_dbt(&child_r->bound_l);
            target = old_n_children;
        }
        // copy piovts of child_r
        for (int i = 0; i < child_r->n_children - 1; i++) {
            toku_copy_dbt(&child_l->childkeys[target + i],
                          child_r->childkeys[i]);
            child_l->totalchildkeylens += child_r->childkeys[i].size;
            toku_init_dbt(&child_r->childkeys[i]);
        }
    }
    toku_init_dbt(&child_r->bound_r);
    // cleanup child_r
    if (child_r->n_children > 0) {
        toku_free(child_r->bp);
        child_r->bp = NULL;
    }
    if (child_r->n_children > 1) {
        toku_free(child_r->childkeys);
        child_r->childkeys = NULL;
    }
    child_r->n_children = 0;
    child_r->totalchildkeylens = 0;

    // record old node n_children
    old_n_children = node->n_children;
    // cleanup BP[cnum_l + 1 ... cnum_r]
    //         CHILD[cnum_l + 1 ... cnum_r - 1]
    //         PVT[cnum_l ... cnum_r - 1]
    // 1. everything [cnum_l + 1 ... cnum_r - 1]
    for (int i = childnum_l + 1; i < childnum_r; i++) {
        // let background gc subtrees
        toku_ft_gc_child(ft, node, i);
        // cleanup the pivot
        node->totalchildkeylens -= node->childkeys[i].size;
        toku_cleanup_dbt(&node->childkeys[i]);
        // cleanup the bp
        toku_cleanup_dbt(&BP_LIFT(node, i));
        toku_cleanup_dbt(&BP_NOT_LIFTED(node, i));
        destroy_nonleaf_childinfo(BNC(node, i));
    }
    // 2. PVT[cnum_l]
    node->totalchildkeylens -= node->childkeys[childnum_l].size;
    toku_cleanup_dbt(&node->childkeys[childnum_l]);
    // 3. BP[cnum_r]
    toku_cleanup_dbt(&BP_LIFT(node, childnum_r));
    toku_cleanup_dbt(&BP_NOT_LIFTED(node, childnum_r));
    destroy_nonleaf_childinfo(BNC(node, childnum_r));
    // dec n_children
    node->n_children -= (childnum_r - childnum_l);

    // shift pivots and bps
    memmove(&node->bp[childnum_l + 1], &node->bp[childnum_r + 1],
            (old_n_children - childnum_r - 1) * sizeof(node->bp[0]));
    REALLOC_N(node->n_children, node->n_children + 1, node->bp);
    memmove(&node->childkeys[childnum_l], &node->childkeys[childnum_r],
            (old_n_children - childnum_r - 1) * sizeof(node->childkeys[0]));
    REALLOC_N(node->n_children - 1, node->n_children, node->childkeys);

    // dirty and unlock child_l
    child_l->dirty = 1;
    toku_unpin_ftnode(ft, child_l);
    // remove child_r
    toku_cachetable_dec_refc_and_unpin(ft, child_r);
    assert_zero(r);
    // keep node locked
    node->dirty = 1;
}

// much the same as toku_ftnode_which_child, but since clone is left exclusive
// we allow the min_key to be the same as the left pivot
static void
ft_clone_which_child(FT ft, FTNODE node,
                     const DBT *min_key, const DBT *max_key,
                     int *childnum_l, int *childnum_r)
{
    if (node->n_children <= 1) {
        *childnum_l = 0;
        *childnum_r = 0;
        return;
    }

    FAKE_DB(db, &ft->cmp_descriptor);
    int n = node->n_children - 1;
    int c;

    if (min_key->size == 0) {
        *childnum_l = 0;
        goto check_r;
    }

    c = ft->key_ops.keycmp(&db, min_key, &node->childkeys[n - 1]);
    if (c >= 0) {
        *childnum_l = n;
    } else {
        int lo = 0;
        int hi = n - 1;
        int mi;
        while (lo < hi) {
            mi = ((lo + hi) >> 1);
            c = ft->key_ops.keycmp(&db, min_key, &node->childkeys[mi]);
            if (c > 0) {
                lo = mi + 1;
            } else if (c < 0) {
                hi = mi;
            } else {
                *childnum_l = mi + 1;
                goto check_r;
            }
        }
        *childnum_l = lo;
    }

check_r:
    if (max_key->size == 0) {
        *childnum_r = node->n_children - 1;
        goto out;
    }

    c = ft->key_ops.keycmp(&db, max_key, &node->childkeys[n - 1]);
    if (c > 0) {
        *childnum_r = n;
    } else {
        int lo = 0;
        int hi = n - 1;
        int mi;
        while (lo < hi) {
            mi = ((lo + hi) >> 1);
            c = ft->key_ops.keycmp(&db, max_key, &node->childkeys[mi]);
            if (c > 0) {
                lo = mi + 1;
            } else if (c < 0) {
                hi = mi;
            } else {
                *childnum_r = mi;
                goto out;
            }
        }
        *childnum_r = lo;
    }

out:
    return;
}

// find the index of the first key that is greater than pivot
static uint32_t
ft_node_clone_search(FT ft, BN_DATA bd, DBT *pivot)
{
    FAKE_DB(db, &ft->cmp_descriptor);
    uint32_t lo = 0;
    uint32_t hi = bd->omt_size();
    uint32_t mid;
    int rr;
    uint32_t keylen;
    void *key;
    while (lo < hi) {
        mid = (lo + hi) >> 1;
        rr = bd->fetch_le_key_and_len(mid, &keylen, &key);
        assert_zero(rr);
        DBT tmp_dbt;
        toku_fill_dbt(&tmp_dbt, key, keylen);
        rr = ft->key_ops.keycmp(&db, &tmp_dbt, pivot);
        if (rr <= 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return hi;
}

// check the left and right indexes to go
// if they are different, merge and gc to make them one
// return the resulting index (the same for both)
static inline int
ft_clone_maybe_merge_and_gc(FT ft, FTNODE node,
                            const DBT *min_key, const DBT *max_key)
{
    int childnum_l, childnum_r;

    ft_clone_which_child(ft, node, min_key, max_key, &childnum_l, &childnum_r);
    if (childnum_l == childnum_r)
        return childnum_l;

    ft_clone_merge_and_gc(ft, node, childnum_l, childnum_r);
    return childnum_l;
}

// make a goto real pivots, the node should already be merge_and_gc'ed
static void
ft_clone_finalize_goto(FT ft, FTNODE node, int childnum, FT_MSG cmd)
{
    int r;
    // check whether smart_pvt is the same as existing pivots
    bool match_l, match_r;
    DBT *bound_l = (childnum == 0) ?
                   &node->bound_l : &node->childkeys[childnum - 1];
    DBT *bound_r = (childnum == node->n_children - 1) ?
                   &node->bound_r : &node->childkeys[childnum];
    if (bound_l->size == 0) {
        // infinite
        match_l = false;
    } else {
        r = ft_compare_pivot(&ft->cmp_descriptor, ft->key_ops.keycmp,
                             bound_l, cmd->key);
        assert(r <= 0);
        match_l = (r == 0);
    }
    if (bound_r->size == 0) {
        match_r = false;
    } else {
        r = ft_compare_pivot(&ft->cmp_descriptor, ft->key_ops.keycmp,
                             cmd->max_key, bound_r);
        assert(r <= 0);
        match_r = (r == 0);
    }

    NONLEAF_CHILDINFO bnc;
    // left and right pivots are the same, replace and GC
    if (match_l && match_r) {
        // send the old subtree to GC
        toku_ft_gc_child(ft, node, childnum);
        // cleanup the old BNC
        assert(BP_STATE(node, childnum) == PT_AVAIL);
        destroy_nonleaf_childinfo(BNC(node, childnum));
        set_BNULL(node, childnum);
        // setup new bnc
        bnc = toku_create_empty_nl();
        // lift should be the same
        // copy not_lifted
        toku_cleanup_dbt(&BP_NOT_LIFTED(node, childnum));
        toku_clone_dbt(&BP_NOT_LIFTED(node, childnum), *cmd->val);
        memset(&node->bp[childnum], 0, sizeof(node->bp[0]));
        BP_BLOCKNUM(node, childnum) = ft_msg_goto_extra(cmd)->blocknum;
        BP_WORKDONE(node, childnum) = 0;
        BP_INIT_TOUCHED_CLOCK(node, childnum);
        BP_STATE(node, childnum) = PT_AVAIL;
        node->dirty = 1;
        return;
    }

    assert(BP_STATE(node, childnum) == PT_AVAIL);
    // in this case, we need to keep the old child
    if (toku_bnc_nbytesinbuf(BNC(node, childnum)) != 0) {
        FTNODE child = NULL;
        ft_nonleaf_fetch_child_and_flush(ft, node, childnum, &child);
        toku_unpin_ftnode(ft, child);
    }
    // this bnc should have nothing now
    NONLEAF_CHILDINFO old_bnc = BNC(node, childnum);
    BLOCKNUM old_blocknum = BP_BLOCKNUM(node, childnum);
    set_BNULL(node, childnum);

    // prepare bnc
    bnc = toku_create_empty_nl();
    assert_zero(BP_NOT_LIFTED(node, childnum).size);
    DBT old_lift;
    toku_copy_dbt(&old_lift, BP_LIFT(node, childnum));
    toku_init_dbt(&BP_LIFT(node, childnum));
    // shift pivots and bps
    if (!match_l && !match_r) {
        int old_n_children = node->n_children;
        node->n_children += 2;

        // shift bps
        REALLOC_N(node->n_children, old_n_children, node->bp);
        memmove(&node->bp[childnum + 3], &node->bp[childnum + 1],
                (node->n_children - childnum - 3) * sizeof(node->bp[0]));

        toku_init_dbt(&BP_LIFT(node, childnum+1));
        toku_init_dbt(&BP_LIFT(node, childnum+2));
        // put new bnc to bp[childnum + 1]
        BP_BLOCKNUM(node, childnum + 1) = ft_msg_goto_extra(cmd)->blocknum;
        BP_WORKDONE(node, childnum + 1) = 0;
        r = toku_ft_lift(ft, &BP_LIFT(node, childnum + 1), cmd->key, cmd->max_key);
        assert_zero(r);
        toku_clone_dbt(&BP_NOT_LIFTED(node, childnum + 1), *cmd->val);
        BP_INIT_TOUCHED_CLOCK(node, childnum + 1);
        BP_STATE(node, childnum + 1) = PT_AVAIL;
        set_BNC(node, childnum + 1, bnc);

        // inc refc old_blocknum
        toku_blocknum_inc_refc(ft->blocktable, old_blocknum, ft);
        // setup bp[childnum]
        bnc = toku_create_empty_nl();
        r = toku_ft_lift(ft, &BP_LIFT(node, childnum), bound_l, cmd->key);
        assert_zero(r);

        r = toku_ft_lift_key(ft, &BP_NOT_LIFTED(node, childnum), &BP_LIFT(node, childnum), &old_lift);
        assert_zero(r);
        BP_BLOCKNUM(node, childnum) = old_blocknum;
        BP_WORKDONE(node, childnum) = 0;
        BP_INIT_TOUCHED_CLOCK(node, childnum);
        BP_STATE(node, childnum) = PT_AVAIL;
        set_BNC(node, childnum, bnc);
        // setup bp[childnum + 2]
        bnc = toku_create_empty_nl();
        r = toku_ft_lift(ft, &BP_LIFT(node, childnum + 2), cmd->max_key, bound_r);
        assert_zero(r);
        r = toku_ft_lift_key(ft, &BP_NOT_LIFTED(node, childnum + 2), &BP_LIFT(node, childnum + 2), &old_lift);
        assert_zero(r);
        BP_BLOCKNUM(node, childnum + 2) = old_blocknum;
        BP_WORKDONE(node, childnum + 2) = 0;
        BP_INIT_TOUCHED_CLOCK(node, childnum + 2);
        BP_STATE(node, childnum + 2) = PT_AVAIL;
        set_BNC(node, childnum + 2, bnc);
        // shift pivots
        REALLOC_N(node->n_children - 1, old_n_children - 1, node->childkeys);
        memmove(&node->childkeys[childnum + 2], &node->childkeys[childnum],
                (node->n_children - childnum - 3) * sizeof(node->childkeys[0]));
        // setup pivot[childnum] and piovt[childnum + 1]
        toku_clone_dbt(&node->childkeys[childnum], *cmd->key);
        node->totalchildkeylens += cmd->key->size;
        toku_clone_dbt(&node->childkeys[childnum + 1], *cmd->max_key);
        node->totalchildkeylens += cmd->max_key->size;
    } else {
        int old_n_children = node->n_children;
        node->n_children += 1;
        REALLOC_N(node->n_children, old_n_children, node->bp);
        memmove(&node->bp[childnum + 2], &node->bp[childnum + 1],
                (node->n_children - childnum - 2) * sizeof(node->bp[0]));
        memset(&node->bp[childnum], 0, 2 * sizeof(node->bp[0]));
        REALLOC_N(node->n_children - 1, old_n_children - 1, node->childkeys);
        memmove(&node->childkeys[childnum + 1], &node->childkeys[childnum],
                (node->n_children - childnum - 2) * sizeof(node->childkeys[0]));

        if (match_l) {
            // left match, put bnc to bp[childnum]
            BP_BLOCKNUM(node, childnum) = ft_msg_goto_extra(cmd)->blocknum;
            BP_WORKDONE(node, childnum) = 0;
            r = toku_ft_lift(ft, &BP_LIFT(node, childnum), cmd->key, cmd->max_key);
            assert_zero(r);
            toku_clone_dbt(&BP_NOT_LIFTED(node, childnum), *cmd->val);
            BP_INIT_TOUCHED_CLOCK(node, childnum);
            BP_STATE(node, childnum) = PT_AVAIL;
            set_BNC(node, childnum, bnc);
            // setup bp[childnum + 1]
            bnc = toku_create_empty_nl();
            r = toku_ft_lift(ft, &BP_LIFT(node, childnum + 1), cmd->max_key, bound_r);
            assert_zero(r);
            r = toku_ft_lift_key(ft, &BP_NOT_LIFTED(node, childnum + 1), &BP_LIFT(node, childnum + 1), &old_lift);
            assert_zero(r);
            BP_BLOCKNUM(node, childnum + 1) = old_blocknum;
            BP_WORKDONE(node, childnum + 1) = 0;
            BP_INIT_TOUCHED_CLOCK(node, childnum + 1);
            BP_STATE(node, childnum + 1) = PT_AVAIL;
            set_BNC(node, childnum + 1, bnc);
            // left match, put cmd->max_key
            toku_clone_dbt(&node->childkeys[childnum], *cmd->max_key);
            node->totalchildkeylens += cmd->max_key->size;
        } else {
            // right match, put bnc to bp[childnum + 1]
            BP_BLOCKNUM(node, childnum + 1) = ft_msg_goto_extra(cmd)->blocknum;
            BP_WORKDONE(node, childnum + 1) = 0;
            r = toku_ft_lift(ft, &BP_LIFT(node, childnum + 1), cmd->key, cmd->max_key);
            assert_zero(r);
            toku_clone_dbt(&BP_NOT_LIFTED(node, childnum + 1), *cmd->val);
            BP_INIT_TOUCHED_CLOCK(node, childnum + 1);
            BP_STATE(node, childnum + 1) = PT_AVAIL;
            set_BNC(node, childnum + 1, bnc);

            // setup bp[childnum]
            bnc = toku_create_empty_nl();
            r = toku_ft_lift(ft, &BP_LIFT(node, childnum), bound_l, cmd->key);
            assert_zero(r);
            r = toku_ft_lift_key(ft, &BP_NOT_LIFTED(node, childnum), &BP_LIFT(node, childnum), &old_lift);
            assert_zero(r);
            BP_BLOCKNUM(node, childnum) = old_blocknum;
            BP_WORKDONE(node, childnum) = 0;
            BP_INIT_TOUCHED_CLOCK(node, childnum);
            BP_STATE(node, childnum) = PT_AVAIL;
            set_BNC(node, childnum, bnc);
            // right match, put cmd->key
            toku_clone_dbt(&node->childkeys[childnum], *cmd->key);
            node->totalchildkeylens += cmd->key->size;
        }
    }

    node->dirty = 1;
    destroy_nonleaf_childinfo(old_bnc);
    toku_cleanup_dbt(&old_lift);
}

static void
ft_nonleaf_cmd_multiple(
    FT ft,
    DESCRIPTOR desc,
    FTNODE node,
    FT_MSG cmd,
    bool is_fresh)
// Effect: Put the cmd into a nonleaf node.  We put it into all children, possibly causing the children to become reactive.
//  We don't do the splitting and merging.  That's up to the caller after doing all the puts it wants to do.
//  The re_array[i] gets set to the reactivity of any modified child i.         (And there may be several such children.)
{
    int start;
    int end;
    if (ft_msg_get_type(cmd) == FT_GOTO) {
        int height = ft_msg_goto_extra(cmd)->height;
        assert(node->height > height);
        int idx = ft_clone_maybe_merge_and_gc(ft, node, cmd->key, cmd->max_key);
        if (ft_msg_goto_extra(cmd)->blocknum.b != 0 && node->height == height + 1) {
            ft_clone_finalize_goto(ft, node, idx, cmd);
            // work done, otherwise the msg is going to be inject to the BNC
            return;
        } else {
            start = idx;
            end = idx;
        }
    } else {
        get_child_bounds_for_msg_put(ft->key_ops.keycmp, desc, node, cmd, &start, &end);
    }
    const DBT *old_key, *old_max_key;
    DBT new_key, new_max_key;
    if (!is_dbt_empty_or_size_zero(cmd->key)) {
        old_key = cmd->key;
        cmd->key = &new_key;
    } else {
        old_key = NULL;
    }
    if (!is_dbt_empty_or_size_zero(cmd->max_key)) {
        old_max_key = cmd->max_key;
        cmd->max_key = &new_max_key;
    } else {
        old_max_key = NULL;
    }
    for (int i = start; i <= end; i++) {
        DBT alloc_key, alloc_max_key;
        toku_init_dbt(&alloc_key);
        toku_init_dbt(&alloc_max_key);

        if (cmd->key == &new_key) {
            if (i != start) {
                toku_copy_dbt(&new_key, node->childkeys[i - 1]);
            } else {
                toku_copy_dbt(&new_key, *old_key);
            }
            ft_process_key(ft, &BP_LIFT(node, i), &BP_NOT_LIFTED(node, i),
                           &new_key, &alloc_key);
        }
        if (cmd->max_key == &new_max_key) {
            if (i != end) {
                toku_copy_dbt(&new_max_key, node->childkeys[i]);
            } else {
                toku_copy_dbt(&new_max_key, *old_max_key);
            }
            ft_process_key(ft, &BP_LIFT(node, i), &BP_NOT_LIFTED(node, i),
                           &new_max_key, &alloc_max_key);
        }

        toku_ft_append_to_child_buffer(ft->key_ops.keycmp, desc, nullptr, node,
                                       i, cmd, is_fresh);

       toku_cleanup_dbt(&alloc_key);
        toku_cleanup_dbt(&alloc_max_key);
    }
    if (old_key) {
        cmd->key = old_key;
    }
    if (old_max_key) {
        cmd->max_key = old_max_key;
    }
}

static bool
ft_msg_applies_once(FT_MSG cmd)
{
    return ft_msg_type_applies_once(cmd->type);
}

static bool
ft_msg_applies_multiple(FT_MSG cmd)
{
    return ft_msg_type_applies_multiple(cmd->type);
}

static bool
ft_msg_does_nothing(FT_MSG cmd)
{
    return ft_msg_type_does_nothing(cmd->type);
}

static void
ft_nonleaf_put_cmd(
    FT ft,
    DESCRIPTOR desc,
    struct ubi_entry *ubi_entry,
    FTNODE node,
    int target_childnum,
    FT_MSG cmd,
    bool is_fresh)
// Effect: Put the cmd into a nonleaf node.  We may put it into a child, possibly causing the child to become reactive.
//  We don't do the splitting and merging.  That's up to the caller after doing all the puts it wants to do.
//  The re_array[i] gets set to the reactivity of any modified child i.         (And there may be several such children.)
//
{
    //
    // see comments in toku_ft_leaf_apply_cmd
    // to understand why we handle setting
    // node->max_msn_applied_to_node_on_disk here,
    // and don't do it in toku_ft_node_put_cmd
    //
    MSN cmd_msn = cmd->msn;
    invariant(cmd_msn.msn > node->max_msn_applied_to_node_on_disk.msn);
    node->max_msn_applied_to_node_on_disk = cmd_msn;

    // Yang: flushing lifts the child with BNC->not_lifted before putting any
    //   msg to the child. If we unlift the keys in the msg with BNC->not_lifted
    //   here, when this msg is flushed to the child, we need to lift keys in it
    //   with BNC->not_lifted. So I make msgs in the BNC not unlifted by the
    //   BNC->not_lifted (msgs in the subtree is 'unlifted' by BNC->not_lifted)
    if (ft_msg_applies_once(cmd)) {
        ft_nonleaf_cmd_once_to_child(ft, desc, ubi_entry, node, target_childnum,
                                     cmd, is_fresh);
    } else if (ft_msg_applies_multiple(cmd)) {
        ft_nonleaf_cmd_multiple(ft, desc, node, cmd, is_fresh);
    } else {
        paranoid_invariant(ft_msg_does_nothing(cmd));
    }
}

#ifdef DEBUG_SEQ_IO
static int
//count the unbound ule in a basement nodes
ft_basement_node_count_unbound_ules(BASEMENTNODE bn) {
	int index = 0;
	int sum = 0;
	while (index <  bn->data_buffer.omt_size()) {
        	void* keyp = NULL;
        	uint32_t keylen = 0;
        	LEAFENTRY leaf_entry;
        	int r = bn->data_buffer.fetch_klpair(index, &leaf_entry, &keylen, &keyp);
        	assert_zero(r);
        	sum += toku_le_unbound_count(leaf_entry);
        	// Check if the leaf entry was deleted or not.
		++index;

    	}
	return sum;
}
#endif
// Garbage collect one leaf entry.
static void
ft_basement_node_gc_once(BASEMENTNODE bn,
                          uint32_t index,
                          void* keyp,
                          uint32_t keylen,
                          LEAFENTRY leaf_entry,
                          const xid_omt_t &snapshot_xids,
                          const rx_omt_t &referenced_xids,
                          const xid_omt_t &live_root_txns,
                          TXNID oldest_referenced_xid_known,
                          STAT64INFO_S * delta)
{
    paranoid_invariant(leaf_entry);

    // Don't run garbage collection on non-mvcc leaf entries.
    if (leaf_entry->type < LE_MVCC || leaf_entry->type >= LE_MVCC_END) {
        goto exit;
    }

    // Don't run garbage collection if this leafentry decides it's not worth it.
    if (!toku_le_innermost_is_delete(leaf_entry, oldest_referenced_xid_known) &&
        !toku_le_worth_running_garbage_collection(leaf_entry, oldest_referenced_xid_known)) {
        goto exit;
    }

    LEAFENTRY new_leaf_entry;
    new_leaf_entry = NULL;

    // The mempool doesn't free itself.  When it allocates new memory,
    // this pointer will be set to the older memory that must now be
    // freed.
    void * maybe_free;
    maybe_free = NULL;

    // These will represent the number of bytes and rows changed as
    // part of the garbage collection.
    int64_t numbytes_delta;
    int64_t numrows_delta;
    toku_le_garbage_collect(leaf_entry,
                            &bn->data_buffer,
                            index,
                            keyp,
                            keylen,
                            &new_leaf_entry,
                            snapshot_xids,
                            referenced_xids,
                            live_root_txns,
                            oldest_referenced_xid_known,
                            &numbytes_delta);

    numrows_delta = 0;
    if (new_leaf_entry) {
        numrows_delta = 0;
    } else {
        numrows_delta = -1;
    }

    // If we created a new mempool buffer we must free the
    // old/original buffer.
    if (maybe_free) {
        toku_free(maybe_free);
    }

    // Update stats.
    bn->stat64_delta.numrows += numrows_delta;
    bn->stat64_delta.numbytes += numbytes_delta;
    delta->numrows += numrows_delta;
    delta->numbytes += numbytes_delta;

exit:
    return;
}

// Garbage collect all leaf entries for a given basement node.
static void
basement_node_gc_all_les(BASEMENTNODE bn,
                         const xid_omt_t &snapshot_xids,
                         const rx_omt_t &referenced_xids,
                         const xid_omt_t &live_root_txns,
                         TXNID oldest_referenced_xid_known,
                         STAT64INFO_S * delta)
{
    int r = 0;
    uint32_t index = 0;
    uint32_t num_leafentries_before;
    while (index < (num_leafentries_before = bn->data_buffer.omt_size())) {
        void* keyp = NULL;
        uint32_t keylen = 0;
        LEAFENTRY leaf_entry;
        r = bn->data_buffer.fetch_klpair(index, &leaf_entry, &keylen, &keyp);
        assert_zero(r);
        ft_basement_node_gc_once(
            bn,
            index,
            keyp,
            keylen,
            leaf_entry,
            snapshot_xids,
            referenced_xids,
            live_root_txns,
            oldest_referenced_xid_known,
            delta
            );
        // Check if the leaf entry was deleted or not.
        if (num_leafentries_before == bn->data_buffer.omt_size()) {
            ++index;
        }
    }
}

// Garbage collect all leaf entires in all basement nodes.
static void
ft_leaf_gc_all_les(FTNODE node,
                   FT ft,
                   const xid_omt_t &snapshot_xids,
                   const rx_omt_t &referenced_xids,
                   const xid_omt_t &live_root_txns,
                   TXNID oldest_referenced_xid_known)
{
    toku_assert_entire_node_in_memory(node);
    paranoid_invariant_zero(node->height);
    // Loop through each leaf entry, garbage collecting as we go.
    for (int i = 0; i < node->n_children; ++i) {
        // Perform the garbage collection.
        BASEMENTNODE bn = BLB(node, i);
        STAT64INFO_S delta;
        delta.numrows = 0;
        delta.numbytes = 0;
        basement_node_gc_all_les(bn, snapshot_xids, referenced_xids, live_root_txns, oldest_referenced_xid_known, &delta);
        toku_ft_update_stats(&ft->in_memory_stats, delta);
    }
}

static void
ft_leaf_run_gc(FTNODE node, FT ft) {
    TOKULOGGER logger = toku_cachefile_logger(ft->cf);
    if (logger) {
        xid_omt_t snapshot_txnids;
        rx_omt_t referenced_xids;
        xid_omt_t live_root_txns;
        toku_txn_manager_clone_state_for_gc(
            logger->txn_manager,
            &snapshot_txnids,
            &referenced_xids,
            &live_root_txns
            );

        // Perform garbage collection. Provide a full snapshot of the transaction
        // system plus the oldest known referenced xid that could have had messages
        // applied to this leaf.
        //
        // Using the oldest xid in either the referenced_xids or live_root_txns
        // snapshots is not sufficient, because there could be something older that is neither
        // live nor referenced, but instead aborted somewhere above us as a message in the tree.
        ft_leaf_gc_all_les(node, ft, snapshot_txnids, referenced_xids, live_root_txns, node->oldest_referenced_xid_known);

        // Free the OMT's we used for garbage collecting.
        snapshot_txnids.destroy();
        referenced_xids.destroy();
        live_root_txns.destroy();
    }
}
//blindwrite
static void ft_msg_free_nonempty_dbt(const DBT * dbt) {
	if(!is_dbt_empty(dbt)) {
		toku_free((void *)dbt);
	}
}

static void ft_msg_destroy(FT_MSG msg) {
	ft_msg_free_nonempty_dbt(msg->key);
	ft_msg_free_nonempty_dbt(msg->val);
	ft_msg_free_nonempty_dbt(msg->max_key);
	toku_free(msg);
}

struct opt_msg {
    FT_MSG msg; //ptr to the msg
    bool is_fresh; //we need it later
    struct toku_list all_msgs; //next msg and prev msg
    struct toku_list range_delete_msgs; //next and prev range delete msgs;
};
struct pacman_opt_mgmt {
    struct toku_list msg_list; //ptr to the 1st msg
    bool done;  //if the last round opt does not change the size of the list, the opt is done
    size_t dismissed_size;  // remaining size has to take this into account.
    int n_range_delete;
    struct toku_list range_delete_list;
};

static bool key_range_strictly_dominates(FT_MSG msg_dominator, FT_MSG msg_dominatee, FT ft) {
	assert(FT_DELETE_MULTICAST == ft_msg_get_type(msg_dominator));
 	const DBT * dominator_key = msg_dominator->key;
	const DBT * dominator_max_key = msg_dominator->max_key;
	const DBT * dominatee_key, *dominatee_max_key;
	if(ft_msg_type_applies_multiple(msg_dominatee->type)) {
		dominatee_key = msg_dominatee->key;
		dominatee_max_key = msg_dominatee->max_key;
	} else {
		dominatee_key = dominatee_max_key = msg_dominatee->key;
	}

	DESCRIPTOR desc = &ft->cmp_descriptor;
    	ft_compare_func cmp = ft->key_ops.keycmp;
    	if(dominator_key == toku_dbt_negative_infinity() && dominator_max_key == toku_dbt_positive_infinity()) {
		return true;
	} else if(dominator_key == toku_dbt_negative_infinity()){
		return !(ft_compare_pivot(desc, cmp, dominatee_max_key, dominator_max_key) > 0);
	} else if(dominator_max_key == toku_dbt_positive_infinity()) {
		return !(ft_compare_pivot(desc, cmp, dominatee_key, dominator_key) < 0);
	} else  {
		return !(ft_compare_pivot(desc, cmp, dominatee_max_key, dominator_max_key) > 0) && !(ft_compare_pivot(desc, cmp, dominatee_key, dominator_key) < 0);
	}

}

//apparently ran out of the idea of naming.
static bool your_type_is_cup_of_pacman_tea(enum ft_msg_type type)
{
	bool ret_val = false;
	switch(type) {
	case FT_INSERT_NO_OVERWRITE:
    	case FT_INSERT:
    	case FT_DELETE_MULTICAST:
    	case FT_ABORT_MULTICAST_TXN:
    	case FT_COMMIT_MULTICAST_TXN:
    	case FT_DELETE_ANY:
    	case FT_ABORT_ANY:
    	case FT_COMMIT_ANY:
     	case FT_NONE:
    	case FT_UPDATE:
        	ret_val = true;
        	break;
    	case FT_COMMIT_BROADCAST_ALL:
    	case FT_COMMIT_BROADCAST_TXN:
    	case FT_ABORT_BROADCAST_TXN:
    	case FT_OPTIMIZE:
    	case FT_OPTIMIZE_FOR_UPGRADE:
    	case FT_UPDATE_BROADCAST_ALL:
    	case FT_COMMIT_MULTICAST_ALL:
        case FT_UNBOUND_INSERT:
        case FT_GOTO:
        	ret_val = false;
        	break;
    	}
    	return ret_val;
}

static void destroy_opt_msg(struct opt_msg * dead)
{
//do not forget the dimissed_size. the delta has to be updated!
    ft_msg_destroy(dead->msg);
    toku_list_remove(&dead->all_msgs);
    if(!toku_list_empty(&dead->range_delete_msgs))
        toku_list_remove(&dead->range_delete_msgs);
    toku_free(dead);
//RIP
}

//target_xid could be the xid of the msg to be devoured
//or the min_ver_num of the basement node. the code looks crappy but I like it.-JYM
bool range_delete_is_granted(FT_MSG rd_msg, TXNID target_xid, FT ft)
{
    assert(rd_msg->u.rd_extra.pm_status != PM_UNCOMMITTED);
    if (ft_msg_get_pm_status(rd_msg) == PM_GRANTED)
        return true;
    struct tokulogger *logger = toku_cachefile_logger(ft->cf);
    if (logger == NULL)
        return false ; //this is for tests which do not even init logger
    TXN_MANAGER txn_manager = logger->txn_manager;
    if (txn_manager->num_snapshots == 0) {
        //this is the best case we hope for -- no live txns, and from now on
        //range delete msg is just granted and no further check on it.
        rd_msg->u.rd_extra.pm_status = PM_GRANTED;
        return true;
    } else {
        //most time life is not that easy. now we check
        //no one is referencing range delete tx, that's say, no live txn that starts running
        //during the lifespan of rd txn. so everyone sees the delete!
        TXNID rd_xid = xids_get_outermost_xid(rd_msg->xids);
        if (toku_txn_manager_is_referenced(txn_manager, rd_xid))
            return false;
        else {
            //ok no one references rd txn, now check the live txn starts before rd txn starts
            TXNID youngest_live_xid_older_than = toku_txn_manager_youngest_snapshot_older_than_xid(txn_manager, rd_xid);
            if (youngest_live_xid_older_than == TXNID_NONE) {
                //happy case, no concurrent live txn before rd txn ever. lol
                rd_msg->u.rd_extra.pm_status = PM_GRANTED;
                return true;
            } else {
                //we have reached a point that we can not easily claim range delete
                //msg is granted. now it depends on specific msg/basement.
                if (youngest_live_xid_older_than > target_xid)
                    return false;
                else
                    return true;
            }
        }
    }
}

static void
may_void_dead_msgs_below_range_delete(
        struct pacman_opt_mgmt *pacman_manager,
        struct opt_msg *range_delete_msg, FT ft)
{
    struct toku_list *elem = range_delete_msg->all_msgs.prev;
    while (elem != &pacman_manager->msg_list) {
        struct opt_msg *target_opt_msg = toku_list_struct(elem, struct opt_msg, all_msgs);
        enum ft_msg_type target_type  = ft_msg_get_type(target_opt_msg->msg);

        if (!your_type_is_cup_of_pacman_tea(target_type))
            return;

        if (key_range_strictly_dominates(range_delete_msg->msg, target_opt_msg->msg, ft)) {
            //now check the liveness of the target msg
            TXNID target_xid = xids_get_outermost_xid(target_opt_msg->msg->xids);
            if (range_delete_is_granted(range_delete_msg->msg, target_xid, ft)) {
                //shot the msg finally!
                pacman_manager->dismissed_size += toku_ft_msg_memsize_in_fifo(target_opt_msg->msg);
                elem = elem->prev;
                destroy_opt_msg(target_opt_msg);
                if(pacman_manager->done)
                    pacman_manager->done = false;
                continue;
            }
        }
        elem = elem->prev;
    }
}

//check if the range delete msg is commited. if no, give up. if yes, set the pm_status = granted.
//for the simplicity now, we check the whole stack of xids are committed or not, a finer granularity can be achieved if we really want to push that far
static bool range_delete_is_committed(FT_MSG msg, FT ft)
{
    if (ft_msg_get_pm_status(msg) == PM_GRANTED)
        return true;
    XIDS xids = ft_msg_get_xids(msg);
    if (xids->num_xids > 1)
        return false;//no nested txn for now
    if (msg->u.rd_extra.pm_status == PM_UNCOMMITTED) {
        struct tokulogger *logger = toku_cachefile_logger(ft->cf);
        if( logger == NULL)
            return false ; //this is for tests which do not even init logger
        TXN_MANAGER txn_manager = logger->txn_manager;
        if (is_txnid_live(txn_manager, xids_get_xid(xids,0))) {
            return false;
        } else {
            msg->u.rd_extra.pm_status = PM_COMMITTED;
            return true;
        }
    } else {
        return true;
    }
}

static void
devour_dead_msgs(struct pacman_opt_mgmt *pacman_manager, FT ft)
{
    if (pacman_manager->n_range_delete == 0)
        return;
    struct toku_list *range_delete_head = &pacman_manager->range_delete_list;
    struct toku_list *elem = range_delete_head->next;
    while (elem != range_delete_head) {
        struct opt_msg *range_delete_msg = toku_list_struct(elem, struct opt_msg, range_delete_msgs);
        assert(FT_DELETE_MULTICAST == ft_msg_get_type(range_delete_msg->msg));
        if (range_delete_is_committed(range_delete_msg->msg, ft)) {
            may_void_dead_msgs_below_range_delete(pacman_manager, range_delete_msg, ft);
        }
        elem = elem->next;
    }
}

void
default_run_optimization(struct pacman_opt_mgmt * pacman_manager, FT ft)
{
    do {
        pacman_manager->done = true;
        devour_dead_msgs(pacman_manager, ft);
    } while (!pacman_manager->done);
}

//destroy the opt_msg too
int default_pacman_opt_iterate(struct pacman_opt_mgmt *pacman_manager, int(*f)(FT_MSG, bool, void*), void * args) {
	int ret = 0;
	struct toku_list * head = &pacman_manager->msg_list;
	struct toku_list* elem = head->next;
	while(elem != head) {
		struct opt_msg * opt_msg = toku_list_struct(elem, struct opt_msg, all_msgs);
		const DBT * key_before = opt_msg->msg->key;
		ret = f(opt_msg->msg, opt_msg->is_fresh, args);
		const DBT * key_after = opt_msg->msg->key;
		if(key_before != key_after) {
			toku_free((void *) key_before);
			opt_msg->msg->key = get_dbt_empty();
		}
		if(ret) break;
		elem = elem->next;
		destroy_opt_msg(opt_msg);

	}
	return ret;
}


void
pacman_opt_mgmt_init(struct pacman_opt_mgmt *&pacman_mgmt)
{
    pacman_mgmt = (struct pacman_opt_mgmt *)toku_xmalloc(sizeof(struct pacman_opt_mgmt));
    pacman_mgmt->done = true;
    pacman_mgmt->dismissed_size = 0;
    toku_list_init(&pacman_mgmt->msg_list);
    pacman_mgmt->n_range_delete = 0;
    toku_list_init(&pacman_mgmt->range_delete_list);
}

void pacman_opt_mgmt_destroy(struct pacman_opt_mgmt * pacman_mgmt) {
	assert(pacman_mgmt->done);
	assert(toku_list_empty(&pacman_mgmt->msg_list));
	assert(toku_list_empty(&pacman_mgmt->range_delete_list));
	toku_free(pacman_mgmt);
}

int
iterate_fn_bnc_build_pacman_opt(struct fifo_entry *e, void *args, FT_MSG &msg)
{
    struct pacman_opt_mgmt *pacman_manager = (struct pacman_opt_mgmt *)args;
    struct opt_msg *XMALLOC(opt_msg);
    //I hate this memcpy thing
    opt_msg->msg = (FT_MSG)toku_xmalloc(sizeof(FT_MSG_S));
    DBT *XMALLOC(k);
    DBT *XMALLOC(m);
    DBT *XMALLOC(v);
    fifo_entry_get_msg(opt_msg->msg, e, k, v, m);
    opt_msg->is_fresh = e->is_fresh;

    if (is_dbt_empty(opt_msg->msg->val))
        toku_free(v);
    if (is_dbt_empty(opt_msg->msg->max_key))
        toku_free(m);

    toku_list_init(&opt_msg->all_msgs);
    toku_list_init(&opt_msg->range_delete_msgs);
    toku_list_push(&pacman_manager->msg_list, &opt_msg->all_msgs);
    if (FT_DELETE_MULTICAST == ft_msg_get_type(opt_msg->msg)) {
        pacman_manager->n_range_delete++;
        toku_list_push(&pacman_manager->range_delete_list,
                       &opt_msg->range_delete_msgs);
    }
    msg = opt_msg->msg;
    return 0;
}

FT_MSG pacman_get_first_msg(struct pacman_opt_mgmt * pacman_manager) {
	if(toku_list_empty(&pacman_manager->msg_list))
		return NULL;
	toku_list * elem = pacman_manager->msg_list.next;
	struct opt_msg * opt_msg = toku_list_struct(elem, struct opt_msg, all_msgs);
	return opt_msg->msg;
}

struct iterate_fn_bnc_flush_args {
    size_t * arg0; //remaining_memsize
    NONLEAF_CHILDINFO arg1;//bnc
    FT arg2;//ft
    FTNODE arg3;// child
    STAT64INFO_S * arg4;//status_delta
};


static int iterate_fn_bnc_flush(FT_MSG msg, bool is_fresh, void *args)
{
    struct iterate_fn_bnc_flush_args *bf_args =
        (struct iterate_fn_bnc_flush_args *)args;
    size_t *p_remaining_memsize = bf_args->arg0;
    NONLEAF_CHILDINFO bnc = bf_args->arg1;
    FT ft = bf_args->arg2;
    FTNODE child = bf_args->arg3;
    STAT64INFO_S *p_stats_delta = bf_args->arg4;
    size_t entry_size = FIFO_CURRENT_ENTRY_MEMSIZE;
    struct ubi_entry *entry = NULL;

    if (msg->type == FT_UNBOUND_INSERT) {
        struct toku_list *head = &bnc->unbound_inserts;
        struct toku_list *lst = toku_list_head(head);
        while (lst != head) {
            entry = toku_list_struct(lst, struct ubi_entry, node_list);
            if (msg_matches_ubi(msg, entry))
                goto found_ubi_entry;
            lst = lst->next;
        }
        printf("could not find unbound_insert_entry within bnc\n");
        abort();
found_ubi_entry:
        bnc->unbound_insert_count--;
        toku_list_remove(&entry->node_list);
    }
    toku_ft_node_put_cmd(
        ft, &ft->cmp_descriptor, entry, child, -1, msg, is_fresh,
        make_gc_info(true), p_stats_delta);
    *p_remaining_memsize -= entry_size;
    return 0;
}

void
toku_bnc_flush_to_child(FT ft, NONLEAF_CHILDINFO bnc, FTNODE child,
                        TXNID oldest_referenced_xid_known)
{
    assert(bnc);
    STAT64INFO_S stats_delta = {0,0};

    size_t remaining_memsize = toku_fifo_buffer_size_in_use(bnc->buffer);
    struct iterate_fn_bnc_flush_args args = {&remaining_memsize,
                                             bnc,
                                             ft,
                                             child,
                                             &stats_delta};
    toku_fifo_iterate(bnc->buffer, iterate_fn_bnc_flush, &args);
    child->oldest_referenced_xid_known = oldest_referenced_xid_known;

    assert_zero(remaining_memsize);
    if (stats_delta.numbytes || stats_delta.numrows) {
        toku_ft_update_stats(&ft->in_memory_stats, stats_delta);
    }

    if (child->height == 0) {
        ft_leaf_run_gc(child, ft);
        size_t buffsize = toku_fifo_buffer_size_in_use(bnc->buffer);
        STATUS_INC(FT_MSG_BYTES_OUT, buffsize);
        // may be misleading if there's a broadcast message in there
        STATUS_INC(FT_MSG_BYTES_CURR, -buffsize);
    }
}

// update the lift in the bp, new_lift should not be shorter than bp
// new_lift would be consumed by this func
static void
ft_bp_update_lift(FT ft, struct ftnode_partition *bp, DBT *new_lift)
{
    if (new_lift->size != bp->lift.size) {
        assert(new_lift->size > bp->lift.size);
        // different lift
        // old_lift abcd, new_lift abcdefg, old_not_lifted xyz
        // lift new_lift with old_lift -> efg
        // unlift this with old_not_lifted -> xyzefg
        DBT tmp, new_not_lifted;
        int r;
        r = toku_ft_lift_key_no_alloc(ft, &tmp, new_lift, &bp->lift);
        assert_zero(r);
        r = toku_ft_unlift_key(ft, &new_not_lifted, &tmp, &bp->not_lifted);
        assert_zero(r);
        toku_cleanup_dbt(&bp->lift);
        toku_copy_dbt(&bp->lift, *new_lift);
        toku_cleanup_dbt(&bp->not_lifted);
        toku_copy_dbt(&bp->not_lifted, new_not_lifted);
    } else {
        toku_cleanup_dbt(new_lift);
    }
}

// this function does two things:
// 1. if there is a not_lifted from parent, lift not_lifted from the child
// 2. if the bounding pivots in the parent (pbound_l and pbound_r) are different
//    from the bound stored in the node, slice the child so they are the same
static void
ft_maybe_slice_child(FT ft, DBT *not_lifted, DBT *pbound_l, DBT *pbound_r, FTNODE child)
{
    DBT bound_l, bound_r;

    // the correct bounds for child (w/ not_lifted);
    if (not_lifted->size) {
        int r;
        r = toku_ft_unlift_key(ft, &bound_l, pbound_l, not_lifted);
        assert_zero(r);
        r = toku_ft_unlift_key(ft, &bound_r, pbound_r, not_lifted);
        assert_zero(r);
    } else {
        if (pbound_l->size) {
            toku_clone_dbt(&bound_l, *pbound_l);
        } else {
            toku_init_dbt(&bound_l);
        }
        if (pbound_r->size) {
            toku_clone_dbt(&bound_r, *pbound_r);
        } else {
            toku_init_dbt(&bound_r);
        }
    }

    // do we need to slice?
    int childnum_l, childnum_r;
    ft_clone_which_child(ft, child, &bound_l, &bound_r, &childnum_l, &childnum_r);
    // GC [0 ... childnum_l - 1] and [childnum_r + 1 ... n_childnre - 1]
    for (int i = 0; i < childnum_l; i++) {
        if (child->height > 0) {
            toku_ft_gc_child(ft, child, i);
            destroy_nonleaf_childinfo(BNC(child, i));
            toku_cleanup_dbt(&BP_LIFT(child, i));
            toku_cleanup_dbt(&BP_NOT_LIFTED(child, i));
        } else {
            destroy_basement_node(BLB(child, i));
        }
        set_BNULL(child, i);
        child->totalchildkeylens -= child->bound_l.size;
        toku_destroy_dbt(&child->bound_l);
        toku_copy_dbt(&child->bound_l, child->childkeys[i]);
        toku_init_dbt(&child->childkeys[i]);
    }
    for (int i = child->n_children - 1; i > childnum_r; i--) {
        if (child->height > 0) {
            toku_ft_gc_child(ft, child, i);
            destroy_nonleaf_childinfo(BNC(child, i));
            toku_cleanup_dbt(&BP_LIFT(child, i));
            toku_cleanup_dbt(&BP_NOT_LIFTED(child, i));
        } else {
            destroy_basement_node(BLB(child, i));
        }
        set_BNULL(child, i);
        child->totalchildkeylens -= child->bound_r.size;
        toku_destroy_dbt(&child->bound_r);
        toku_copy_dbt(&child->bound_r, child->childkeys[i - 1]);
        toku_init_dbt(&child->childkeys[i - 1]);
    }
    // now adjust pivots and bps
    if (childnum_l != 0) {
        memmove(&child->bp[0], &child->bp[childnum_l],
                (childnum_r - childnum_l + 1) * sizeof(child->bp[0]));
        if (childnum_l != childnum_r) {
            memmove(&child->childkeys[0], &child->childkeys[childnum_l],
                    (childnum_r - childnum_l) * sizeof(child->childkeys[0]));
        }
    }
    if (child->n_children != (childnum_r - childnum_l) + 1) {
        int old_n_children = child->n_children;
        child->n_children = (childnum_r - childnum_l) + 1;
        REALLOC_N(old_n_children, child->n_children, child->bp);
        REALLOC_N(old_n_children - 1, child->n_children - 1, child->childkeys);
        child->dirty = 1;
    }

    int cl, cr;
    FAKE_DB(db, &ft->cmp_descriptor);
    // if bound_l != child->bound_l
    if (child->bound_l.size) {
        cl = ft->key_ops.keycmp(&db, &child->bound_l, &bound_l);
    } else {
        // if child bound_l is infnity
        // cl = 0 if bound_l.size = 0, < 0 if bound_l.size > 0
        cl = child->bound_l.size - bound_l.size;
    }
    if (cl) {
        child->totalchildkeylens -= child->bound_l.size;
        toku_cleanup_dbt(&child->bound_l);
        child->totalchildkeylens += bound_l.size;
        toku_copy_dbt(&child->bound_l, bound_l);
    } else {
        toku_cleanup_dbt(&bound_l);
    }
    // if bound_r != child->bound_r
    if (child->bound_r.size) {
        cr = ft->key_ops.keycmp(&db, &child->bound_r, &bound_r);
    } else {
        // if child bound_r is infnity
        // cr = 0 if bound_r.size = 0, > if bound_r.size > 0
        cr = bound_r.size - child->bound_r.size;
    }
    if (cr) {
        child->totalchildkeylens -= child->bound_r.size;
        toku_cleanup_dbt(&child->bound_r);
        child->totalchildkeylens += bound_r.size;
        toku_copy_dbt(&child->bound_r, bound_r);
    } else {
        toku_cleanup_dbt(&bound_r);
    }
    DBT empty_lift;
    toku_init_dbt(&empty_lift);

    // if bounds changed, we need to:
    // 1. if child is a nonleaf, we need to change lifting
    // 2. if child is a leaf, we need to remove some kv pairs
    if (child->n_children == 1) {
        if (cl || cr) {
            if (child->height == 0) {
                BASEMENTNODE bn = BLB(child, 0);
                uint32_t lbi, ube;
                lbi = (cl) ? ft_node_clone_search(ft, &bn->data_buffer, &child->bound_l) : 0;
                ube = (cr) ? ft_node_clone_search(ft, &bn->data_buffer, &child->bound_r) : bn->data_buffer.omt_size();
                bn->data_buffer.relift_leafentries(ft, &empty_lift, &empty_lift, lbi, ube);
            } else {
                DBT new_lift;
                int r = toku_ft_lift(ft, &new_lift, &child->bound_l, &child->bound_r);
                assert_zero(r);
                ft_bp_update_lift(ft, &child->bp[0], &new_lift);
            }
            child->dirty = 1;
        }
    } else {
        if (cl) {
            if (child->height == 0) {
                BASEMENTNODE bn = BLB(child, 0);
                uint32_t lbi;
                lbi = ft_node_clone_search(ft, &bn->data_buffer, &child->bound_l);
                bn->data_buffer.relift_leafentries(ft, &empty_lift, &empty_lift, lbi, bn->data_buffer.omt_size());
            } else {
                DBT new_lift;
                int r = toku_ft_lift(ft, &new_lift, &child->bound_l, &child->childkeys[0]);
                assert_zero(r);
                ft_bp_update_lift(ft, &child->bp[0], &new_lift);
            }
            child->dirty = 1;
        }
        if (cr) {
            if (child->height == 0) {
                BASEMENTNODE bn = BLB(child, child->n_children - 1);
                uint32_t ube;
                ube = ft_node_clone_search(ft, &bn->data_buffer, &child->bound_r);
                bn->data_buffer.relift_leafentries(ft, &empty_lift, &empty_lift, 0, ube);
            } else {
                DBT new_lift;
                int r = toku_ft_lift(ft, &new_lift, &child->childkeys[child->n_children - 2], &child->bound_r);
                assert_zero(r);
                ft_bp_update_lift(ft, &child->bp[child->n_children - 1], &new_lift);
            }
            child->dirty = 1;
        }
    }

    if (not_lifted->size) {
        int r;
        r = toku_ft_node_relift(ft, child, &empty_lift, not_lifted);
        assert_zero(r);
        child->dirty = 1;
    }
}

void toku_bnc_flush_to_child_maybe_slice(
    FT ft,
    NONLEAF_CHILDINFO bnc,
    DBT *not_lifted,
    DBT *pbound_l,
    DBT *pbound_r,
    FTNODE child,
    TXNID oldest_referenced_xid_known
    )
{
    if (toku_bnc_n_entries(bnc) > 0) {
        child->dirty = 1;
        toku_bnc_flush_to_child(ft, bnc, child, oldest_referenced_xid_known);
    }

    ft_maybe_slice_child(ft, not_lifted, pbound_l, pbound_r, child);
}

void
toku_ft_node_put_cmd (
    FT ft,
    DESCRIPTOR desc,
    struct ubi_entry *ubi_entry,
    FTNODE node,
    int target_childnum,
    FT_MSG cmd,
    bool is_fresh,
    GC_INFO gc_info,
    STAT64INFO stats_to_update
    )
// Effect: Push CMD into the subtree rooted at NODE.
//   If NODE is a leaf, then
//   put CMD into leaf, applying it to the leafentries
//   If NODE is a nonleaf, then push the cmd into the FIFO(s) of the relevent child(ren).
//   The node may become overfull.  That's not our problem.
{
    toku_assert_entire_node_in_memory(node);
    toku_ft_node_unbound_inserts_validation(node, cmd, __LINE__);
    //
    // see comments in toku_ft_leaf_apply_cmd
    // to understand why we don't handle setting
    // node->max_msn_applied_to_node_on_disk here,
    // and instead defer to these functions
    //

    if (node->height == 0) {
        toku_ft_leaf_apply_cmd(ft, desc, ubi_entry, node, target_childnum, cmd, gc_info, nullptr, stats_to_update);
    } else {
        ft_nonleaf_put_cmd(ft, desc, ubi_entry, node, target_childnum, cmd, is_fresh);
    }

    toku_ft_node_unbound_inserts_validation(node, cmd, __LINE__);
}

// Effect: applies the cmd to the leaf if the appropriate basement node is in memory.
//           This function is called during message injection and/or flushing, so the entire
//           node MUST be in memory.
void toku_ft_leaf_apply_cmd(
    FT ft,
    DESCRIPTOR desc,
    struct ubi_entry *ubi_entry,
    FTNODE node,
    int target_childnum,  // which child to inject to, or -1 if unknown
    FT_MSG cmd,
    GC_INFO gc_info,
    uint64_t *workdone,
    STAT64INFO stats_to_update
    )
{
    VERIFY_NODE(t, node);
    toku_assert_entire_node_in_memory(node);

    //
    // Because toku_ft_leaf_apply_cmd is called with the intent of permanently
    // applying a message to a leaf node (meaning the message is permanently applied
    // and will be purged from the system after this call, as opposed to
    // toku_apply_ancestors_messages_to_node, which applies a message
    // for a query, but the message may still reside in the system and
    // be reapplied later), we mark the node as dirty and
    // take the opportunity to update node->max_msn_applied_to_node_on_disk.
    //
    node->dirty = 1;

    //
    // we cannot blindly update node->max_msn_applied_to_node_on_disk,
    // we must check to see if the msn is greater that the one already stored,
    // because the cmd may have already been applied earlier (via
    // toku_apply_ancestors_messages_to_node) to answer a query
    //
    // This is why we handle node->max_msn_applied_to_node_on_disk both here
    // and in ft_nonleaf_put_cmd, as opposed to in one location, toku_ft_node_put_cmd.
    //
    MSN cmd_msn = cmd->msn;
    if (cmd_msn.msn > node->max_msn_applied_to_node_on_disk.msn) {
        node->max_msn_applied_to_node_on_disk = cmd_msn;
    }

    // Pass the oldest possible live xid value to each basementnode
    // when we apply messages to them.
    TXNID oldest_referenced_xid_known = node->oldest_referenced_xid_known;

    if (ft_msg_applies_once(cmd)) {
        unsigned int childnum;
        bool ubi_applied_just_now = false;

        if (target_childnum >= 0) {
            childnum = target_childnum;
        } else {
            childnum = toku_ftnode_which_child(node, cmd->key, desc, ft->key_ops.keycmp);
        }
        for (FTNODE it = node; it; it = it->shadow_next) {
            if (BP_STATE(it, childnum) != PT_AVAIL) {
                assert(it != node);
                continue;
            }

            BASEMENTNODE bn = BLB(it, childnum);
            if (cmd->msn.msn > bn->max_msn_applied.msn) {
                bn->max_msn_applied = cmd->msn;
                toku_ft_bn_apply_cmd(ft, desc, (it == node) ? ubi_entry : NULL,
                                     bn, cmd, oldest_referenced_xid_known,
                                     gc_info, workdone, stats_to_update);
                if (cmd->type == FT_UNBOUND_INSERT) {
                        ubi_applied_just_now = true;
                        assert(ubi_entry);
                        node->unbound_insert_count++;  // when FT_INDIRECT is not turned on, unbound_insert_count does not increase to sync with bn.
                }
            }
        }
        if (ubi_applied_just_now == false && ubi_entry != NULL) {
            toku_mutex_lock(&global_logger->ubi_lock);
            toku_list_remove(&ubi_entry->in_or_out);
            toku_mutex_unlock(&global_logger->ubi_lock);
            destroy_ubi_entry(ubi_entry);
        }
    } else if (ft_msg_applies_multiple(cmd)) {
        int start = 0;
        int end = node->n_children - 1;
        if (cmd->type != FT_GOTO) {
            get_child_bounds_for_msg_put(ft->key_ops.keycmp, desc, node, cmd, &start, &end);
        }

        for (FTNODE it = node; it; it = it->shadow_next) {
            for (int i = start; i <= end; i++) {
                if (BP_STATE(it, i) != PT_AVAIL) {
                    assert(it != node);
                    continue;
                }

                BASEMENTNODE bn = BLB(it, i);
                if (cmd->msn.msn > bn->max_msn_applied.msn) {
                    bn->max_msn_applied = cmd->msn;
                    toku_ft_bn_apply_cmd(ft, desc, NULL, bn, cmd,
                                         oldest_referenced_xid_known,
                                         gc_info, workdone, stats_to_update);
                }
            }
        }
    } else {
        assert(ft_msg_does_nothing(cmd));
    }
    VERIFY_NODE(t, node);
}

static void
inject_message_in_locked_node_no_unlocking(
    FT ft,
    FTNODE node,
    int childnum,
    struct ubi_entry *ubi_entry,
    FT_MSG_S *cmd,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info
    )
{
    // No guarantee that we're the writer, but oh well.
    // TODO(leif): Implement "do I have the lock or is it someone else?"
    // check in frwlock.  Should be possible with TOKU_PTHREAD_DEBUG, nop
    // otherwise.
    invariant(toku_ctpair_is_write_locked(node->ct_pair));
    toku_assert_entire_node_in_memory(node);

    // Update the oldest known referenced xid for this node if it is younger
    // than the one currently known. Otherwise, it's better to keep the heurstic
    // we have and ignore this one.
    if (oldest_referenced_xid >= node->oldest_referenced_xid_known) {
        node->oldest_referenced_xid_known = oldest_referenced_xid;
    }

    // Get the MSN from the header.  Now that we have a write lock on the
    // node we're injecting into, we know no other thread will get an MSN
    // after us and get that message into our subtree before us.
    cmd->msn.msn = toku_sync_add_and_fetch(&ft->h->max_msn_in_ft.msn, 1);
    //printf("cmd [msn=%" PRIu64 ", type=%d]\n", cmd->msn.msn, cmd->type);
    if (ubi_entry) {
        ubi_entry->msn = cmd->msn;
        ubi_entry->state = UBI_UNBOUND;
    }

    paranoid_invariant(cmd->msn.msn > node->max_msn_applied_to_node_on_disk.msn);
    STAT64INFO_S stats_delta = {0,0};
    toku_ft_node_put_cmd(
        ft,
        &ft->cmp_descriptor,
        ubi_entry,
        node,
        childnum,
        cmd,
        true,
        gc_info,
        &stats_delta
        );
    if (stats_delta.numbytes || stats_delta.numrows) {
        toku_ft_update_stats(&ft->in_memory_stats, stats_delta);
    }
    //
    // assumption is that toku_ft_node_put_cmd will
    // mark the node as dirty.
    // enforcing invariant here.
    //
    paranoid_invariant(node->dirty != 0);

    // TODO: Why not at height 0?
    // update some status variables
    if (node->height != 0) {
        uint64_t msgsize = ft_msg_size(cmd);
        STATUS_INC(FT_MSG_BYTES_IN, msgsize);
        STATUS_INC(FT_MSG_BYTES_CURR, msgsize);
        STATUS_INC(FT_MSG_NUM, 1);
        if (ft_msg_applies_multiple(cmd)) {
            STATUS_INC(FT_MSG_NUM_BROADCAST, 1);
        }
    }

    // verify that msn of latest message was captured in root node
    paranoid_invariant(cmd->msn.msn == node->max_msn_applied_to_node_on_disk.msn);
}

static void
inject_message_in_locked_node(
    FT ft,
    FTNODE node,
    int childnum,
    struct ubi_entry *ubi_entry,
    FT_MSG_S *cmd,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info
    )
{
    inject_message_in_locked_node_no_unlocking(ft, node, childnum, ubi_entry, cmd, oldest_referenced_xid, gc_info);
    // if we call toku_ft_flush_some_child, then that function unpins the root
    // otherwise, we unpin ourselves
    if (node->height > 0 && toku_ft_nonleaf_is_gorged(node, ft->h->nodesize)) {
        toku_ft_flush_node_on_background_thread(ft, node);
    }
    else {
        toku_unpin_ftnode(ft, node);
    }
}

// seqinsert_loc is a bitmask.
// The root counts as being both on the "left extreme" and on the "right extreme".
// Therefore, at the root, you're at LEFT_EXTREME | RIGHT_EXTREME.
typedef char seqinsert_loc;
static const seqinsert_loc NEITHER_EXTREME = 0;
static const seqinsert_loc LEFT_EXTREME = 1;
static const seqinsert_loc RIGHT_EXTREME = 2;

static bool process_maybe_reactive_child(FT ft, FTNODE parent, FTNODE child, int childnum, seqinsert_loc loc)
// Effect:
//  If child needs to be split or merged, do that.
//  parent and child will be unlocked if this happens
//  also, the batched pin will have ended if this happens
// Requires: parent and child are read locked
// Returns:
//  true if relocking is needed
//  false otherwise
{
    enum reactivity re = get_node_reactivity(child, ft->h->nodesize);
    enum reactivity newre;
    BLOCKNUM child_blocknum;
    uint32_t child_fullhash;
    switch (re) {
    case RE_STABLE:
        return false;
    case RE_FISSIBLE:
        {
            // We only have a read lock on the parent.  We need to drop both locks, and get write locks.
            BLOCKNUM parent_blocknum = parent->thisnodename;
            uint32_t parent_fullhash = toku_cachetable_hash(ft->cf, parent_blocknum);
            int parent_height = parent->height;
            int parent_n_children = parent->n_children;
            toku_unpin_ftnode_read_only(ft, child);
            toku_unpin_ftnode_read_only(ft, parent);
            struct ftnode_fetch_extra bfe;
            fill_bfe_for_full_read(&bfe, ft);
            FTNODE newparent, newchild;
            toku_pin_ftnode_off_client_thread_batched(ft, parent_blocknum, parent_fullhash, &bfe, PL_WRITE_CHEAP, 0, nullptr, &newparent);
            if (newparent->height != parent_height || newparent->n_children != parent_n_children ||
                childnum >= newparent->n_children || toku_bnc_n_entries(BNC(newparent, childnum))) {
                // If the height changed or childnum is now off the end, something clearly got split or merged out from under us.
                // If something got injected in this node, then it got split or merged and we shouldn't be splitting it.
                // But we already unpinned the child so we need to have the caller re-try the pins.
                toku_unpin_ftnode_read_only(ft, newparent);
                return true;
            }
            // It's ok to reuse the same childnum because if we get something
            // else we need to split, well, that's crazy, but let's go ahead
            // and split it.
            child_blocknum = BP_BLOCKNUM(newparent, childnum);
            child_fullhash = compute_child_fullhash(ft->cf, newparent, childnum);
            toku_pin_ftnode_off_client_thread_batched(ft, child_blocknum, child_fullhash, &bfe, PL_WRITE_CHEAP, 1, &newparent, &newchild);
            newre = get_node_reactivity(newchild, ft->h->nodesize);
            if (newre == RE_FISSIBLE) {
                enum split_mode split_mode;
                if (newparent->height == 1 && (loc & LEFT_EXTREME) && childnum == 0) {
                    split_mode = SPLIT_RIGHT_HEAVY;
                } else if (newparent->height == 1 && (loc & RIGHT_EXTREME) && childnum == newparent->n_children - 1) {
                    split_mode = SPLIT_LEFT_HEAVY;
                } else {
                    split_mode = SPLIT_EVENLY;
                }
                toku_ft_split_child(ft, newparent, childnum, newchild, split_mode);
            } else {
                // some other thread already got it, just unpin and tell the
                // caller to retry
                toku_unpin_ftnode_read_only(ft, newchild);
                toku_unpin_ftnode_read_only(ft, newparent);
            }
            return true;
        }
    case RE_FUSIBLE:
        {
            if (parent->height == 1) {
                // prevent re-merging of recently unevenly-split nodes
                if (((loc & LEFT_EXTREME) && childnum <= 1) ||
                    ((loc & RIGHT_EXTREME) && childnum >= parent->n_children - 2)) {
                    return false;
                }
            }

            int parent_height = parent->height;
            BLOCKNUM parent_blocknum = parent->thisnodename;
            uint32_t parent_fullhash = toku_cachetable_hash(ft->cf, parent_blocknum);
            toku_unpin_ftnode_read_only(ft, child);
            toku_unpin_ftnode_read_only(ft, parent);
            struct ftnode_fetch_extra bfe;
            fill_bfe_for_full_read(&bfe, ft);
            FTNODE newparent, newchild;
            toku_pin_ftnode_off_client_thread_batched(ft, parent_blocknum, parent_fullhash, &bfe, PL_WRITE_CHEAP, 0, nullptr, &newparent);
            if (newparent->height != parent_height || childnum >= newparent->n_children) {
                // looks like this is the root and it got merged, let's just start over (like in the split case above)
                toku_unpin_ftnode_read_only(ft, newparent);
                return true;
            }
            child_blocknum = BP_BLOCKNUM(newparent, childnum);
            child_fullhash = compute_child_fullhash(ft->cf, newparent, childnum);
            toku_pin_ftnode_off_client_thread_batched(ft, child_blocknum, child_fullhash, &bfe, PL_READ, 1, &newparent, &newchild);
            newre = get_node_reactivity(newchild, ft->h->nodesize);
            if (newre == RE_FUSIBLE && newparent->n_children >= 2) {
                toku_unpin_ftnode_read_only(ft, newchild);
                toku_ft_merge_child(ft, newparent, childnum);
            } else {
                // Could be a weird case where newparent has only one
                // child.  In this case, we want to inject here but we've
                // already unpinned the caller's copy of parent so we have
                // to ask them to re-pin, or they could (very rarely)
                // dereferenced memory in a freed node.  TODO: we could
                // give them back the copy of the parent we pinned.
                //
                // Otherwise, some other thread already got it, just unpin
                // and tell the caller to retry
                toku_unpin_ftnode_read_only(ft, newchild);
                toku_unpin_ftnode_read_only(ft, newparent);
            }
            return true;
        }
    }
    abort();
}

static void
inject_message_at_this_blocknum(
    FT ft,
    CACHEKEY cachekey,
    uint32_t fullhash,
    FT_MSG_S *cmd,
    struct ubi_entry *ubi_entry,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info)
// Effect:
//  Inject cmd into the node at this blocknum (cachekey).
//  Gets a write lock on the node for you.
{
    FTNODE node;
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, ft);
    toku_pin_ftnode_off_client_thread_batched(ft, cachekey, fullhash, &bfe, PL_WRITE_CHEAP, 0, NULL, &node);
    toku_assert_entire_node_in_memory(node);
    paranoid_invariant(node->fullhash==fullhash);
    ft_verify_flags(ft, node);
    inject_message_in_locked_node(ft, node, -1, ubi_entry, cmd, oldest_referenced_xid, gc_info);
}

__attribute__((const))
static inline bool should_inject_in_node(seqinsert_loc loc, int height, int depth)
// We should inject directly in a node if:
//  - it's a leaf, or
//  - it's a height 1 node not at either extreme, or
//  - it's a depth 2 node not at either extreme
{
    return (height == 0 || (loc == NEITHER_EXTREME && (height <= 1 || depth >= 2)));
}

static void
helper_inject_message_at_blocknum(
    FT ft,
    FTNODE subtree_root,
    int depth,
    FT_MSG_S *cmd,
    struct ubi_entry *ubi_entry,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info
)
{
    // Right now we have a read lock on subtree_root, but we want
    // to inject into it so we get a write lock instead.
    BLOCKNUM subtree_root_blocknum = subtree_root->thisnodename;
    uint32_t subtree_root_fullhash = toku_cachetable_hash(ft->cf, subtree_root_blocknum);
    toku_unpin_ftnode_read_only(ft, subtree_root);
    switch (depth) {
    case 0:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_0, 1); break;
    case 1:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_1, 1); break;
    case 2:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_2, 1); break;
    case 3:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_3, 1); break;
    default:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_GT3, 1); break;
    }
    inject_message_at_this_blocknum(ft, subtree_root_blocknum, subtree_root_fullhash, cmd, ubi_entry, oldest_referenced_xid, gc_info);
}

static void
helper_inject_in_node(
    FT ft,
    FTNODE subtree_root,
    int target_childnum,
    struct ubi_entry *ubi_entry,
    FT_MSG_S *cmd,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info,
    int depth
)
{
    switch (depth) {
    case 0:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_0, 1); break;
    case 1:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_1, 1); break;
    case 2:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_2, 1); break;
    case 3:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_3, 1); break;
    default:
        STATUS_INC(FT_PRO_NUM_INJECT_DEPTH_GT3, 1); break;
    }
    inject_message_in_locked_node(ft, subtree_root, target_childnum, ubi_entry, cmd, oldest_referenced_xid, gc_info);
}

static void push_something_in_subtree(
    FT ft,
    FTNODE subtree_root,
    int target_childnum,
    FT_MSG_S *cmd,
    struct ubi_entry *ubi_entry,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info,
    int depth,
    seqinsert_loc loc,
    bool just_did_split_or_merge
    )
// Effects:
//  Assign cmd an MSN from ft->h.
//  Put cmd in the subtree rooted at node.  Due to promotion the message may not be injected directly in this node.
//  Unlock node or schedule it to be unlocked (after a background flush).
//   Either way, the caller is not responsible for unlocking node.
// Requires:
//  subtree_root is read locked and fully in memory.
// Notes:
//  In Ming, the basic rules of promotion are as follows:
//   Don't promote broadcast messages.
//   Don't promote past non-empty buffers.
//   Otherwise, promote at most to height 1 or depth 2 (whichever is highest), as far as the birdie asks you to promote.
//    We don't promote to leaves because injecting into leaves is expensive, mostly because of #5605 and some of #5552.
//    We don't promote past depth 2 because we found that gives us enough parallelism without costing us too much pinning work.
//
//    This is true with the following caveats:
//     We always promote all the way to the leaves on the rightmost and leftmost edges of the tree, for sequential insertions.
//      (That means we can promote past depth 2 near the edges of the tree.)
//
//   When the birdie is still saying we should promote, we use get_and_pin so that we wait to get the node.
//   If the birdie doesn't say to promote, we try maybe_get_and_pin.  If we get the node cheaply, and it's dirty, we promote anyway.
{
    toku_assert_entire_node_in_memory(subtree_root);
    assert(ft_msg_applies_once(cmd));

    if (should_inject_in_node(loc, subtree_root->height, depth)) {
        helper_inject_in_node(ft, subtree_root, target_childnum, ubi_entry, cmd,
                              oldest_referenced_xid, gc_info, depth);

        return;
    }
    int r;
    int childnum;
    NONLEAF_CHILDINFO bnc;

    if (target_childnum >= 0) {
        childnum = target_childnum;
    } else {
        childnum = toku_ftnode_which_child(subtree_root, cmd->key, &ft->cmp_descriptor, ft->key_ops.keycmp);
    }
    bnc = BNC(subtree_root, childnum);

    if (toku_bnc_n_entries(bnc) > 0 || BP_NOT_LIFTED(subtree_root, childnum).size) {
        // The buffer is non-empty, give up on promoting.
        STATUS_INC(FT_PRO_NUM_STOP_NONEMPTY_BUF, 1);
        goto relock_and_push_here;
    }

    seqinsert_loc next_loc;
    if ((loc & LEFT_EXTREME) && childnum == 0) {
        next_loc = LEFT_EXTREME;
    } else if ((loc & RIGHT_EXTREME) && childnum == subtree_root->n_children - 1) {
        next_loc = RIGHT_EXTREME;
    } else {
        next_loc = NEITHER_EXTREME;
    }

    if (next_loc == NEITHER_EXTREME && subtree_root->height <= 1) {
        // Never promote to leaf nodes except on the edges
        STATUS_INC(FT_PRO_NUM_STOP_H1, 1);
        goto relock_and_push_here;
    }

    {
        const BLOCKNUM child_blocknum = BP_BLOCKNUM(subtree_root, childnum);
        if (toku_blocknum_multi_copy(ft->blocktable, child_blocknum)) {
            goto relock_and_push_here;
        }
        toku_verify_blocknum_allocated(ft->blocktable, child_blocknum);
        const uint32_t child_fullhash = toku_cachetable_hash(ft->cf, child_blocknum);

        FTNODE child;
        {
            const int child_height = subtree_root->height - 1;
            const int child_depth = depth + 1;
            // If we're locking a leaf, or a height 1 node or depth 2
            // node in the middle, we know we won't promote further
            // than that, so just get a write lock now.
            pair_lock_type lock_type;
            if (should_inject_in_node(next_loc, child_height, child_depth)) {
                lock_type = PL_WRITE_CHEAP;
            } else {
                lock_type = PL_READ;
            }
            if (next_loc != NEITHER_EXTREME) {
                // If we're on either extreme, or the birdie wants to
                // promote and we're in the top two levels of the
                // tree, don't stop just because someone else has the
                // node locked.
                struct ftnode_fetch_extra bfe;
                fill_bfe_for_full_read(&bfe, ft);
                toku_pin_ftnode_off_client_thread_batched(
                    ft, child_blocknum, child_fullhash, &bfe,
                    lock_type, 0, nullptr, &child);
            } else {
                r = toku_maybe_pin_ftnode_clean(ft, child_blocknum, child_fullhash, lock_type, &child);
                if (r != 0) {
                    // We couldn't get the child cheaply, so give up on promoting.
                    STATUS_INC(FT_PRO_NUM_STOP_LOCK_CHILD, 1);
                    goto relock_and_push_here;
                }
                if (is_entire_node_in_memory(child)) {
                    // toku_pin_ftnode... touches the clock but toku_maybe_pin_ftnode... doesn't.
                    // This prevents partial eviction.
                    for (int i = 0; i < child->n_children; ++i) {
                        BP_TOUCH_CLOCK(child, i);
                    }
                } else {
                    // We got the child, but it's not fully in memory.  Give up on promoting.
                    STATUS_INC(FT_PRO_NUM_STOP_CHILD_INMEM, 1);
                    goto unlock_child_and_push_here;
                }
            }
        }
        assert(child != NULL);

        if (!just_did_split_or_merge) {
            BLOCKNUM subtree_root_blocknum = subtree_root->thisnodename;
            uint32_t subtree_root_fullhash = toku_cachetable_hash(ft->cf, subtree_root_blocknum);
            const bool did_split_or_merge = process_maybe_reactive_child(ft, subtree_root, child, childnum, loc);
            if (did_split_or_merge) {
                // Need to re-pin this node and try at this level again.
                FTNODE newparent;
                struct ftnode_fetch_extra bfe;
                fill_bfe_for_full_read(&bfe, ft); // should be fully in memory, we just split it
                toku_pin_ftnode_off_client_thread_batched(
                    ft, subtree_root_blocknum, subtree_root_fullhash, &bfe,
                    PL_READ, 0, nullptr, &newparent);
                push_something_in_subtree(ft, newparent, -1, cmd, ubi_entry, oldest_referenced_xid, gc_info, depth, loc, true);
                return;
            }
        }

        if (next_loc != NEITHER_EXTREME || child->dirty) {
            DBT lifted_key;
            const DBT *old_key = NULL;

            if (BP_LIFT(subtree_root, childnum).size) {
                old_key = cmd->key;
                r = toku_ft_lift_key_no_alloc(ft, &lifted_key, old_key, &BP_LIFT(subtree_root, childnum));
                assert_zero(r);
                cmd->key = &lifted_key;
            }
            assert_zero(BP_NOT_LIFTED(subtree_root, childnum).size);
            push_something_in_subtree(ft, child, -1, cmd, ubi_entry, oldest_referenced_xid, gc_info, depth + 1, next_loc, false);
            // The recursive call unpinned the child, but
            // we're responsible for unpinning subtree_root.

            if (old_key) {
                cmd->key = old_key;
            }
            toku_unpin_ftnode_read_only(ft, subtree_root);
            return;
        }

        STATUS_INC(FT_PRO_NUM_DIDNT_WANT_PROMOTE, 1);
unlock_child_and_push_here:
        // We locked the child, but we decided not to promote.
        // Unlock the child, and fall through to the next case.
        toku_unpin_ftnode_read_only(ft, child);
    }
relock_and_push_here:
    // Give up on promoting.
    // We have subtree_root read-locked and we don't have a child locked.
    // Drop the read lock, grab a write lock, and inject here.
    helper_inject_message_at_blocknum(ft, subtree_root, depth, cmd, ubi_entry,
                                      oldest_referenced_xid, gc_info);
}

void toku_ft_root_put_cmd(
    FT ft,
    FT_MSG_S *cmd,
    struct ubi_entry *ubi_entry,
    TXNID oldest_referenced_xid,
    GC_INFO gc_info
    )
// Effect:
//  - assign msn to cmd and update msn in the header
//  - push the cmd into the ft

// As of Clayface, the root blocknum is a constant, so preventing a
// race between message injection and the split of a root is the job
// of the cachetable's locking rules.
//
// We also hold the MO lock for a number of reasons, but an important
// one is to make sure that a begin_checkpoint may not start while
// this code is executing. A begin_checkpoint does (at least) two things
// that can interfere with the operations here:
//  - Copies the header to a checkpoint header. Because we may change
//    the max_msn_in_ft below, we don't want the header to be copied in
//    the middle of these operations.
//  - Takes note of the log's LSN. Because this put operation has
//    already been logged, this message injection must be included
//    in any checkpoint that contains this put's logentry.
//    Holding the mo lock throughout this function ensures that fact.
{
    // blackhole fractal trees drop all messages, so do nothing.
    if (ft->blackhole) {
        return;
    }

    FTNODE node;

    uint32_t fullhash;
    CACHEKEY root_key;
    toku_calculate_root_offset_pointer(ft, &root_key, &fullhash);
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, ft);

    pair_lock_type lock_type;
    lock_type = PL_READ; // try first for a read lock
    // If we need to split the root, we'll have to change from a read lock
    // to a write lock and check again.  We change the variable lock_type
    // and jump back to here.
 change_lock_type:
    // get the root node
    toku_pin_ftnode_off_client_thread_batched(ft, root_key, fullhash, &bfe, lock_type, 0, NULL, &node);
    toku_assert_entire_node_in_memory(node);
    paranoid_invariant(node->fullhash==fullhash);
    ft_verify_flags(ft, node);

    // First handle a reactive root.
    // This relocking for split algorithm will cause every message
    // injection thread to change lock type back and forth, when only one
    // of them needs to in order to handle the split.  That's not great,
    // but root splits are incredibly rare.
    enum reactivity re = get_node_reactivity(node, ft->h->nodesize);
    switch (re) {
    case RE_STABLE:
    case RE_FUSIBLE: // cannot merge anything at the root
        if (lock_type != PL_READ) {
            // We thought we needed to split, but someone else got to
            // it before us.  Downgrade to a read lock.
            toku_unpin_ftnode_read_only(ft, node);
            lock_type = PL_READ;
            goto change_lock_type;
        }
        break;
    case RE_FISSIBLE:
        if (lock_type == PL_READ) {
            // Here, we only have a read lock on the root.  In order
            // to split it, we need a write lock, but in the course of
            // gaining the write lock, someone else may have gotten in
            // before us and split it.  So we upgrade to a write lock
            // and check again.
            toku_unpin_ftnode_read_only(ft, node);
            lock_type = PL_WRITE_CHEAP;
            goto change_lock_type;
        } else {
            // We have a write lock, now we can split.
            ft_init_new_root(ft, node, &node);
            // Then downgrade back to a read lock, and we can finally
            // do the injection.
            toku_unpin_ftnode_off_client_thread(ft, node);
            lock_type = PL_READ;
            STATUS_INC(FT_PRO_NUM_ROOT_SPLIT, 1);
            goto change_lock_type;
        }
        break;
    }
    // If we get to here, we have a read lock and the root doesn't
    // need to be split.  It's safe to inject the message.
    paranoid_invariant(lock_type == PL_READ);
    // We cannot assert that we have the read lock because frwlock asserts
    // that its mutex is locked when we check if there are any readers.
    // That wouldn't give us a strong guarantee that we have the read lock
    // anyway.

    // Now, either inject here or promote.  We decide based on a heuristic:
    if (node->height == 0 || !ft_msg_applies_once(cmd)) {
        // If the root's a leaf or we're injecting a broadcast, drop the read lock and inject here.
        toku_unpin_ftnode_read_only(ft, node);
        STATUS_INC(FT_PRO_NUM_ROOT_H0_INJECT, 1);
        inject_message_at_this_blocknum(ft, root_key, fullhash, cmd, ubi_entry, oldest_referenced_xid, gc_info);
    } else if (node->height > 1) {
        // If the root's above height 1, we are definitely eligible for promotion.
        push_something_in_subtree(ft, node, -1, cmd, ubi_entry, oldest_referenced_xid, gc_info, 0, LEFT_EXTREME | RIGHT_EXTREME, false);
    } else {
        // The root's height 1.  We may be eligible for promotion here.
        // On the extremes, we want to promote, in the middle, we don't.
        int childnum = toku_ftnode_which_child(node, cmd->key, &ft->cmp_descriptor, ft->key_ops.keycmp);
        if (childnum == 0 || childnum == node->n_children - 1) {
            // On the extremes, promote.  We know which childnum we're going to, so pass that down too.
            push_something_in_subtree(ft, node, childnum, cmd, ubi_entry, oldest_referenced_xid, gc_info, 0, LEFT_EXTREME | RIGHT_EXTREME, false);
        } else {
            // At height 1 in the middle, don't promote, drop the read lock and inject here.
            toku_unpin_ftnode_read_only(ft, node);
            STATUS_INC(FT_PRO_NUM_ROOT_H1_INJECT, 1);
            inject_message_at_this_blocknum(ft, root_key, fullhash, cmd, ubi_entry, oldest_referenced_xid, gc_info);
        }
    }
}

// Effect: Insert the key-val pair into brt.
void toku_ft_insert (FT_HANDLE brt, DBT *key, DBT *val, TOKUTXN txn) {
    toku_ft_maybe_insert(brt, key, val, txn, false, ZERO_LSN, true, FT_INSERT);
}

void toku_ft_seq_insert (FT_HANDLE brt, DBT *key, DBT *val, TOKUTXN txn) {
    toku_ft_maybe_insert(brt, key, val, txn, false, ZERO_LSN, true, FT_UNBOUND_INSERT);
}

void toku_ft_load_recovery(TOKUTXN txn, FILENUM old_filenum, char const * new_iname, int do_fsync, int do_log, LSN *load_lsn) {
    paranoid_invariant(txn);
    toku_txn_force_fsync_on_commit(txn);  //If the txn commits, the commit MUST be in the log
                                          //before the (old) file is actually unlinked
    TOKULOGGER logger = toku_txn_logger(txn);

    BYTESTRING new_iname_bs = {.len=(uint32_t) strlen(new_iname), .data=(char*)new_iname};
    toku_logger_save_rollback_load(txn, old_filenum, &new_iname_bs);
    if (do_log && logger) {
        TXNID_PAIR xid = toku_txn_get_txnid(txn);
        toku_log_load(logger, load_lsn, do_fsync, txn, xid, old_filenum, new_iname_bs);
    }
}

// 2954
// this function handles the tasks needed to be recoverable
//  - write to rollback log
//  - write to recovery log
void toku_ft_hot_index_recovery(TOKUTXN txn, FILENUMS filenums, int do_fsync, int do_log, LSN *hot_index_lsn)
{
    paranoid_invariant(txn);
    TOKULOGGER logger = toku_txn_logger(txn);

    // write to the rollback log
    toku_logger_save_rollback_hot_index(txn, &filenums);
    if (do_log && logger) {
        TXNID_PAIR xid = toku_txn_get_txnid(txn);
        // write to the recovery log
        toku_log_hot_index(logger, hot_index_lsn, do_fsync, txn, xid, filenums);
    }
}

// Effect: Optimize the ft.
void toku_ft_optimize (FT_HANDLE brt) {
    TOKULOGGER logger = toku_cachefile_logger(brt->ft->cf);
    if (logger) {
        TXNID oldest = toku_txn_manager_get_oldest_living_xid(logger->txn_manager);

        XIDS root_xids = xids_get_root_xids();
        XIDS message_xids;
        if (oldest == TXNID_NONE_LIVING) {
            message_xids = root_xids;
        }
        else {
            int r = xids_create_child(root_xids, &message_xids, oldest);
            invariant(r == 0);
        }

        DBT key;
        DBT val;
        toku_init_dbt(&key);
        toku_init_dbt(&val);
        FT_MSG_S ftcmd;
        ft_msg_init(&ftcmd,  FT_OPTIMIZE, ZERO_MSN, message_xids, &key,&val);
        toku_ft_root_put_cmd(brt->ft, &ftcmd, nullptr, TXNID_NONE, make_gc_info(true));
        xids_destroy(&message_xids);
    }
}

void toku_ft_load(FT_HANDLE brt, TOKUTXN txn, char const * new_iname, int do_fsync, LSN *load_lsn) {
    FILENUM old_filenum = toku_cachefile_filenum(brt->ft->cf);
    int do_log = 1;
    toku_ft_load_recovery(txn, old_filenum, new_iname, do_fsync, do_log, load_lsn);
}

// ft actions for logging hot index filenums
void toku_ft_hot_index(FT_HANDLE brt __attribute__ ((unused)), TOKUTXN txn, FILENUMS filenums, int do_fsync, LSN *lsn) {
    int do_log = 1;
    toku_ft_hot_index_recovery(txn, filenums, do_fsync, do_log, lsn);
}

void
toku_ft_log_put (TOKUTXN txn, FT_HANDLE brt, const DBT *key, const DBT *val) {
    TOKULOGGER logger = toku_txn_logger(txn);
    if (logger) {
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        BYTESTRING valbs = {.len=val->size, .data=(char *) val->data};
        TXNID_PAIR xid = toku_txn_get_txnid(txn);
        toku_log_enq_insert(logger, (LSN*)0, 0, txn, toku_cachefile_filenum(brt->ft->cf), xid, keybs, valbs);
    }
}

void
toku_ft_log_put_multiple (TOKUTXN txn, FT_HANDLE src_ft, FT_HANDLE *brts, uint32_t num_fts, const DBT *key, const DBT *val) {
    assert(txn);
    assert(num_fts > 0);
    TOKULOGGER logger = toku_txn_logger(txn);
    if (logger) {
        FILENUM         fnums[num_fts];
        uint32_t i;
        for (i = 0; i < num_fts; i++) {
            fnums[i] = toku_cachefile_filenum(brts[i]->ft->cf);
        }
        FILENUMS filenums = {.num = num_fts, .filenums = fnums};
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        BYTESTRING valbs = {.len=val->size, .data=(char *) val->data};
        TXNID_PAIR xid = toku_txn_get_txnid(txn);
        FILENUM src_filenum = src_ft ? toku_cachefile_filenum(src_ft->ft->cf) : FILENUM_NONE;
        toku_log_enq_insert_multiple(logger, (LSN*)0, 0, txn, src_filenum, filenums, xid, keybs, valbs);
    }
}

void
toku_ft_maybe_insert(FT_HANDLE ft_h, DBT *key, DBT *val, TOKUTXN txn, bool oplsn_valid, LSN oplsn, bool do_logging, enum ft_msg_type type)
{
    paranoid_invariant(type==FT_INSERT || type==FT_INSERT_NO_OVERWRITE || type==FT_UNBOUND_INSERT);
    XIDS message_xids = xids_get_root_xids(); //By default use committed messages
    TXNID_PAIR xid = toku_txn_get_txnid(txn);
    if (txn) {
        BYTESTRING keybs = {key->size, (char *) key->data};
        if (type != FT_UNBOUND_INSERT) {
            toku_logger_save_rollback_cmdinsert(txn, toku_cachefile_filenum(ft_h->ft->cf), &keybs);
        } else {
            // perhaps don't need to differentiate between seqinsert here
            toku_logger_save_rollback_cmdseqinsert(txn, toku_cachefile_filenum(ft_h->ft->cf), &keybs);
        }
        toku_txn_maybe_note_ft(txn, ft_h->ft);
        message_xids = toku_txn_get_xids(txn);
    }
    TOKULOGGER logger = toku_txn_logger(txn);
    LSN ubi_lsn;
    if (do_logging && logger) {
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        BYTESTRING valbs = {.len=val->size, .data=(char *) val->data};
        if (type == FT_INSERT) {
            toku_log_enq_insert(logger, (LSN*)0, 0, txn, toku_cachefile_filenum(ft_h->ft->cf), xid, keybs, valbs);
        } else if (type == FT_INSERT_NO_OVERWRITE) {
            toku_log_enq_insert_no_overwrite(logger, (LSN*)0, 0, txn, toku_cachefile_filenum(ft_h->ft->cf), xid, keybs, valbs);
        } else {
            //FT_UNBOUND_INSERT
            toku_log_enq_unbound_insert(logger, &ubi_lsn, 0, txn, toku_cachefile_filenum(ft_h->ft->cf), xid, keybs);
        }
    }

    // without some extra work, we cannot use placeholder information
    // in our unbound_insert log entries. we are better off appending
    // the location information when we commit our insert
    // (toku_log_sync_unbound_insert).
    //Reason:
    // We cannot update the unbound_insert list with the cmd's node info
    // until the cmd is actually in the node.
    // But suppose another process tries to flush the log before the
    // insert operation returns. that process must write out all of
    // the unbound_insert values (i.e. the nodes containing those
    // values) before it can flush the log, but that is impossible
    // because the message is not yet in any node...

    LSN treelsn;
    if (oplsn_valid && oplsn.lsn <= (treelsn = toku_ft_checkpoint_lsn(ft_h->ft)).lsn) {
        // do nothing
        assert(!do_logging);
    } else {
        struct ubi_entry *entry = NULL;
        if (type == FT_UNBOUND_INSERT && do_logging && logger) {
            entry = toku_alloc_ubi_entry(UBI_INSERTING, ubi_lsn, ZERO_MSN, key);
        }

        TXNID oldest_referenced_xid = (txn) ? txn->oldest_referenced_xid : TXNID_NONE;
        toku_ft_send_insert(ft_h, key, val, message_xids, type, entry, oldest_referenced_xid,
                            make_gc_info(txn ? !txn->for_recovery : false));

        if (type == FT_UNBOUND_INSERT && do_logging && logger) {
            toku_logger_append_ubi_entry(logger, entry);
            if (!global_logger) {
                global_logger = logger;
            }
        }
    }
}

static void
ft_send_update_msg(FT_HANDLE brt, FT_MSG_S *msg, TOKUTXN txn) {
    msg->xids = (txn
                 ? toku_txn_get_xids(txn)
                 : xids_get_root_xids());

    TXNID oldest_referenced_xid = (txn) ? txn->oldest_referenced_xid : TXNID_NONE;
    toku_ft_root_put_cmd(brt->ft, msg, nullptr, oldest_referenced_xid, make_gc_info(txn ? !txn->for_recovery : false));
}

void toku_ft_maybe_update(FT_HANDLE ft_h, const DBT *key, const DBT *update_function_extra,
                      TOKUTXN txn, bool oplsn_valid, LSN oplsn,
                      bool do_logging) {
    TXNID_PAIR xid = toku_txn_get_txnid(txn);
    if (txn) {
        BYTESTRING keybs = { key->size, (char *) key->data };
        toku_logger_save_rollback_cmdupdate(
            txn, toku_cachefile_filenum(ft_h->ft->cf), &keybs);
        toku_txn_maybe_note_ft(txn, ft_h->ft);
    }

    TOKULOGGER logger;
    logger = toku_txn_logger(txn);
    if (do_logging && logger) {
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        BYTESTRING extrabs = {.len=update_function_extra->size,
                              .data = (char *) update_function_extra->data};
        toku_log_enq_update(logger, NULL, 0, txn,
                                toku_cachefile_filenum(ft_h->ft->cf),
                                xid, keybs, extrabs);
    }

    LSN treelsn;
    if (oplsn_valid && oplsn.lsn <= (treelsn = toku_ft_checkpoint_lsn(ft_h->ft)).lsn) {
        // do nothing
    } else {
        FT_MSG_S msg;
        ft_msg_init(&msg, FT_UPDATE, ZERO_MSN, NULL,
                         key, update_function_extra);
        ft_send_update_msg(ft_h, &msg, txn);
    }
}

void toku_ft_maybe_update_broadcast(FT_HANDLE ft_h, const DBT *update_function_extra,
                                TOKUTXN txn, bool oplsn_valid, LSN oplsn,
                                bool do_logging, bool is_resetting_op) {
    TXNID_PAIR xid = toku_txn_get_txnid(txn);
    uint8_t  resetting = is_resetting_op ? 1 : 0;
    if (txn) {
        toku_logger_save_rollback_cmdupdatebroadcast(txn, toku_cachefile_filenum(ft_h->ft->cf), resetting);
        toku_txn_maybe_note_ft(txn, ft_h->ft);
    }

    TOKULOGGER logger;
    logger = toku_txn_logger(txn);
    if (do_logging && logger) {
        BYTESTRING extrabs = {.len=update_function_extra->size,
                              .data = (char *) update_function_extra->data};
        toku_log_enq_updatebroadcast(logger, NULL, 0, txn,
                                         toku_cachefile_filenum(ft_h->ft->cf),
                                         xid, extrabs, resetting);
    }

    //TODO(yoni): remove treelsn here and similar calls (no longer being used)
    LSN treelsn;
    if (oplsn_valid &&
        oplsn.lsn <= (treelsn = toku_ft_checkpoint_lsn(ft_h->ft)).lsn) {

    } else {
        DBT nullkey;
        const DBT *nullkeyp = toku_init_dbt(&nullkey);
        FT_MSG_S msg;
        ft_msg_init(&msg,  FT_UPDATE_BROADCAST_ALL, ZERO_MSN, NULL,
                          nullkeyp, update_function_extra);
        ft_send_update_msg(ft_h, &msg, txn);
    }
}

void toku_ft_send_insert(FT_HANDLE brt, DBT *key, DBT *val, XIDS xids, enum ft_msg_type type, struct ubi_entry *entry, TXNID oldest_referenced_xid, GC_INFO gc_info) {
    FT_MSG_S ftcmd;
    ft_msg_init(&ftcmd, type, ZERO_MSN, xids, key, val);
    toku_ft_root_put_cmd(brt->ft, &ftcmd, entry, oldest_referenced_xid, gc_info);
}

void toku_ft_send_commit_any(FT_HANDLE brt, DBT *key, XIDS xids, TXNID oldest_referenced_xid, GC_INFO gc_info) {
    DBT val;
    FT_MSG_S ftcmd;
   ft_msg_init(&ftcmd, FT_COMMIT_ANY, ZERO_MSN, xids, key, toku_init_dbt(&val));
   toku_ft_root_put_cmd(brt->ft, &ftcmd, nullptr, oldest_referenced_xid, gc_info);
}

void toku_ft_rd(FT_HANDLE brt, DBT *min_key, DBT *max_key, TOKUTXN txn)
{
    DBT empty_dbt;
    toku_init_dbt(&empty_dbt);
    toku_ft_maybe_clone(brt, min_key, max_key, &empty_dbt, &empty_dbt,
                        &empty_dbt, &empty_dbt, false, txn, true);
}

void toku_ft_clone(FT_HANDLE brt, DBT *min_key, DBT *max_key,
                   DBT *new_min_key, DBT *new_max_key,
                   DBT *old_prefix, DBT *new_prefix,
                   bool delete_old, TOKUTXN txn)
{
    toku_ft_maybe_clone(brt, min_key, max_key, new_min_key, new_max_key,
                        old_prefix, new_prefix, delete_old, txn, true);
}

void toku_ft_delete(FT_HANDLE brt, DBT *key, TOKUTXN txn) {
    toku_ft_maybe_delete(brt, key, txn, false, ZERO_LSN, true);
}

void toku_ft_delete_multicast(FT_HANDLE brt, DBT *min_key, DBT *max_key,
                              bool is_right_excl, enum pacman_status pm_status,
                              TOKUTXN txn)
{
    toku_ft_maybe_delete_multicast(brt, min_key, max_key, is_right_excl,
                                   pm_status, txn, false, ZERO_LSN, false, true);
}

void
toku_ft_log_del(TOKUTXN txn, FT_HANDLE brt, const DBT *key) {
    TOKULOGGER logger = toku_txn_logger(txn);
    if (logger) {
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        TXNID_PAIR xid = toku_txn_get_txnid(txn);
        toku_log_enq_delete_any(logger, (LSN*)0, 0, txn, toku_cachefile_filenum(brt->ft->cf), xid, keybs);
    }
}

void
toku_ft_log_del_multiple (TOKUTXN txn, FT_HANDLE src_ft, FT_HANDLE *brts, uint32_t num_fts, const DBT *key, const DBT *val) {
    assert(txn);
    assert(num_fts > 0);
    TOKULOGGER logger = toku_txn_logger(txn);
    if (logger) {
        FILENUM         fnums[num_fts];
        uint32_t i;
        for (i = 0; i < num_fts; i++) {
            fnums[i] = toku_cachefile_filenum(brts[i]->ft->cf);
        }
        FILENUMS filenums = {.num = num_fts, .filenums = fnums};
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        BYTESTRING valbs = {.len=val->size, .data=(char *) val->data};
        TXNID_PAIR xid = toku_txn_get_txnid(txn);
        FILENUM src_filenum = src_ft ? toku_cachefile_filenum(src_ft->ft->cf) : FILENUM_NONE;
        toku_log_enq_delete_multiple(logger, (LSN*)0, 0, txn, src_filenum, filenums, xid, keybs, valbs);
    }
}

void toku_ft_maybe_delete(FT_HANDLE ft_h, DBT *key, TOKUTXN txn, bool oplsn_valid, LSN oplsn, bool do_logging) {
    XIDS message_xids = xids_get_root_xids(); //By default use committed messages
    TXNID_PAIR xid = toku_txn_get_txnid(txn);
    if (txn) {
        BYTESTRING keybs = {key->size, (char *) key->data};
        toku_logger_save_rollback_cmddelete(txn, toku_cachefile_filenum(ft_h->ft->cf), &keybs);
        toku_txn_maybe_note_ft(txn, ft_h->ft);
        message_xids = toku_txn_get_xids(txn);
    }
    TOKULOGGER logger = toku_txn_logger(txn);
    if (do_logging && logger) {
        BYTESTRING keybs = {.len=key->size, .data=(char *) key->data};
        toku_log_enq_delete_any(logger, (LSN*)0, 0, txn, toku_cachefile_filenum(ft_h->ft->cf), xid, keybs);
    }

    LSN treelsn;
    if (oplsn_valid && oplsn.lsn <= (treelsn = toku_ft_checkpoint_lsn(ft_h->ft)).lsn) {
        // do nothing
    } else {
        TXNID oldest_referenced_xid = (txn) ? txn->oldest_referenced_xid : TXNID_NONE;
        toku_ft_send_delete(ft_h, key, message_xids, oldest_referenced_xid, make_gc_info(txn ? !txn->for_recovery : false));
    }
}

struct setkey_extra {
    int error;
    DBT *new_key;
};

static void setkey_func(const DBT *new_key, void *extra)
{
    struct setkey_extra *setkey_extra = (struct setkey_extra *)extra;

    if (new_key->size == 0) {
        toku_init_dbt(setkey_extra->new_key);
    } else {
        toku_clone_dbt(setkey_extra->new_key, *new_key);
        setkey_extra->error = (setkey_extra->new_key->data == NULL) ?
                              ENOMEM : 0;
    }
}

int toku_ft_lift(FT ft, DBT *lift, const DBT *lpivot, const DBT *rpivot)
{
    int r;
    struct setkey_extra extra;

    // nothing to lift
    if (lpivot->size == 0 || rpivot->size == 0 || ft->key_ops.keylift == NULL) {
        toku_init_dbt(lift);
        return 0;
    }

    extra.error = 0;
    extra.new_key = lift;

    FAKE_DB(db, &ft->cmp_descriptor);
    r = ft->key_ops.keylift(&db, lpivot, rpivot, setkey_func, &extra);
    if (r == 0)
        r = extra.error;

    return r;
}

int toku_ft_lift_key(FT ft, DBT *lifted_key, const DBT *key, const DBT *lift)
{
    int r;
    struct setkey_extra extra;

    if (lift->size == 0) {
        if (key->size == 0) {
            toku_init_dbt(lifted_key);
        } else {
            toku_clone_dbt(lifted_key, *key);
        }
        return 0;
    }

    extra.error = 0;
    extra.new_key = lifted_key;

    r = ft->key_ops.liftkey(key, lift, setkey_func, &extra);
    if (r == 0)
        r = extra.error;

    return r;
}

// XXX: the proper way should be passing a no_alloc_flag to user, but this is
//   easier
struct setkey_no_alloc_extra {
    int error;
    DBT *new_key;
};

static void setkey_no_alloc_func(const DBT *new_key, void *extra)
{
    struct setkey_no_alloc_extra *setkey_no_alloc_extra;

    setkey_no_alloc_extra = (struct setkey_no_alloc_extra *)extra;

    if (new_key->size == 0) {
        toku_init_dbt(setkey_no_alloc_extra->new_key);
    } else {
        toku_copy_dbt(setkey_no_alloc_extra->new_key, *new_key);
    }
}

int toku_ft_lift_key_no_alloc(FT UU(ft), DBT *lifted_key,
                              const DBT *key, const DBT *lift)
{
    int r;
    struct setkey_no_alloc_extra extra;

    if (lift->size == 0) {
        if (key->size == 0) {
            toku_init_dbt(lifted_key);
        } else {
            toku_copy_dbt(lifted_key, *key);
        }
        return 0;
    }

    char *key_buf = (char*)key->data;
    if (key_buf[0] == 'm' && key->size <= lift->size &&
        key_buf[key->size-1] == 0x00 && key_buf[key->size-2] == 0x01) {
        lifted_key->data = &key_buf[key->size-2];
        lifted_key->size = 2;
        lifted_key->ulen = 0;
        lifted_key->flags = DB_DBT_MALLOC;
        return 0;
    }

    extra.error = 0;
    extra.new_key = lifted_key;

    r = ft->key_ops.liftkey(key, lift, setkey_no_alloc_func, &extra);
    if (r == 0)
        r = extra.error;

    return r;
}

int
toku_ft_unlift_key(FT ft, DBT *key, const DBT *lifted_key, const DBT *lift)
{
    int r;
    struct setkey_extra extra;

    if (lift->size == 0) {
        if (lifted_key->size == 0) {
            toku_init_dbt(key);
        } else {
            toku_clone_dbt(key, *lifted_key);
        }
        return 0;
    }

    extra.error = 0;
    extra.new_key = key;

    r = ft->key_ops.unliftkey(lifted_key, lift, setkey_func, &extra);
    if (r == 0)
        r = extra.error;

    return r;
}

int
toku_ft_transform_prefix(FT ft, const DBT *old_prefix, const DBT *new_prefix,
                         const DBT *old_key, DBT *new_key)
{
    int r;
    DBT lifted_key;

    if (old_prefix->size > 0) {
        r = toku_ft_lift_key_no_alloc(ft, &lifted_key, old_key, old_prefix);
        if (r)
            return r;
    } else {
        toku_copy_dbt(&lifted_key, *old_key);
    }

    if (new_prefix->size > 0) {
        r = toku_ft_unlift_key(ft, new_key, &lifted_key, new_prefix);
        if (r)
            return r;
    } else {
        toku_clone_dbt(new_key, lifted_key);
    }

    return 0;
}

static void
ft_inject_rd(FT ft, DBT *min_key, DBT *max_key, TOKUTXN txn, FTNODE root)
{
    FT_MSG_S msg;
    XIDS message_xids;
    TXNID oldest_referenced_xid;

    if (txn) {
        message_xids = toku_txn_get_xids(txn);
        oldest_referenced_xid = txn->oldest_referenced_xid;
    } else {
        message_xids = xids_get_root_xids();
        oldest_referenced_xid = TXNID_NONE;
    }

    ft_msg_goto_init(&msg, ZERO_MSN, message_xids, min_key, max_key,
                     get_dbt_empty(), {0}, 0);
    inject_message_in_locked_node_no_unlocking(
        ft, root, -1, NULL, &msg, oldest_referenced_xid,
        make_gc_info(txn ? !txn->for_recovery : false));
}

void
toku_ft_rd_commit(FT ft, DBT *min_key, DBT *max_key, TOKUTXN txn)
{
    // pin root
    FTNODE root = NULL;
    {
        struct ftnode_fetch_extra bfe;
        CACHEKEY blocknum;
        uint32_t fullhash;
        fill_bfe_for_full_read(&bfe, ft);
        toku_calculate_root_offset_pointer(ft, &blocknum, &fullhash);
        toku_pin_ftnode_off_client_thread_batched(
            ft, blocknum, fullhash, &bfe, PL_WRITE_EXPENSIVE, 0, NULL, &root);
    }

    ft_inject_rd(ft, min_key, max_key, txn, root);

    // unlock root
    // this is copied from the end of inject_message_in_locked_node.
    if (root->height > 0 && toku_ft_nonleaf_is_gorged(root, ft->h->nodesize)) {
        toku_ft_flush_node_on_background_thread(ft, root);
    } else {
        toku_unpin_ftnode(ft, root);
    }
}

void
toku_ft_clone_commit(FT ft, DBT *min_key, DBT *max_key,
                     DBT *new_min_key, DBT *new_max_key, bool delete_old,
                     TOKUTXN txn)
{
    // pin root
    FTNODE root = NULL;
    {
        struct ftnode_fetch_extra bfe;
        CACHEKEY blocknum;
        uint32_t fullhash;
        fill_bfe_for_full_read(&bfe, ft);
        toku_calculate_root_offset_pointer(ft, &blocknum, &fullhash);
        toku_pin_ftnode_off_client_thread_batched(
            ft, blocknum, fullhash, &bfe, PL_WRITE_EXPENSIVE, 0, NULL, &root);
    }

    int r;
    int childnum_l, childnum_r;
    FTNODE LCA;
    DBT key_l, key_r, alloc_l, alloc_r;
    LCA = root;
    toku_copy_dbt(&key_l, *min_key);
    toku_copy_dbt(&key_r, *max_key);
    toku_init_dbt(&alloc_l);
    toku_init_dbt(&alloc_r);
    while (LCA->height > 0) {
        // here we use ft_clone_which_child, which means if pivot[i] == min_key,
        // childnum_l == i + 1. This would push LCA deeper, but that should be
        // better
        ft_clone_which_child(ft, LCA, &key_l, &key_r, &childnum_l, &childnum_r);
        if (childnum_l != childnum_r) {
            break;
        } else {
            // pin child
            FTNODE child = NULL;
            ft_nonleaf_fetch_child_and_flush(ft, LCA, childnum_l, &child);

            // lift/unlift keys
            ft_process_key(ft, &BP_LIFT(LCA, childnum_l),
                           &BP_NOT_LIFTED(LCA, childnum_l), &key_l, &alloc_l);
            ft_process_key(ft, &BP_LIFT(LCA, childnum_l),
                           &BP_NOT_LIFTED(LCA, childnum_l), &key_r, &alloc_r);

            // maybe unlock the parent
            if (LCA != root)
               toku_unpin_ftnode(ft, LCA);
            LCA = child;
        }
    }

    // now LCA = src_LCA
    if (LCA->height == 0) {
        // src is empty?
        ft_clone_which_child(ft, LCA, &key_l, &key_r, &childnum_l, &childnum_r);
        if (childnum_l == childnum_r) {
            BN_DATA bd = BLB_DATA(LCA, childnum_l);
            if (ft_node_clone_search(ft, bd, &key_l) ==
                    ft_node_clone_search(ft, bd, &key_r)) {
                // if the src is empty, we don't need to do anything
                if (LCA != root) {
                    toku_unpin_ftnode(ft, LCA);
                }
                goto unlock_root_out;
            }
        }
    }

    if (LCA == root) {
        FTNODE newroot;
        ft_init_new_root_no_split_no_unlock(ft, root, &newroot);
        root = newroot;
        // LCA is the old root
    }

    XIDS message_xids;
    TXNID oldest_referenced_xid;
    if (txn) {
        message_xids = toku_txn_get_xids(txn);
        oldest_referenced_xid = txn->oldest_referenced_xid;
    } else {
        message_xids = xids_get_root_xids();
        oldest_referenced_xid = TXNID_NONE;
    }

    {
        DBT src_not_lifted;
        toku_init_dbt(&src_not_lifted);
        r = toku_ft_lift(ft, &src_not_lifted, &key_l, &key_r);
        assert_zero(r);
        // key_l and key_r are no longer useful
        toku_cleanup_dbt(&alloc_l);
        toku_cleanup_dbt(&alloc_r);

        FT_MSG_S msg;
        CACHEKEY blocknum = LCA->thisnodename;
        toku_blocknum_inc_refc(ft->blocktable, blocknum, ft);
        // we have created a new root if LCA is the root, so LCA != root
        // smart_pvt finalize might cause locking, release here
        toku_unpin_ftnode(ft, LCA);

        ft_msg_goto_init(&msg, ZERO_MSN, message_xids,
                         new_min_key, new_max_key, &src_not_lifted,
                         blocknum, LCA->height);
        childnum_l = ft_clone_maybe_merge_and_gc(ft, root,
                                                 new_min_key, new_max_key);
        inject_message_in_locked_node_no_unlocking(
            ft, root, childnum_l, NULL, &msg, oldest_referenced_xid,
            make_gc_info(txn ? !txn->for_recovery : false));

        toku_cleanup_dbt(&src_not_lifted);
    }

    // for rename, use RD to delete old
    if (delete_old) {
        ft_inject_rd(ft, min_key, max_key, txn, root);
    }


unlock_root_out:
    // unlock root
    // this is copied from the end of inject_message_in_locked_node.
    if (root->height > 0 && toku_ft_nonleaf_is_gorged(root, ft->h->nodesize)) {
        toku_ft_flush_node_on_background_thread(ft, root);
    } else {
        toku_unpin_ftnode(ft, root);
    }
}

void toku_ft_maybe_clone(FT_HANDLE brt, DBT *min_key, DBT *max_key,
                         DBT *new_min_key, DBT *new_max_key,
                         DBT *old_prefix, DBT *new_prefix, bool delete_old,
                         TOKUTXN txn, bool do_logging)
{
    TXNID_PAIR xid = toku_txn_get_txnid(txn);

    BYTESTRING minkeybs = {min_key->size, (char *)min_key->data};
    BYTESTRING maxkeybs = {max_key->size, (char *)max_key->data};
    BYTESTRING newminkeybs = {new_min_key->size, (char *)new_min_key->data};
    BYTESTRING newmaxkeybs = {new_max_key->size, (char *)new_max_key->data};
    BYTESTRING oldprefixbs = {old_prefix->size, (char *)old_prefix->data};
    BYTESTRING newprefixbs = {new_prefix->size, (char *)new_prefix->data};

    if (txn) {
        toku_logger_save_rollback_cmdclone(txn, toku_cachefile_filenum(brt->ft->cf),
                                           &minkeybs, &maxkeybs, &newminkeybs, &newmaxkeybs,
                                           &oldprefixbs, &newprefixbs, delete_old);
        toku_txn_maybe_note_ft(txn, brt->ft);
    }
    TOKULOGGER logger = toku_txn_logger(txn);
    if (do_logging && logger) {
        toku_log_enq_clone(logger, (LSN *)0, 0, txn,
                           toku_cachefile_filenum(brt->ft->cf), xid,
                           minkeybs, maxkeybs, newminkeybs, newmaxkeybs,
                           oldprefixbs, newprefixbs, delete_old);
    }
}

void toku_ft_maybe_delete_multicast(
    FT_HANDLE ft_h,
    DBT *min_key,
    DBT *max_key,
    bool is_right_excl,
    enum pacman_status pm_status,
    TOKUTXN txn,
    bool oplsn_valid,
    LSN oplsn,
    bool is_resetting_op,
    bool do_logging
    )
{
    uint8_t resetting = is_resetting_op ? 1 : 0;
    // By default use committed messages
    XIDS message_xids = xids_get_root_xids();
    TXNID_PAIR xid = toku_txn_get_txnid(txn);
    if (txn) {
        BYTESTRING minkeybs = {min_key->size, (char *)min_key->data};
        BYTESTRING maxkeybs = {max_key->size, (char *)max_key->data};
        toku_logger_save_rollback_cmddeletemulti(
            txn, toku_cachefile_filenum(ft_h->ft->cf), &minkeybs, &maxkeybs,
            is_right_excl, pm_status, resetting);
        toku_txn_maybe_note_ft(txn, ft_h->ft);
        message_xids = toku_txn_get_xids(txn);
    }
    TOKULOGGER logger = toku_txn_logger(txn);
    if (do_logging && logger) {
        BYTESTRING min_keybs = {.len=min_key->size, .data=(char *)min_key->data};
        BYTESTRING max_keybs = {.len=max_key->size, .data=(char *)max_key->data};
        toku_log_enq_delete_multi(
            logger, (LSN*)0, 0, txn, toku_cachefile_filenum(ft_h->ft->cf), xid,
            min_keybs, max_keybs, is_right_excl, pm_status, resetting);
    }

    LSN treelsn;
    if (oplsn_valid && oplsn.lsn <= (treelsn = toku_ft_checkpoint_lsn(ft_h->ft)).lsn) {
        // do nothing
    } else {
        TXNID oldest_referenced_xid = (txn) ? txn->oldest_referenced_xid : TXNID_NONE;

        DBT val;
        toku_init_dbt(&val);
        FT_MSG_S msg;
        ft_msg_multicast_init(&msg, FT_DELETE_MULTICAST, ZERO_MSN, message_xids,
                              min_key, max_key, &val, is_right_excl, pm_status);
        toku_ft_root_put_cmd(ft_h->ft, &msg, nullptr, oldest_referenced_xid,
                             make_gc_info(txn ? !txn->for_recovery : false));
    }
}
void toku_ft_send_delete(FT_HANDLE brt, DBT *key, XIDS xids, TXNID oldest_referenced_xid, GC_INFO gc_info) {
    DBT val; toku_init_dbt(&val);
    FT_MSG_S ftcmd;
    ft_msg_init(&ftcmd,  FT_DELETE_ANY, ZERO_MSN, xids,  key, &val );
    toku_ft_root_put_cmd(brt->ft, &ftcmd, nullptr, oldest_referenced_xid, gc_info);
}

/* ******************** open,close and create  ********************** */

// Test only function (not used in running system). This one has no env
int toku_open_ft_handle (const char *fname, int is_create, FT_HANDLE *ft_handle_p, int nodesize,
                   int basementnodesize,
                   enum toku_compression_method compression_method,
                   CACHETABLE cachetable, TOKUTXN txn,
                   int (*compare_fun)(DB *, const DBT*,const DBT*)) {
    FT_HANDLE brt;
    const int only_create = 0;

    toku_ft_handle_create(&brt);
    toku_ft_handle_set_nodesize(brt, nodesize);
    toku_ft_handle_set_basementnodesize(brt, basementnodesize);
    toku_ft_handle_set_compression_method(brt, compression_method);
    struct toku_db_key_operations key_ops;
    memset(&key_ops, 0, sizeof(key_ops));
    key_ops.keycmp = compare_fun;
    key_ops.keyprint = toku_builtin_print_fun;
    toku_ft_set_key_ops(brt, &key_ops);

    int r = toku_ft_handle_open(brt, fname, is_create, only_create, cachetable, txn);
    if (r != 0) {
        return r;
    }

    *ft_handle_p = brt;
    return r;
}

/* dup open_ft_handle*/
int toku_open_ft_handle_key_ops(
        const char *fname, int is_create, FT_HANDLE *ft_handle_p, int nodesize,
        int basementnodesize,
        enum toku_compression_method compression_method,
        CACHETABLE cachetable, TOKUTXN txn,
        struct toku_db_key_operations *key_ops)
{
    FT_HANDLE brt;
    const int only_create = 0;

    toku_ft_handle_create(&brt);
    toku_ft_handle_set_nodesize(brt, nodesize);
    toku_ft_handle_set_basementnodesize(brt, basementnodesize);
    toku_ft_handle_set_compression_method(brt, compression_method);
    toku_ft_set_key_ops(brt, key_ops);

    int r = toku_ft_handle_open(brt, fname, is_create, only_create, cachetable, txn);
    if (r != 0) {
        return r;
    }

    *ft_handle_p = brt;
    return r;
}


static bool use_direct_io = true;

void toku_ft_set_direct_io (bool direct_io_on) {
    use_direct_io = direct_io_on;
}

static inline int ft_open_maybe_direct(const char *filename, int oflag, int mode) {
    if (use_direct_io) {
        return toku_os_open_direct(filename, oflag, mode);
    } else {
        return toku_os_open(filename, oflag, mode);
    }
}

// open a file for use by the brt
// Requires:  File does not exist.
static int ft_create_file(FT_HANDLE UU(brt), const char *fname, int *fdp) {
    mode_t mode = S_IRWXU|S_IRWXG|S_IRWXO;
    int r;
    int fd;
    //int er;
    fd = ft_open_maybe_direct(fname, O_RDWR | O_BINARY, mode);
    assert(fd < 0);
    fd = ft_open_maybe_direct(fname, O_RDWR | O_CREAT | O_BINARY, mode);
    if (fd < 0) {
        r = -fd;
        return r;
    }
    r = toku_fsync_directory(fname);
    if (r == 0) {
        *fdp = fd;
    } else {
        int rr = close(fd);
        assert_zero(rr);
    }
    return r;
}

// open a file for use by the brt.  if the file does not exist, error
static int ft_open_file(const char *fname, int *fdp) {
    mode_t mode = S_IRWXU|S_IRWXG|S_IRWXO;
    int fd;
    fd = ft_open_maybe_direct(fname, O_RDWR | O_BINARY, mode);
    if (fd < 0){
        return get_error_errno(fd);
    }
    *fdp = fd;
    return 0;
}

void
toku_ft_handle_set_compression_method(FT_HANDLE t, enum toku_compression_method method)
{
    if (t->ft) {
        toku_ft_set_compression_method(t->ft, method);
    }
    else {
        t->options.compression_method = method;
    }
}

void
toku_ft_handle_get_compression_method(FT_HANDLE t, enum toku_compression_method *methodp)
{
    if (t->ft) {
        toku_ft_get_compression_method(t->ft, methodp);
    }
    else {
        *methodp = t->options.compression_method;
    }
}

extern struct toku_db_key_operations toku_builtin_key_ops;

static int
verify_builtin_comparisons_consistent(FT_HANDLE t, uint32_t flags) {
    if ((flags & TOKU_DB_KEYCMP_BUILTIN) && (memcmp(&t->options.key_ops, &toku_builtin_key_ops, sizeof(toku_builtin_key_ops) != 0)))
        return EINVAL;
    return 0;
}

//
// See comments in toku_db_change_descriptor to understand invariants
// in the system when this function is called
//
void toku_ft_change_descriptor(
    FT_HANDLE ft_h,
    const DBT* old_descriptor,
    const DBT* new_descriptor,
    bool do_log,
    TOKUTXN txn,
    bool update_cmp_descriptor
    )
{
    DESCRIPTOR_S new_d;

    // if running with txns, save to rollback + write to recovery log
    if (txn) {
        // put information into rollback file
        BYTESTRING old_desc_bs = { old_descriptor->size, (char *) old_descriptor->data };
        BYTESTRING new_desc_bs = { new_descriptor->size, (char *) new_descriptor->data };
        toku_logger_save_rollback_change_fdescriptor(
            txn,
            toku_cachefile_filenum(ft_h->ft->cf),
            &old_desc_bs
            );
        toku_txn_maybe_note_ft(txn, ft_h->ft);

        if (do_log) {
            TOKULOGGER logger = toku_txn_logger(txn);
            TXNID_PAIR xid = toku_txn_get_txnid(txn);
            toku_log_change_fdescriptor(
                logger, NULL, 0,
                txn,
                toku_cachefile_filenum(ft_h->ft->cf),
                xid,
                old_desc_bs,
                new_desc_bs,
                update_cmp_descriptor
                );
        }
    }

    // write new_descriptor to header
    new_d.dbt = *new_descriptor;
    toku_ft_update_descriptor(ft_h->ft, &new_d);
    // very infrequent operation, worth precise threadsafe count
    STATUS_INC(FT_DESCRIPTOR_SET, 1);

    if (update_cmp_descriptor) {
        toku_ft_update_cmp_descriptor(ft_h->ft);
    }
}

static void
toku_ft_handle_inherit_options(FT_HANDLE t, FT ft) {
    struct ft_options options;
    options.nodesize = ft->h->nodesize;
    options.basementnodesize = ft->h->basementnodesize;
    options.compression_method = ft->h->compression_method;
    options.flags = ft->h->flags;
    memcpy(&options.key_ops, &ft->key_ops, sizeof(ft->key_ops));
    options.update_fun = ft->update_fun;
    t->options = options;
    t->did_set_flags = true;
}

// This is the actual open, used for various purposes, such as normal use, recovery, and redirect.
// fname_in_env is the iname, relative to the env_dir  (data_dir is already in iname as prefix).
// The checkpointed version (checkpoint_lsn) of the dictionary must be no later than max_acceptable_lsn .
// Requires: The multi-operation client lock must be held to prevent a checkpoint from occuring.
static int
ft_handle_open(FT_HANDLE ft_h, const char *fname_in_env, int is_create, int only_create, CACHETABLE cachetable, TOKUTXN txn, FILENUM use_filenum, DICTIONARY_ID use_dictionary_id, LSN max_acceptable_lsn) {
    int r;
    bool txn_created = false;
    char *fname_in_cwd = NULL;
    CACHEFILE cf = NULL;
    FT ft = NULL;
    bool did_create = false;
    toku_ft_open_close_lock();

    if (ft_h->did_set_flags) {
        r = verify_builtin_comparisons_consistent(ft_h, ft_h->options.flags);
        if (r!=0) { goto exit; }
    }

    assert(is_create || !only_create);
    FILENUM reserved_filenum;
    reserved_filenum = use_filenum;
    fname_in_cwd = toku_cachetable_get_fname_in_cwd(cachetable, fname_in_env);
    bool was_already_open;
    {
        int fd = -1;
        r = ft_open_file(fname_in_cwd, &fd);
        if (reserved_filenum.fileid == FILENUM_NONE.fileid) {
            reserved_filenum = toku_cachetable_reserve_filenum(cachetable);
        }
        if (r ==ENOENT && is_create) {
            did_create = true;
            mode_t mode = S_IRWXU|S_IRWXG|S_IRWXO;
            if (txn) {
                BYTESTRING bs = { .len=(uint32_t) strlen(fname_in_env), .data = (char*)fname_in_env };
                toku_logger_save_rollback_fcreate(txn, reserved_filenum, &bs); // bs is a copy of the fname relative to the environment
            }
            txn_created = (bool)(txn!=NULL);
            toku_logger_log_fcreate(txn, fname_in_env, reserved_filenum, mode, ft_h->options.flags, ft_h->options.nodesize, ft_h->options.basementnodesize, ft_h->options.compression_method);
            r = ft_create_file(ft_h, fname_in_cwd, &fd);
            if (r) { goto exit; }
        }
        if (r) { goto exit; }
        r=toku_cachetable_openfd_with_filenum(&cf, cachetable, fd, fname_in_env, reserved_filenum, &was_already_open);
        if (r) { goto exit; }
    }
    assert(ft_h->options.nodesize>0);
    if (is_create) {
        r = toku_read_ft_and_store_in_cachefile(ft_h, cf, max_acceptable_lsn, &ft);
        if (r==TOKUDB_DICTIONARY_NO_HEADER) {
            toku_ft_create(&ft, &ft_h->options, cf, txn);
        }
        else if (r!=0) {
            goto exit;
        }
        else if (only_create) {
            assert_zero(r);
            r = EEXIST;
            goto exit;
        }
        // if we get here, then is_create was true but only_create was false,
        // so it is ok for toku_read_ft_and_store_in_cachefile to have read
        // the header via toku_read_ft_and_store_in_cachefile
    } else {
        r = toku_read_ft_and_store_in_cachefile(ft_h, cf, max_acceptable_lsn, &ft);
        if (r) { goto exit; }
    }
    if (!ft_h->did_set_flags) {
        r = verify_builtin_comparisons_consistent(ft_h, ft_h->options.flags);
        if (r) { goto exit; }
    } else if (ft_h->options.flags != ft->h->flags) {                  /* if flags have been set then flags must match */
        r = EINVAL;
        goto exit;
    }
    toku_ft_handle_inherit_options(ft_h, ft);
    if (!was_already_open) {
        if (!did_create) { //Only log the fopen that OPENs the file.  If it was already open, don't log.
            toku_logger_log_fopen(txn, fname_in_env, toku_cachefile_filenum(cf), ft_h->options.flags);
        }
    }
    int use_reserved_dict_id;
    use_reserved_dict_id = use_dictionary_id.dictid != DICTIONARY_ID_NONE.dictid;
    if (!was_already_open) {
        DICTIONARY_ID dict_id;
        if (use_reserved_dict_id) {
            dict_id = use_dictionary_id;
        }
        else {
            dict_id = next_dict_id();
        }
        ft->dict_id = dict_id;
    }
    else {
        // dict_id is already in header
        if (use_reserved_dict_id) {
            assert(ft->dict_id.dictid == use_dictionary_id.dictid);
        }
    }
    assert(ft);
    assert(ft->dict_id.dictid != DICTIONARY_ID_NONE.dictid);
    assert(ft->dict_id.dictid < dict_id_serial);

    // important note here,
    // after this point, where we associate the header
    // with the brt, the function is not allowed to fail
    // Code that handles failure (located below "exit"),
    // depends on this
    toku_ft_note_ft_handle_open(ft, ft_h);
    if (txn_created) {
        assert(txn);
        toku_txn_maybe_note_ft(txn, ft);
    }

    // YZJ: No need to truncate db file when it it just opened

    r = 0;
exit:
    if (fname_in_cwd) {
        toku_free(fname_in_cwd);
    }
    if (r != 0 && cf) {
        if (ft) {
            // we only call toku_ft_note_ft_handle_open
            // when the function succeeds, so if we are here,
            // then that means we have a reference to the header
            // but we have not linked it to this brt. So,
            // we can simply try to remove the header.
            // We don't need to unlink this brt from the header
            toku_ft_grab_reflock(ft);
            bool needed = toku_ft_needed_unlocked(ft);
            toku_ft_release_reflock(ft);
            if (!needed) {
                // close immediately.
                toku_ft_evict_from_memory(ft, false, ZERO_LSN);
            }
        }
        else {
            toku_cachefile_close(&cf, false, ZERO_LSN);
        }
    }
    toku_ft_open_close_unlock();
    return r;
}

// Open a brt for the purpose of recovery, which requires that the brt be open to a pre-determined FILENUM
// and may require a specific checkpointed version of the file.
// (dict_id is assigned by the ft_handle_open() function.)
int
toku_ft_handle_open_recovery(FT_HANDLE t, const char *fname_in_env, int is_create, int only_create, CACHETABLE cachetable, TOKUTXN txn, FILENUM use_filenum, LSN max_acceptable_lsn) {
    int r;
    assert(use_filenum.fileid != FILENUM_NONE.fileid);
    r = ft_handle_open(t, fname_in_env, is_create, only_create, cachetable,
                 txn, use_filenum, DICTIONARY_ID_NONE, max_acceptable_lsn);
    return r;
}

// Open a brt in normal use.  The FILENUM and dict_id are assigned by the ft_handle_open() function.
// Requires: The multi-operation client lock must be held to prevent a checkpoint from occuring.
int
toku_ft_handle_open(FT_HANDLE t, const char *fname_in_env, int is_create, int only_create, CACHETABLE cachetable, TOKUTXN txn) {
    int r;
    r = ft_handle_open(t, fname_in_env, is_create, only_create, cachetable, txn, FILENUM_NONE, DICTIONARY_ID_NONE, MAX_LSN);
    return r;
}

// clone an ft handle. the cloned handle has a new dict_id but refers to the same fractal tree
int
toku_ft_handle_clone(FT_HANDLE *cloned_ft_handle, FT_HANDLE ft_handle, TOKUTXN txn) {
    FT_HANDLE result_ft_handle;
    toku_ft_handle_create(&result_ft_handle);

    // we're cloning, so the handle better have an open ft and open cf
    invariant(ft_handle->ft);
    invariant(ft_handle->ft->cf);

    // inherit the options of the ft whose handle is being cloned.
    toku_ft_handle_inherit_options(result_ft_handle, ft_handle->ft);

    // we can clone the handle by creating a new handle with the same fname
    CACHEFILE cf = ft_handle->ft->cf;
    CACHETABLE ct = toku_cachefile_get_cachetable(cf);
    const char *fname_in_env = toku_cachefile_fname_in_env(cf);
    int r = toku_ft_handle_open(result_ft_handle, fname_in_env, false, false, ct, txn);
    if (r != 0) {
        toku_ft_handle_close(result_ft_handle);
        result_ft_handle = NULL;
    }
    *cloned_ft_handle = result_ft_handle;
    return r;
}

// Open a brt in normal use.  The FILENUM and dict_id are assigned by the ft_handle_open() function.
int
toku_ft_handle_open_with_dict_id(
    FT_HANDLE t,
    const char *fname_in_env,
    int is_create,
    int only_create,
    CACHETABLE cachetable,
    TOKUTXN txn,
    DICTIONARY_ID use_dictionary_id
    )
{
    int r;
    r = ft_handle_open(
        t,
        fname_in_env,
        is_create,
        only_create,
        cachetable,
        txn,
        FILENUM_NONE,
        use_dictionary_id,
        MAX_LSN
        );
    return r;
}

DICTIONARY_ID
toku_ft_get_dictionary_id(FT_HANDLE brt) {
    FT h = brt->ft;
    DICTIONARY_ID dict_id = h->dict_id;
    return dict_id;
}

void toku_ft_set_flags(FT_HANDLE ft_handle, unsigned int flags) {
    ft_handle->did_set_flags = true;
    ft_handle->options.flags = flags;
}

void toku_ft_get_flags(FT_HANDLE ft_handle, unsigned int *flags) {
    *flags = ft_handle->options.flags;
}

void toku_ft_get_maximum_advised_key_value_lengths (unsigned int *max_key_len, unsigned int *max_val_len)
// return the maximum advisable key value lengths.  The brt doesn't enforce these.
{
    *max_key_len = 32*1024;
    *max_val_len = 32*1024*1024;
}


void toku_ft_handle_set_nodesize(FT_HANDLE ft_handle, unsigned int nodesize) {
    if (ft_handle->ft) {
        toku_ft_set_nodesize(ft_handle->ft, nodesize);
    }
    else {
        ft_handle->options.nodesize = nodesize;
    }
}

void toku_ft_handle_get_nodesize(FT_HANDLE ft_handle, unsigned int *nodesize) {
    if (ft_handle->ft) {
        toku_ft_get_nodesize(ft_handle->ft, nodesize);
    }
    else {
        *nodesize = ft_handle->options.nodesize;
    }
}

void toku_ft_handle_set_basementnodesize(FT_HANDLE ft_handle, unsigned int basementnodesize) {
    if (ft_handle->ft) {
        toku_ft_set_basementnodesize(ft_handle->ft, basementnodesize);
    }
    else {
        ft_handle->options.basementnodesize = basementnodesize;
    }
}

void toku_ft_handle_get_basementnodesize(FT_HANDLE ft_handle, unsigned int *basementnodesize) {
    if (ft_handle->ft) {
        toku_ft_get_basementnodesize(ft_handle->ft, basementnodesize);
    }
    else {
        *basementnodesize = ft_handle->options.basementnodesize;
    }
}

void toku_ft_set_key_ops(FT_HANDLE brt, struct toku_db_key_operations *key_ops)
{
    memcpy(&brt->options.key_ops, key_ops, sizeof(*key_ops));
}

void toku_ft_set_redirect_callback(FT_HANDLE brt, on_redirect_callback redir_cb, void* extra) {
    brt->redirect_callback = redir_cb;
    brt->redirect_callback_extra = extra;
}

void toku_ft_set_update(FT_HANDLE brt, ft_update_func update_fun) {
    brt->options.update_fun = update_fun;
}

ft_compare_func toku_ft_get_bt_compare (FT_HANDLE brt) {
    return brt->options.key_ops.keycmp;
}

static void
ft_remove_handle_ref_callback(FT UU(ft), void *extra) {
    FT_HANDLE CAST_FROM_VOIDP(handle, extra);
    toku_list_remove(&handle->live_ft_handle_link);
}

// close an ft handle during normal operation. the underlying ft may or may not close,
// depending if there are still references. an lsn for this close will come from the logger.
void
toku_ft_handle_close(FT_HANDLE ft_handle) {
    // There are error paths in the ft_handle_open that end with ft_handle->ft==NULL.
    FT ft = ft_handle->ft;
    if (ft) {
        const bool oplsn_valid = false;
        toku_ft_remove_reference(ft, oplsn_valid, ZERO_LSN, ft_remove_handle_ref_callback, ft_handle);
    }
    toku_free(ft_handle);
}

// close an ft handle during recovery. the underlying ft must close, and will use the given lsn.
void
toku_ft_handle_close_recovery(FT_HANDLE ft_handle, LSN oplsn) {
    FT ft = ft_handle->ft;
    // the ft must exist if closing during recovery. error paths during
    // open for recovery should close handles using toku_ft_handle_close()
    assert(ft);
    const bool oplsn_valid = true;
    toku_ft_remove_reference(ft, oplsn_valid, oplsn, ft_remove_handle_ref_callback, ft_handle);
    toku_free(ft_handle);
}

// TODO: remove this, callers should instead just use toku_ft_handle_close()
int
toku_close_ft_handle_nolsn (FT_HANDLE ft_handle, char** UU(error_string)) {
    toku_ft_handle_close(ft_handle);
    return 0;
}

void toku_ft_handle_create(FT_HANDLE *ft_handle_ptr) {
    FT_HANDLE XMALLOC(brt);
    memset(brt, 0, sizeof *brt);
    toku_list_init(&brt->live_ft_handle_link);
    brt->options.flags = 0;
    brt->did_set_flags = false;
    brt->options.nodesize = FT_DEFAULT_NODE_SIZE;
    brt->options.basementnodesize = FT_DEFAULT_BASEMENT_NODE_SIZE;
    brt->options.compression_method = TOKU_DEFAULT_COMPRESSION_METHOD;
    memcpy(&brt->options.key_ops, &toku_builtin_key_ops, sizeof(toku_builtin_key_ops));
    brt->options.update_fun = NULL;
    *ft_handle_ptr = brt;
}

/* ************* CURSORS ********************* */

static inline void
ft_cursor_cleanup_dbts(FT_CURSOR c) {
    toku_destroy_dbt(&c->key);
    toku_destroy_dbt(&c->val);
}

//
// This function is used by the leafentry iterators.
// returns TOKUDB_ACCEPT if live transaction context is allowed to read a value
// that is written by transaction with LSN of id
// live transaction context may read value if either id is the root ancestor of context, or if
// id was committed before context's snapshot was taken.
// For id to be committed before context's snapshot was taken, the following must be true:
//  - id < context->snapshot_txnid64 AND id is not in context's live root transaction list
// For the above to NOT be true:
//  - id > context->snapshot_txnid64 OR id is in context's live root transaction list
//
static int
does_txn_read_entry(TXNID id, TOKUTXN context) {
    int rval;
    TXNID oldest_live_in_snapshot = toku_get_oldest_in_live_root_txn_list(context);
    if (oldest_live_in_snapshot == TXNID_NONE && id < context->snapshot_txnid64) {
        rval = TOKUDB_ACCEPT;
    }
    else if (id < oldest_live_in_snapshot || id == context->txnid.parent_id64) {
        rval = TOKUDB_ACCEPT;
    }
    else if (id > context->snapshot_txnid64 || toku_is_txn_in_live_root_txn_list(*context->live_root_txn_list, id)) {
        rval = 0;
    }
    else {
        rval = TOKUDB_ACCEPT;
    }
    return rval;
}

static inline void
ft_cursor_extract_val(LEAFENTRY le,
                               FT_CURSOR cursor,
                               uint32_t *vallen,
                               void            **val) {
    if (toku_ft_cursor_is_leaf_mode(cursor)) {
        *val = le;
        *vallen = leafentry_memsize(le);
    } else if (cursor->is_snapshot_read) {
        int r = le_iterate_val(
            le,
            does_txn_read_entry,
            val,
            vallen,
            cursor->ttxn
            );
        lazy_assert_zero(r);
    } else {
        *val = le_latest_val_and_len(le, vallen);
    }
}

int toku_ft_cursor (
    FT_HANDLE brt,
    FT_CURSOR *cursorptr,
    TOKUTXN ttxn,
    bool is_snapshot_read,
    bool disable_prefetching
    )
{
    if (is_snapshot_read) {
        invariant(ttxn != NULL);
        int accepted = does_txn_read_entry(brt->ft->h->root_xid_that_created, ttxn);
        if (accepted!=TOKUDB_ACCEPT) {
            invariant(accepted==0);
            return TOKUDB_MVCC_DICTIONARY_TOO_NEW;
        }
    }
    FT_CURSOR XCALLOC(cursor);
    cursor->ft_handle = brt;
    cursor->prefetching = false;
    toku_init_dbt(&cursor->range_lock_left_key);
    toku_init_dbt(&cursor->range_lock_right_key);
    cursor->left_is_neg_infty = false;
    cursor->right_is_pos_infty = false;
    cursor->is_snapshot_read = is_snapshot_read;
    cursor->is_leaf_mode = false;
    cursor->ttxn = ttxn;
    cursor->disable_prefetching = disable_prefetching;
    cursor->is_temporary = false;
    cursor->is_seqread = false;
    *cursorptr = cursor;
    return 0;
}

void toku_ft_cursor_remove_restriction(FT_CURSOR ftcursor) {
    ftcursor->out_of_range_error = 0;
    ftcursor->direction = 0;
}

void
toku_ft_cursor_set_temporary(FT_CURSOR ftcursor) {
    ftcursor->is_temporary = true;
}

void
toku_ft_cursor_set_seqread(FT_CURSOR ftcursor) {
    ftcursor->is_seqread = true;
}

void
toku_ft_cursor_set_leaf_mode(FT_CURSOR ftcursor) {
    ftcursor->is_leaf_mode = true;
}

int
toku_ft_cursor_is_leaf_mode(FT_CURSOR ftcursor) {
    return ftcursor->is_leaf_mode;
}

void
toku_ft_cursor_set_range_lock(FT_CURSOR cursor, const DBT *left, const DBT *right,
                              bool left_is_neg_infty, bool right_is_pos_infty,
                              int out_of_range_error)
{
    // Destroy any existing keys and then clone the given left, right keys
    toku_destroy_dbt(&cursor->range_lock_left_key);
    if (left_is_neg_infty) {
        cursor->left_is_neg_infty = true;
    } else {
        toku_clone_dbt(&cursor->range_lock_left_key, *left);
    }

    toku_destroy_dbt(&cursor->range_lock_right_key);
    if (right_is_pos_infty) {
        cursor->right_is_pos_infty = true;
    } else {
        toku_clone_dbt(&cursor->range_lock_right_key, *right);
    }

    // TOKUDB_FOUND_BUT_REJECTED is a DB_NOTFOUND with instructions to stop looking. (Faster)
    cursor->out_of_range_error = out_of_range_error == DB_NOTFOUND ? TOKUDB_FOUND_BUT_REJECTED : out_of_range_error;
    cursor->direction = 0;
}

void toku_ft_cursor_close(FT_CURSOR cursor) {
    ft_cursor_cleanup_dbts(cursor);
    toku_destroy_dbt(&cursor->range_lock_left_key);
    toku_destroy_dbt(&cursor->range_lock_right_key);
    toku_free(cursor);
}

static inline void ft_cursor_set_prefetching(FT_CURSOR cursor) {
    cursor->prefetching = true;
}

static inline bool ft_cursor_prefetching(FT_CURSOR cursor) {
    return cursor->prefetching;
}

//Return true if cursor is uninitialized.  false otherwise.
static bool
ft_cursor_not_set(FT_CURSOR cursor) {
    assert((cursor->key.data==NULL) == (cursor->val.data==NULL));
    return (bool)(cursor->key.data == NULL);
}

static int
heaviside_from_search_t(const DBT &kdbt, ft_search_t &search)
{
    int cmp = search.compare(search, search.user_key ? &kdbt : 0);
    // The search->compare function returns only 0 or 1
    switch (search.direction) {
    case FT_SEARCH_LEFT:   return cmp==0 ? -1 : +1;
    case FT_SEARCH_RIGHT:  return cmp==0 ? +1 : -1; // Because the comparison runs backwards for right searches.
    }
    abort(); return 0;
}


//
// Returns true if the value that is to be read is empty.
//
static inline int
is_le_val_del(LEAFENTRY le, FT_CURSOR ftcursor) {
    int rval;
    if (ftcursor->is_snapshot_read) {
        bool is_del;
        le_iterate_is_del(
            le,
            does_txn_read_entry,
            &is_del,
            ftcursor->ttxn
            );
        rval = is_del;
    }
    else {
        rval = le_latest_is_del(le);
    }
    return rval;
}

// reverse order of ancestors, so we can do lift and relift
struct ancs_list {
    ANCESTORS ancs;
    struct ancs_list *next;
};

/**
 * Given a fifo_entry, either decompose it into its parameters and call
 * toku_ft_bn_apply_cmd, or discard it, based on its MSN and the MSN of the
 * basement node.
 */
static void
do_bn_apply_cmd(FT_HANDLE t, BASEMENTNODE bn, struct fifo_entry *entry,
                const struct pivot_bounds *const bounds,
                struct ancs_list *ancs_list,
                TXNID oldest_referenced_xid, uint64_t *workdone,
                STAT64INFO stats_to_update)
{
    // The messages are being iterated over in (key,msn) order or just in
    // msn order, so all the messages for one key, from one buffer, are in
    // ascending msn order.  So it's ok that we don't update the basement
    // node's msn until the end.
    // don't apply msgs that are older than spvt msn
    if (entry->msn.msn > bn->max_msn_applied.msn ||
            (ancs_list->ancs->is_goto && ancs_list->ancs->goto_msn.msn >= entry->msn.msn)) {
        FT_MSG_S ftcmd;
        DBT k, v, m, alloc_k, alloc_m;
        NONLEAF_CHILDINFO bnc;
        toku_init_dbt(&alloc_k);
        toku_init_dbt(&alloc_m);
        fifo_entry_get_msg(&ftcmd, entry, &k, &v, &m);

        if (ft_msg_applies_multiple(&ftcmd)) {
            assert(bounds != NULL);
            FAKE_DB(db, &t->ft->cmp_descriptor);
            if (bounds->lk && !is_dbt_empty(ftcmd.max_key)) {
                if (t->ft->key_ops.keycmp(&db, ftcmd.max_key, bounds->lk) <= 0) {
                    // this multicase message has nothing to do with us
                    // as entry is tmp when calling, we dont even need to mark
                    return;
                } else if (t->ft->key_ops.keycmp(&db, ftcmd.key, bounds->lk) < 0) {
                    toku_copy_dbt(&k, *bounds->lk);
                }
            }
            if (bounds->uk && !is_dbt_empty(ftcmd.max_key)) {
                if (t->ft->key_ops.keycmp(&db, ftcmd.key, bounds->uk) >= 0) {
                    // this multicase message has nothing to do with us
                    // as entry is tmp when calling, we dont even need to mark
                    return;
                } else if (t->ft->key_ops.keycmp(&db, ftcmd.max_key, bounds->uk) > 0) {
                    toku_copy_dbt(&m, *bounds->uk);
                }
            }
        }

        for (struct ancs_list *it = ancs_list; it != NULL; it = it->next) {
            bnc = BNC(it->ancs->node, it->ancs->childnum);
            if (it != ancs_list) {
                if (!is_dbt_empty(ftcmd.key)) {
                    ft_process_key(t->ft, &BP_LIFT(it->ancs->node, it->ancs->childnum),
                                   &BP_NOT_LIFTED(it->ancs->node, it->ancs->childnum),
                                   &k, &alloc_k);
                }
                if (!is_dbt_empty(ftcmd.max_key)) {
                    ft_process_key(t->ft, &BP_LIFT(it->ancs->node, it->ancs->childnum),
                                   &BP_NOT_LIFTED(it->ancs->node, it->ancs->childnum),
                                   &m, &alloc_m);
                }
            }
            if (it->ancs->is_goto) {
                if (!is_dbt_empty(ftcmd.key)) {
                    ft_process_key(t->ft, &it->ancs->goto_lift, &it->ancs->goto_not_lifted,
                                   &k, &alloc_k);
                }
                if (!is_dbt_empty(ftcmd.max_key)) {
                    ft_process_key(t->ft, &it->ancs->goto_lift, &it->ancs->goto_not_lifted,
                                   &m, &alloc_m);
                }
            }
        }

        toku_ft_bn_apply_cmd(t->ft, &t->ft->cmp_descriptor, NULL,
                             bn, &ftcmd, oldest_referenced_xid,
                             make_gc_info(true), //mvcc is needed
                             workdone, stats_to_update);

        toku_cleanup_dbt(&alloc_k);
        toku_cleanup_dbt(&alloc_m);
    }
    // We must always mark entry as stale since it has been marked
    // (using omt::iterate_and_mark_range)
    // It is possible to call do_bn_apply_cmd even when it won't apply the message because
    // the node containing it could have been evicted and brought back in.
    entry->is_fresh = false;
}

struct iterate_do_bn_apply_cmd_extra {
    FT_HANDLE t;
    BASEMENTNODE bn;
    NONLEAF_CHILDINFO bnc;
    struct ancs_list *ancs_list;
    TXNID oldest_referenced_xid;
    uint64_t *workdone;
    STAT64INFO stats_to_update;
};

int iterate_do_bn_apply_cmd(const int32_t &offset, const uint32_t UU(idx),
                            struct iterate_do_bn_apply_cmd_extra *const e)
                           __attribute__((nonnull(3)));
int iterate_do_bn_apply_cmd(const int32_t &offset, const uint32_t UU(idx),
                            struct iterate_do_bn_apply_cmd_extra *const e)
{
    struct fifo_entry *entry = toku_fifo_get_entry(e->bnc->buffer, offset);

    do_bn_apply_cmd(e->t, e->bn, entry, NULL, e->ancs_list,
                    e->oldest_referenced_xid, e->workdone, e->stats_to_update);
    return 0;
}

/**
 * Given the bounds of the basement node to which we will apply messages,
 * find the indexes within message_tree which contain the range of
 * relevant messages.
 *
 * The message tree contains offsets into the buffer, where messages are
 * found.  The pivot_bounds are the lower bound exclusive and upper bound
 * inclusive, because they come from pivot keys in the tree.  We want OMT
 * indices, which must have the lower bound be inclusive and the upper
 * bound exclusive.  We will get these by telling toku_omt_find to look
 * for something strictly bigger than each of our pivot bounds.
 *
 * Outputs the OMT indices in lbi (lower bound inclusive) and ube (upper
 * bound exclusive).
 */
template<typename find_bounds_omt_t>
static void
find_bounds_within_message_tree(
    FT ft,
    const find_bounds_omt_t &message_tree,      /// tree holding FIFO offsets, in which we want to look for indices
    FIFO buffer,           /// buffer in which messages are found
    struct pivot_bounds const * const bounds,  /// key bounds within the basement node we're applying messages to
    uint32_t *lbi,        /// (output) "lower bound inclusive" (index into message_tree)
    uint32_t *ube         /// (output) "upper bound exclusive" (index into message_tree)
    )
{
    int r = 0;
    DESCRIPTOR desc = &ft->cmp_descriptor;
    ft_compare_func cmp = ft->key_ops.keycmp;
    if (bounds->lk) {
        // By setting msn to MAX_MSN and by using direction of +1, we will
        // get the first message greater than (in (key, msn) order) any
        // message (with any msn) with the key lower_bound_exclusive.
        // This will be a message we want to try applying, so it is the
        // "lower bound inclusive" within the message_tree.
        struct toku_fifo_entry_key_msn_heaviside_extra lbi_extra;
        ZERO_STRUCT(lbi_extra);
        lbi_extra.desc = desc;
        lbi_extra.cmp = cmp;
        lbi_extra.fifo = buffer;
        lbi_extra.key = bounds->lk;
        lbi_extra.msn = MAX_MSN;
        int32_t found_lb;

        r = message_tree.template find<struct toku_fifo_entry_key_msn_heaviside_extra, toku_fifo_entry_key_msn_heaviside>(lbi_extra, +1, &found_lb, lbi);
        if (r == DB_NOTFOUND) {
            // There is no relevant data (the lower bound is bigger than
            // any message in this tree), so we have no range and we're
            // done.
            *lbi = 0;
            *ube = 0;
            return;
        }
        if (bounds->uk) {
            // Check if what we found for lbi is greater than the upper
            // bound inclusive that we have.  If so, there are no relevant
            // messages between these bounds.
            const DBT *ubi = bounds->uk;
            const int32_t offset = found_lb;
            DBT found_lbidbt;
            const struct fifo_entry *query = toku_fifo_get_entry(buffer, offset);
            fill_dbt_for_fifo_entry(&found_lbidbt, query);

            FAKE_DB(db, desc);
            DBT clone_found_lbidbt;
            toku_memdup_dbt(&clone_found_lbidbt, found_lbidbt.data, found_lbidbt.size);
            int c = cmp(&db, &clone_found_lbidbt, ubi);
            toku_destroy_dbt(&clone_found_lbidbt);
            // These DBTs really are both inclusive bounds, so we need
            // strict inequality in order to determine that there's
            // nothing between them.  If they're equal, then we actually
            // need to apply the message pointed to by lbi, and also
            // anything with the same key but a bigger msn.
            if (c > 0) {
                *lbi = 0;
                *ube = 0;
                return;
            }
        }
    } else {
        // No lower bound given, it's negative infinity, so we start at
        // the first message in the OMT.
        *lbi = 0;
    }
    if (bounds->uk) {
        // Again, we use an msn of MAX_MSN and a direction of +1 to get
        // the first thing bigger than the upper_bound_inclusive key.
        // This is therefore the smallest thing we don't want to apply,
        // and toku_omt_iterate_on_range will not examine it.
        struct toku_fifo_entry_key_msn_heaviside_extra ube_extra;
        ZERO_STRUCT(ube_extra);
        ube_extra.desc = desc;
        ube_extra.cmp = cmp;
        ube_extra.fifo = buffer;
        ube_extra.key = bounds->uk;
        ube_extra.msn = MAX_MSN;
        r = message_tree.template find<struct toku_fifo_entry_key_msn_heaviside_extra, toku_fifo_entry_key_msn_heaviside>(ube_extra, +1, nullptr, ube);
        if (r == DB_NOTFOUND) {
            // Couldn't find anything in the buffer bigger than our key,
            // so we need to look at everything up to the end of
            // message_tree.
            *ube = message_tree.size();
        }
    } else {
        // No upper bound given, it's positive infinity, so we need to go
        // through the end of the OMT.
        *ube = message_tree.size();
    }
}

/**
 * For each message in the ancestor's buffer (determined by childnum) that
 * is key-wise between lower_bound_exclusive and upper_bound_inclusive,
 * apply the message to the basement node.  We treat the bounds as minus
 * or plus infinity respectively if they are NULL.  Do not mark the node
 * as dirty (preserve previous state of 'dirty' bit).
 */
static void
bnc_apply_msgs_to_bn(
    // used for comparison function
    FT_HANDLE brt,
    // where to apply messages
    BASEMENTNODE bn,
    struct ancs_list *ancs_list,
    // contains pivot key bounds of this basement node
    struct pivot_bounds const * const bounds,
    // may be younger than what's in ancestor, we should grab the value from the highest node we have
    TXNID oldest_referenced_xid,
    bool *msgs_applied)
{
    int r;
    NONLEAF_CHILDINFO bnc;

    bnc = BNC(ancs_list->ancs->node, ancs_list->ancs->childnum);

    // Determine the offsets in the message trees between which we need to
    // apply messages from this buffer
    STAT64INFO_S stats_delta = {0, 0};
    uint64_t workdone_this_ancestor = 0;

    uint32_t stale_lbi, stale_ube;
    uint32_t fresh_lbi, fresh_ube;
    if (!bn->stale_ancestor_messages_applied) {
        find_bounds_within_message_tree(brt->ft,
                                        bnc->stale_message_tree, bnc->buffer,
                                        bounds, &stale_lbi, &stale_ube);
    } else {
        stale_lbi = 0;
        stale_ube = 0;
    }
    find_bounds_within_message_tree(brt->ft, bnc->fresh_message_tree,
                                    bnc->buffer, bounds,
                                    &fresh_lbi, &fresh_ube);

    // We now know where all the messages we must apply are, so one of the
    // following 4 cases will do the application, depending on which of
    // the lists contains relevant messages:
    //
    // 1. broadcast messages and anything else, or a mix of fresh and stale
    // 2. only fresh messages
    // 3. only stale messages
    if (bnc->broadcast_list.size() > 0 || bnc->goto_list.size() > 0 ||
            (stale_lbi != stale_ube && fresh_lbi != fresh_ube)) {
        // We have messages in multiple trees, so we grab all
        // the relevant messages' offsets and sort them by MSN, then apply
        // them in MSN order.
        const int buffer_size = ((stale_ube - stale_lbi) +
                                 (fresh_ube - fresh_lbi) +
                                 bnc->goto_list.size() +
                                 bnc->broadcast_list.size());

        int32_t *XMALLOC_N(buffer_size, offsets);
        struct store_fifo_offset_extra sfo_extra = {.offsets = offsets, .i = 0};

        // Populate offsets array with offsets to stale messages
        r = bnc->stale_message_tree.iterate_on_range
                    <struct store_fifo_offset_extra, store_fifo_offset>
                    (stale_lbi, stale_ube, &sfo_extra);
        assert_zero(r);

        // Then store fresh offsets, and mark them to be moved to stale later.
        r = bnc->fresh_message_tree.iterate_and_mark_range
                    <struct store_fifo_offset_extra, store_fifo_offset>
                    (fresh_lbi, fresh_ube, &sfo_extra);
        assert_zero(r);

        r = bnc->goto_list.iterate
                    <struct store_fifo_offset_extra, store_fifo_offset>
                    (&sfo_extra);
        assert_zero(r);

        // Store offsets of all broadcast messages.
        r = bnc->broadcast_list.iterate
                    <struct store_fifo_offset_extra, store_fifo_offset>
                    (&sfo_extra);
        assert_zero(r);

        invariant(sfo_extra.i == buffer_size);

        // Sort by MSN.
        r = toku::sort<int32_t, FIFO, fifo_offset_msn_cmp>::mergesort_r(offsets, buffer_size, bnc->buffer);
        assert_zero(r);

        // Apply the messages in MSN order.
        *msgs_applied = true;
        for (int i = 0; i < buffer_size; ++i) {
            struct fifo_entry *entry = toku_fifo_get_entry(bnc->buffer, offsets[i]);
            do_bn_apply_cmd(brt, bn, entry, bounds, ancs_list,
                            oldest_referenced_xid, &workdone_this_ancestor,
                            &stats_delta);
        }

        toku_free(offsets);
    } else if (fresh_lbi != fresh_ube) {
        // No stale messages to apply, we just apply fresh messages
        // and mark them to be moved to stale later.
        assert(fresh_lbi < fresh_ube);
        assert(stale_lbi == stale_ube);
        *msgs_applied = true;

        struct iterate_do_bn_apply_cmd_extra iter_extra = {
            .t = brt, .bn = bn, .bnc = bnc, .ancs_list = ancs_list,
            .oldest_referenced_xid = oldest_referenced_xid,
            .workdone = &workdone_this_ancestor,
            .stats_to_update = &stats_delta
        };
        r = bnc->fresh_message_tree.iterate_and_mark_range
                    <struct iterate_do_bn_apply_cmd_extra, iterate_do_bn_apply_cmd>
                    (fresh_lbi, fresh_ube, &iter_extra);
        assert_zero(r);
    } else if (stale_lbi != stale_ube) {
        // No fresh messages to apply, we just apply stale messages.
        assert(fresh_lbi == fresh_ube);
        assert(stale_lbi < stale_ube);
        *msgs_applied = true;

        struct iterate_do_bn_apply_cmd_extra iter_extra = {
            .t = brt, .bn = bn, .bnc = bnc, .ancs_list = ancs_list,
            .oldest_referenced_xid = oldest_referenced_xid,
            .workdone = &workdone_this_ancestor,
            .stats_to_update = &stats_delta
        };

        r = bnc->stale_message_tree.iterate_on_range
                    <struct iterate_do_bn_apply_cmd_extra, iterate_do_bn_apply_cmd>
                    (stale_lbi, stale_ube, &iter_extra);
        assert_zero(r);
    }

    // update stats
    if (workdone_this_ancestor > 0) {
        (void) toku_sync_fetch_and_add(&BP_WORKDONE(ancs_list->ancs->node, ancs_list->ancs->childnum), workdone_this_ancestor);
    }
    if (stats_delta.numbytes || stats_delta.numrows) {
        toku_ft_update_stats(&brt->ft->in_memory_stats, stats_delta);
    }
}

static inline int
compare_k_x(FT ft, const DBT *k, const DBT *x)
{
    FAKE_DB(db, &ft->cmp_descriptor);
    return ft->key_ops.keycmp(&db, k, x);
}

// set up the correct bounds for the BN (node->bp[cnum])
// pbounds is the range specified by parent, might be smaller than node bound
// bn_bounds is the output
static void
setup_bn_bounds(FT ft, FTNODE node, int cnum, struct pivot_bounds *pbounds,
                struct pivot_bounds *bn_bounds, DBT *lk, DBT *uk)
{
    if (pbounds == NULL) {
      bn_bounds->lk = NULL;
      bn_bounds->uk = NULL;
      return;
    }

    // lower bound
    if (cnum == 0 && !pbounds->lk) {
        bn_bounds->lk = NULL;
    } else {
        bn_bounds->lk = lk;
        if (cnum == 0) {
            // pbound range should always be smaller than node range
            toku_copy_dbt(lk, *pbounds->lk);
        } else if (!pbounds->lk) {
            // get from the node bounds, cnum != 0 (so not bound_l)
            toku_copy_dbt(lk, node->childkeys[cnum - 1]);
        } else if (compare_k_x(ft, pbounds->lk, &node->childkeys[cnum - 1]) > 0) {
            // now we get whichever is larger
            toku_copy_dbt(lk, *pbounds->lk);
        } else {
            toku_copy_dbt(lk, node->childkeys[cnum - 1]);
        }
    }

    // upper bound
    if (cnum == node->n_children - 1 && !pbounds->uk) {
        bn_bounds->uk = NULL;
    } else {
        bn_bounds->uk = uk;
        if (cnum == node->n_children - 1) {
            // pbound range should always be smaller than node range
            toku_copy_dbt(uk, *pbounds->uk);
        } else if (!pbounds->uk) {
            // get from the node bounds, cnum != node->nc - 1 (so not bound_r)
            toku_copy_dbt(uk, node->childkeys[cnum]);
        } else if (compare_k_x(ft, pbounds->uk, &node->childkeys[cnum]) > 0) {
            // now we get whichever is smaller
            toku_copy_dbt(uk, node->childkeys[cnum]);
        } else {
            toku_copy_dbt(uk, *pbounds->uk);
        }
    }

    if (bn_bounds->lk && bn_bounds->uk) {
        // this is not true for all BNs
        // but true for a BN that is being searched
        assert(compare_k_x(ft, bn_bounds->lk, bn_bounds->uk) < 0);
    }
}

// update the bounds if the ancs is from a GOTO
static inline void
update_bn_bounds(FT ft, DBT *lift, DBT *not_lifted,
                 struct pivot_bounds *bn_bounds, DBT *alloc_lk, DBT *alloc_uk)
{
    if (bn_bounds->lk) {
       ft_recover_key(ft, lift, not_lifted, bn_bounds->lk, alloc_lk);
    }
    if (bn_bounds->uk) {
        ft_recover_key(ft, lift, not_lifted, bn_bounds->uk, alloc_uk);
    }
}


static void
apply_ancestors_messages_to_bn(
    FT_HANDLE t, FTNODE node, int cnum, ANCESTORS ancestors,
    struct pivot_bounds *bounds, TXNID oldest_referenced_xid, bool *msgs_applied)
{
    BASEMENTNODE bn = BLB(node, cnum);
    DBT alloc_lk, alloc_uk, lk, uk;
    // this is the bound we use to search
    struct pivot_bounds cur_bounds;
    toku_init_dbt(&alloc_lk);
    toku_init_dbt(&alloc_uk);

    // bn bounds
    setup_bn_bounds(t->ft, node, cnum, bounds, &cur_bounds, &lk, &uk);

    // a ancs list (backward ancestors) so we can lift/unlift keys in BNCs
    struct ancs_list *ancs_list = NULL;
    for (ANCESTORS cur_ancs = ancestors; cur_ancs; cur_ancs = cur_ancs->next) {
        assert(BP_STATE(cur_ancs->node, cur_ancs->childnum) == PT_AVAIL);

        // if this ancestors is from a GOTO, handle lifting
        if (cur_ancs->is_goto) {
            update_bn_bounds(t->ft,
                             &cur_ancs->goto_lift, &cur_ancs->goto_not_lifted,
                             &cur_bounds, &alloc_lk, &alloc_uk);
        }

        {
            struct ancs_list *tmp_al = ancs_list;
            ancs_list = (struct ancs_list *)alloca(sizeof(*ancs_list));
            ancs_list->ancs = cur_ancs;
            ancs_list->next = tmp_al;
        }

        if (cur_ancs->node->max_msn_applied_to_node_on_disk.msn > bn->max_msn_applied.msn) {
            bnc_apply_msgs_to_bn(t, bn, ancs_list, &cur_bounds,
                                 oldest_referenced_xid, msgs_applied);
            // We don't want to check this ancestor node again if the
            // next time we query it, the msn hasn't changed.
            bn->max_msn_applied = cur_ancs->node->max_msn_applied_to_node_on_disk;
        }
        // handle BNC lifting
        update_bn_bounds(t->ft,
                         &BP_LIFT(cur_ancs->node, cur_ancs->childnum),
                         &BP_NOT_LIFTED(cur_ancs->node, cur_ancs->childnum),
                         &cur_bounds, &alloc_lk, &alloc_uk);
    }
    // At this point, we know all the stale messages above this
    // basement node have been applied, and any new messages will be
    // fresh, so we don't need to look at stale messages for this
    // basement node, unless it gets evicted (and this field becomes
    // false when it's read in again).
    bn->stale_ancestor_messages_applied = true;

    toku_cleanup_dbt(&alloc_lk);
    toku_cleanup_dbt(&alloc_uk);
}

void
toku_apply_ancestors_messages_to_node (
    FT_HANDLE t, FTNODE node, ANCESTORS ancestors,
    struct pivot_bounds *bounds, bool *msgs_applied, int child_to_read)
// Effect:
//   Bring a leaf node up-to-date according to all the messages in the ancestors.
//   If the leaf node is already up-to-date then do nothing.
//   If the leaf node is not already up-to-date, then record the work done
//   for that leaf in each ancestor.
// Requires:
//   This is being called when pinning a leaf node for the query path.
//   The entire root-to-leaf path is pinned and appears in the ancestors list.
{
    VERIFY_NODE(t, node);
    assert_zero(node->height);
    TXNID oldest_referenced_xid = ancestors->node->oldest_referenced_xid_known;

    for (ANCESTORS cur_ancs = ancestors; cur_ancs; cur_ancs = cur_ancs->next) {
        if (cur_ancs->node->oldest_referenced_xid_known > oldest_referenced_xid) {
            oldest_referenced_xid = cur_ancs->node->oldest_referenced_xid_known;
        }
    }

    if (!node->dirty && child_to_read >= 0) {
        assert(BP_STATE(node, child_to_read) == PT_AVAIL);
        apply_ancestors_messages_to_bn(
            t, node, child_to_read, ancestors, bounds,
            oldest_referenced_xid, msgs_applied);
    } else {
        // know we are a leaf node
        // An important invariant:
        // We MUST bring every available basement node for a dirty node up to date.
        // flushing on the cleaner thread depends on this. This invariant
        // allows the cleaner thread to just pick an internal node and flush it
        // as opposed to being forced to start from the root.
        for (int i = 0; i < node->n_children; i++) {
            if (BP_STATE(node, i) != PT_AVAIL) {
                continue;
            }
            apply_ancestors_messages_to_bn(
                t, node, i, ancestors, bounds,
                oldest_referenced_xid, msgs_applied);
        }
    }

    VERIFY_NODE(t, node);
}

static bool
bn_needs_ancestors_messages(
    FT ft, FTNODE node, int cnum, ANCESTORS ancestors,
    struct pivot_bounds *bounds, MSN *max_msn_applied)
{
    BASEMENTNODE bn = BLB(node, cnum);
    DBT alloc_lk, alloc_uk, lk, uk;
    // this is the bound we use to search
    struct pivot_bounds cur_bounds;
    toku_init_dbt(&alloc_lk);
    toku_init_dbt(&alloc_uk);
    toku_init_dbt(&lk);
    toku_init_dbt(&uk);

    // bn bounds
    setup_bn_bounds(ft, node, cnum, bounds, &cur_bounds, &lk, &uk);

    bool needs_ancestors_messages = false;
    for (ANCESTORS cur_ancs = ancestors; cur_ancs; cur_ancs = cur_ancs->next) {
        assert(BP_STATE(cur_ancs->node, cur_ancs->childnum) == PT_AVAIL);
        NONLEAF_CHILDINFO bnc = BNC(cur_ancs->node, cur_ancs->childnum);

        // if this ancestors is from a GOTO, handle lifting
        if (cur_ancs->is_goto) {
            update_bn_bounds(ft,
                             &cur_ancs->goto_lift, &cur_ancs->goto_not_lifted,
                             &cur_bounds, &alloc_lk, &alloc_uk);
        }

        if (cur_ancs->node->max_msn_applied_to_node_on_disk.msn > bn->max_msn_applied.msn) {
            if (bnc->broadcast_list.size() > 0 || bnc->goto_list.size()) {
                needs_ancestors_messages = true;
                goto cleanup;
            }
            if (!bn->stale_ancestor_messages_applied) {
                uint32_t stale_lbi, stale_ube;
                find_bounds_within_message_tree(ft,
                                                bnc->stale_message_tree,
                                                bnc->buffer,
                                                &cur_bounds,
                                                &stale_lbi,
                                                &stale_ube);
                if (stale_lbi < stale_ube) {
                    needs_ancestors_messages = true;
                    goto cleanup;
                }
            }
            uint32_t fresh_lbi, fresh_ube;
            find_bounds_within_message_tree(ft,
                                            bnc->fresh_message_tree,
                                            bnc->buffer,
                                            &cur_bounds,
                                            &fresh_lbi,
                                            &fresh_ube);
            if (fresh_lbi < fresh_ube) {
                needs_ancestors_messages = true;
                goto cleanup;
            }
            if (cur_ancs->node->max_msn_applied_to_node_on_disk.msn > max_msn_applied->msn) {
                max_msn_applied->msn = cur_ancs->node->max_msn_applied_to_node_on_disk.msn;
            }
        }
        // handle BNC lifting
        update_bn_bounds(ft,
                         &BP_LIFT(cur_ancs->node, cur_ancs->childnum),
                         &BP_NOT_LIFTED(cur_ancs->node, cur_ancs->childnum),
                         &cur_bounds, &alloc_lk, &alloc_uk);
    }
cleanup:
    toku_cleanup_dbt(&alloc_lk);
    toku_cleanup_dbt(&alloc_uk);

    return needs_ancestors_messages;
}

bool
toku_ft_leaf_needs_ancestors_messages(
    FT ft, FTNODE node, ANCESTORS ancestors, struct pivot_bounds *bounds,
    MSN *const max_msn_in_path, int child_to_read)
// Effect: Determine whether there are messages in a node's ancestors
//  which must be applied to it.  These messages are in the correct
//  keyrange for any available basement nodes, and are in nodes with the
//  correct max_msn_applied_to_node_on_disk.
// Notes:
//  This is an approximate query.
// Output:
//  max_msn_in_path: max of "max_msn_applied_to_node_on_disk" over
//    ancestors.  This is used later to update basement nodes'
//    max_msn_applied values in case we don't do the full algorithm.
// Returns:
//  true if there may be some such messages
//  false only if there are definitely no such messages
// Rationale:
//  When we pin a node with a read lock, we want to quickly determine if
//  we should exchange it for a write lock in preparation for applying
//  messages.  If there are no messages, we don't need the write lock.
{
    assert_zero(node->height);
    bool needs_ancestors_messages = false;
    // child_to_read may be -1 in test cases
    if (!node->dirty && child_to_read >= 0) {
        assert(BP_STATE(node, child_to_read) == PT_AVAIL);
        needs_ancestors_messages =
            bn_needs_ancestors_messages(ft, node, child_to_read, ancestors,
                                        bounds, max_msn_in_path);
    } else {
        for (int i = 0; i < node->n_children; ++i) {
            if (BP_STATE(node, i) == PT_AVAIL) {
                needs_ancestors_messages =
                    bn_needs_ancestors_messages(ft, node, i, ancestors,
                                                bounds, max_msn_in_path);
                if (needs_ancestors_messages) {
                    break;
                }
            }
        }
    }

    return needs_ancestors_messages;
}

void toku_ft_bn_update_max_msn(FTNODE node, MSN max_msn_applied, int child_to_read) {
    invariant(node->height == 0);
    if (!node->dirty && child_to_read >= 0) {
        paranoid_invariant(BP_STATE(node, child_to_read) == PT_AVAIL);
        BASEMENTNODE bn = BLB(node, child_to_read);
        if (max_msn_applied.msn > bn->max_msn_applied.msn) {
            // see comment below
            (void) toku_sync_val_compare_and_swap(&bn->max_msn_applied.msn, bn->max_msn_applied.msn, max_msn_applied.msn);
        }
    }
    else {
        for (int i = 0; i < node->n_children; ++i) {
            if (BP_STATE(node, i) != PT_AVAIL) { continue; }
            BASEMENTNODE bn = BLB(node, i);
            if (max_msn_applied.msn > bn->max_msn_applied.msn) {
                // This function runs in a shared access context, so to silence tools
                // like DRD, we use a CAS and ignore the result.
                // Any threads trying to update these basement nodes should be
                // updating them to the same thing (since they all have a read lock on
                // the same root-to-leaf path) so this is safe.
                (void) toku_sync_val_compare_and_swap(&bn->max_msn_applied.msn, bn->max_msn_applied.msn, max_msn_applied.msn);
            }
        }
    }
}

struct copy_to_stale_extra {
    FT ft;
    NONLEAF_CHILDINFO bnc;
};

int copy_to_stale(const int32_t &offset, const uint32_t UU(idx), struct copy_to_stale_extra *const extra) __attribute__((nonnull(3)));
int copy_to_stale(const int32_t &offset, const uint32_t UU(idx), struct copy_to_stale_extra *const extra)
{
    struct fifo_entry *entry = toku_fifo_get_entry(extra->bnc->buffer, offset);
    DBT keydbt;
    DBT *key = fill_dbt_for_fifo_entry(&keydbt, entry);
    struct toku_fifo_entry_key_msn_heaviside_extra heaviside_extra = { .desc = &extra->ft->cmp_descriptor, .cmp = extra->ft->key_ops.keycmp, .fifo = extra->bnc->buffer, .key = key, .msn = entry->msn };
    int r = extra->bnc->stale_message_tree.insert<struct toku_fifo_entry_key_msn_heaviside_extra, toku_fifo_entry_key_msn_heaviside>(offset, heaviside_extra, nullptr);
    invariant_zero(r);
    return 0;
}

__attribute__((nonnull))
void
toku_move_ftnode_messages_to_stale(FT ft, FTNODE node) {
    invariant(node->height > 0);
    for (int i = 0; i < node->n_children; ++i) {
        if (BP_STATE(node, i) != PT_AVAIL) {
            continue;
        }
        NONLEAF_CHILDINFO bnc = BNC(node, i);
        // We can't delete things out of the fresh tree inside the above
        // procedures because we're still looking at the fresh tree.  Instead
        // we have to move messages after we're done looking at it.
        struct copy_to_stale_extra cts_extra = { .ft = ft, .bnc = bnc };
        int r = bnc->fresh_message_tree.iterate_over_marked<struct copy_to_stale_extra, copy_to_stale>(&cts_extra);
        invariant_zero(r);
        bnc->fresh_message_tree.delete_all_marked();
    }
}

static int cursor_check_restricted_range(FT_CURSOR c, bytevec key, ITEMLEN keylen) {
    if (c->out_of_range_error) {
        FT ft = c->ft_handle->ft;
        FAKE_DB(db, &ft->cmp_descriptor);
        DBT found_key;
        toku_fill_dbt(&found_key, key, keylen);
        if ((!c->left_is_neg_infty && c->direction <= 0 && ft->key_ops.keycmp(&db, &found_key, &c->range_lock_left_key) < 0) ||
            (!c->right_is_pos_infty && c->direction >= 0 && ft->key_ops.keycmp(&db, &found_key, &c->range_lock_right_key) > 0)) {
            invariant(c->out_of_range_error);
            return c->out_of_range_error;
        }
    }
    // Reset cursor direction to mitigate risk if some query type doesn't set the direction.
    // It is always correct to check both bounds (which happens when direction==0) but it can be slower.
    c->direction = 0;
    return 0;
}

void
toku_ft_recover_key_by_ancs(FT ft, ANCESTORS ancs, DBT *key, DBT *alloc_key)
{
    for (ANCESTORS cur_ancs = ancs; cur_ancs; cur_ancs = cur_ancs->next) {
        if (cur_ancs->is_goto) {
            ft_recover_key(ft, &cur_ancs->goto_lift, &cur_ancs->goto_not_lifted,
                           key, alloc_key);
        }

        ft_recover_key(ft, &BP_LIFT(cur_ancs->node, cur_ancs->childnum),
                       &BP_NOT_LIFTED(cur_ancs->node, cur_ancs->childnum),
                       key, alloc_key);
    }
}

static int
ft_cursor_shortcut (
    FT ft,
    FT_CURSOR cursor,
    ANCESTORS ancestors,
    struct pivot_bounds *bounds,
    int direction,
    uint32_t index,
    bn_data* bd,
    FT_GET_CALLBACK_FUNCTION getf,
    void *getf_v,
    uint32_t *keylen,
    void **key,
    DBT *alloc,
    uint32_t *vallen,
    void **val
    )
{
    int r = 0;
    // if we are searching towards the end, limit is last element
    // if we are searching towards the beginning, limit is the first element
    uint32_t limit = (direction > 0) ? (bd->omt_size() - 1) : 0;

    //Starting with the prev, find the first real (non-provdel) leafentry.
    while (index != limit) {
        index += direction;
        LEAFENTRY le;
        void* foundkey = NULL;
        uint32_t foundkeylen = 0;

        r = bd->fetch_klpair(index, &le, &foundkeylen, &foundkey);
        invariant_zero(r);
        {
            DBT tmp;
            toku_fill_dbt(&tmp, foundkey, foundkeylen);
            if (direction == 1) {
                if (bounds->uk && compare_k_x(ft, bounds->uk, &tmp) < 0) {
                    return TOKUDB_CURSOR_CONTINUE;
                }
            } else {
                assert(direction == -1);
                if (bounds->lk && compare_k_x(ft, bounds->lk, &tmp) >= 0) {
                    return TOKUDB_CURSOR_CONTINUE;
                }
            }
        }
        if (toku_ft_cursor_is_leaf_mode(cursor) || !is_le_val_del(le, cursor)) {
            ft_cursor_extract_val(
                le,
                cursor,
                vallen,
                val
                );
            // key is from buffer, but unlifted_key is alloc'ed by us
            toku_cleanup_dbt(alloc);
            {
                DBT key_dbt;
                toku_fill_dbt(&key_dbt, foundkey, foundkeylen);
                toku_ft_recover_key_by_ancs(ft, ancestors, &key_dbt, alloc);
                *key = key_dbt.data;
                *keylen = key_dbt.size;
            }

            cursor->direction = direction;
            r = cursor_check_restricted_range(cursor, *key, *keylen);
            if (r != 0) {
                paranoid_invariant(r == cursor->out_of_range_error);
                // We already got at least one entry from the bulk fetch.
                // Return 0 (instead of out of range error).
                r = 0;
                break;
            }
            r = getf(*keylen, *key, *vallen, *val, getf_v, false);
            if (r == TOKUDB_CURSOR_CONTINUE) {
                continue;
            } else {
                break;
            }
        }
    }

    return r;
}

static int ft_cursor_compare_prev(const ft_search_t &search, const DBT *x);
static int
ft_cursor_compare_one(const ft_search_t &search __attribute__((__unused__)), const DBT *x __attribute__((__unused__)));


// This is a bottom layer of the search functions.
static int
ft_search_basement_node(
    FT ft,
    BASEMENTNODE bn,
    ft_search_t *search,
    FT_GET_CALLBACK_FUNCTION getf,
    void *getf_v,
    bool *doprefetch,
    FT_CURSOR ftcursor,
    ANCESTORS ancestors,
    struct pivot_bounds *bounds,
    bool can_bulk_fetch
    )
{
    // Now we have to convert from ft_search_t to the heaviside function with a direction.  What a pain...

    int direction;
    switch (search->direction) {
    case FT_SEARCH_LEFT:   direction = +1; goto ok;
    case FT_SEARCH_RIGHT:  direction = -1; goto ok;
    }
    return EINVAL;  // This return and the goto are a hack to get both compile-time and run-time checking on enum
ok: ;
    uint32_t idx = 0;
    LEAFENTRY le;
    uint32_t keylen;
    void *key;

    // To fix le-cursor-walk unittest case:
    bool search_k_changed = false;
    if (search->use_pivot_bound
      && search->direction == FT_SEARCH_RIGHT
      && search->compare == ft_cursor_compare_prev
      && search->user_key) {
        // we need to use the original user_key to do actual search
        toku_copy_dbt(&search->k, *search->user_key);
        search_k_changed = true;
    }
    // To fix le-cursor-walk unittest case */

    int r = bn->data_buffer.find<decltype(*search), heaviside_from_search_t>(
        *search,
        direction,
        &le,
        &key,
        &keylen,
        &idx
        );

    // To fix le-cursor-walk unittest case:
    if (search_k_changed == true) {
        // modify back to pivot_bound, which is still needed for toku_ft_search_which_child()
        toku_copy_dbt(&search->k, search->pivot_bound);
    }
    // To fix le-cursor-walk unittest case */

    if (r != 0) {
        return r;
    }

    // as now nodes are not actually bounded by pivots, we may need to ignore entries
    if (search->direction == FT_SEARCH_LEFT && bounds->lk) {
        do {
            DBT tmp;
            toku_fill_dbt(&tmp, key, keylen);
            if (compare_k_x(ft, bounds->lk, &tmp) < 0) {
                break;
            }
            idx++;
            if (idx >= bn->data_buffer.omt_size()) {
                return DB_NOTFOUND;
            }
            r = bn->data_buffer.fetch_klpair(idx, &le, &keylen, &key);
            assert_zero(r);
        } while (1);
    } else if (search->direction == FT_SEARCH_RIGHT && bounds->uk) {
        do {
            DBT tmp;
            toku_fill_dbt(&tmp, key, keylen);
            if (compare_k_x(ft, bounds->uk, &tmp) >= 0) {
                break;
            }
            if (idx == 0) {
                return DB_NOTFOUND;
            }
            idx--;
            r = bn->data_buffer.fetch_klpair(idx, &le, &keylen, &key);
            assert_zero(r);
        } while (1);
    }

    if (toku_ft_cursor_is_leaf_mode(ftcursor))
        goto got_a_good_value;        // leaf mode cursors see all leaf entries
    if (is_le_val_del(le,ftcursor)) {
        // Provisionally deleted stuff is gone.
        // So we need to scan in the direction to see if we can find something
        while (1) {
            switch (search->direction) {
            case FT_SEARCH_LEFT:
                idx++;
                if (idx >= bn->data_buffer.omt_size())
                    return DB_NOTFOUND;
                break;
            case FT_SEARCH_RIGHT:
                if (idx == 0)
                    return DB_NOTFOUND;
                idx--;
                break;
            default:
                abort();
            }
            r = bn->data_buffer.fetch_klpair(idx, &le, &keylen, &key);
            assert_zero(r); // we just validated the index
            if (!is_le_val_del(le,ftcursor)) goto got_a_good_value;
        }
    }
got_a_good_value:
    {
        {
            DBT tmp;
            toku_fill_dbt(&tmp, key, keylen);
            if (search->direction == FT_SEARCH_LEFT) {
                if (bounds->uk && compare_k_x(ft, bounds->uk, &tmp) < 0) {
                    return DB_NOTFOUND;
                }
            } else {
                if (bounds->lk && compare_k_x(ft, bounds->lk, &tmp) >= 0) {
                    return DB_NOTFOUND;
                }
            }
        }
        DBT alloc;
        toku_init_dbt(&alloc);
        {
            DBT key_dbt;
            toku_fill_dbt(&key_dbt, key, keylen);
            toku_ft_recover_key_by_ancs(ft, ancestors, &key_dbt, &alloc);
            key = key_dbt.data;
            keylen = key_dbt.size;
        }

        uint32_t vallen;
        void *val;

        ft_cursor_extract_val(le, ftcursor, &vallen, &val);
        r = cursor_check_restricted_range(ftcursor, key, keylen);
        if (r == 0) {
            r = getf(keylen, key, vallen, val, getf_v, false);
        }
        if (r == 0 || r == TOKUDB_CURSOR_CONTINUE) {
            //
            // IMPORTANT: bulk fetch CANNOT go past the current basement node,
            // because there is no guarantee that messages have been applied
            // to other basement nodes, as part of #5770
            //
            if (r == TOKUDB_CURSOR_CONTINUE && can_bulk_fetch) {
                r = ft_cursor_shortcut(ft, ftcursor, ancestors, bounds,
                                       direction, idx,
                                       &bn->data_buffer, getf, getf_v,
                                       &keylen, &key, &alloc,
                                       &vallen, &val);
            }

            ft_cursor_cleanup_dbts(ftcursor);
            if (!ftcursor->is_temporary) {
                toku_memdup_dbt(&ftcursor->key, key, keylen);
                toku_memdup_dbt(&ftcursor->val, val, vallen);
            }
            //The search was successful.  Prefetching can continue.
            *doprefetch = true;
        }
        toku_cleanup_dbt(&alloc);
    }
    if (r == TOKUDB_CURSOR_CONTINUE) {
        r = 0;
    }
    return r;
}

static int
ftnode_fetch_callback_and_free_bfe(CACHEFILE cf, PAIR p, int fd, BLOCKNUM nodename, uint32_t fullhash, void **ftnode_pv, void** UU(disk_data), PAIR_ATTR *sizep, int *dirtyp, void *extraargs)
{
    int r = toku_ftnode_fetch_callback(cf, p, fd, nodename, fullhash, ftnode_pv, disk_data, sizep, dirtyp, extraargs);
    struct ftnode_fetch_extra *CAST_FROM_VOIDP(ffe, extraargs);
    destroy_bfe_for_prefetch(ffe);
    toku_free(ffe);
    return r;
}

static int
ftnode_pf_callback_and_free_bfe(void *ftnode_pv, void* disk_data, void *read_extraargs, int fd, PAIR_ATTR *sizep)
{
    int r = toku_ftnode_pf_callback(ftnode_pv, disk_data, read_extraargs, fd, sizep);
    struct ftnode_fetch_extra *CAST_FROM_VOIDP(ffe, read_extraargs);
    destroy_bfe_for_prefetch(ffe);
    toku_free(ffe);
    return r;
}

static void
ft_node_maybe_prefetch(FT_HANDLE brt, FTNODE node, int childnum, FT_CURSOR ftcursor, bool *doprefetch) {
    // the number of nodes to prefetch
    int num_nodes_to_prefetch;
    if (ftfs_is_hdd()) {
        num_nodes_to_prefetch = 1;
    } else {
        num_nodes_to_prefetch = 2;
    }

    int fd = toku_cachefile_get_fd(brt->ft->cf);
    if (!sb_sfs_is_data_db(fd)) {
        return;
    }

    // if we want to prefetch in the tree
    // then prefetch the next children if there are any
    if (*doprefetch && ft_cursor_prefetching(ftcursor) && !ftcursor->disable_prefetching) {
        int rc;
        if (ftfs_is_hdd()) {
            rc = ft_cursor_rightmost_child_wanted(ftcursor, brt, node);
        } else {
            rc = node->n_children - 1;
        }
        for (int i = childnum + 1; (i <= childnum + num_nodes_to_prefetch) && (i <= rc); i++) {
            BLOCKNUM nextchildblocknum = BP_BLOCKNUM(node, i);
            uint32_t nextfullhash = compute_child_fullhash(brt->ft->cf, node, i);
            struct ftnode_fetch_extra *MALLOC(bfe);
            fill_bfe_for_prefetch(bfe, brt->ft, ftcursor);
            bfe->is_seqread = true;
            bool doing_prefetch = false;
            toku_cachefile_prefetch(
                brt->ft->cf,
                nextchildblocknum,
                nextfullhash,
                get_write_callbacks_for_node(brt->ft),
                ftnode_fetch_callback_and_free_bfe,
                toku_ftnode_pf_req_callback,
                ftnode_pf_callback_and_free_bfe,
                bfe,
                &doing_prefetch
                );
            if (!doing_prefetch) {
                destroy_bfe_for_prefetch(bfe);
                toku_free(bfe);
            }
            *doprefetch = false;
        }
    }
}

struct unlock_ftnode_extra {
    FT_HANDLE ft_handle;
    FTNODE node;
    bool msgs_applied;
};

// When this is called, the cachetable lock is held
static void
unlock_ftnode_fun (void *v) {
    struct unlock_ftnode_extra *x = NULL;
    CAST_FROM_VOIDP(x, v);
    FT_HANDLE brt = x->ft_handle;
    FTNODE node = x->node;
    // CT lock is held
    int r = toku_cachetable_unpin_ct_prelocked_no_flush(
        brt->ft->cf,
        node->ct_pair,
        (enum cachetable_dirty)node->dirty,
        x->msgs_applied ? make_ftnode_pair_attr(node) : make_invalid_pair_attr()
        );
    assert_zero(r);
}


static inline int
search_which_child_cmp_with_bound(DB *db, FT ft,
                                  FTNODE node, int childnum, ft_search_t *search, DBT *dbt)
{
    int ret = 0;
    toku_memdup_dbt(dbt, node->childkeys[childnum].data,
                    node->childkeys[childnum].size);
    ret = ft->key_ops.keycmp(db, dbt, &search->pivot_bound);
    toku_destroy_dbt(dbt);
    return ret;
}

int
toku_ft_search_which_child(FT ft, FTNODE node, ft_search_t *search)
{
    if (node->n_children <= 1) return 0;
    DBT pivotkey;
    toku_init_dbt(&pivotkey);
    int lo = 0;
    int hi = node->n_children - 1;
    int mi;
    while (lo < hi) {
        mi = (lo + hi) / 2;
        toku_copy_dbt(&pivotkey, node->childkeys[mi]);
        // search->compare is really strange, and only works well with a
        // linear search, it makes binary search a pita.
        //
        // if you are searching left to right, it returns
        //   "0" for pivots that are < the target, and
        //   "1" for pivots that are >= the target
        // if you are searching right to left, it's the opposite.
        //
        // so if we're searching from the left and search->compare says
        // "1", we want to go left from here, if it says "0" we want to go
        // right.  searching from the right does the opposite.
        bool c = search->compare(*search, &node->childkeys[mi]);
        if (((search->direction == FT_SEARCH_LEFT) && c) ||
            ((search->direction == FT_SEARCH_RIGHT) && !c)) {
            hi = mi;
        } else {
            assert(((search->direction == FT_SEARCH_LEFT) && !c) ||
                   ((search->direction == FT_SEARCH_RIGHT) && c));
            lo = mi + 1;
        }
    }

    // ready to return something, if the pivot is bounded, we have to move
    // over a bit to get away from what we've already searched
    // we still need following for ft_cursor_compare_one which doesn't use search.k in compare
    if (search->compare == ft_cursor_compare_one
        && search->pivot_bound.data != nullptr) {
        FAKE_DB(db, &ft->cmp_descriptor);
        if (search->direction == FT_SEARCH_LEFT) {
            while (lo < node->n_children - 1 &&
                   search_which_child_cmp_with_bound(&db, ft, node, lo, search, &pivotkey) <= 0) {
                // searching left to right, if the comparison says the
                // current pivot (lo) is left of or equal to our bound,
                // don't search that child again
                lo++;
            }
        } else {
            while (lo > 0 &&
                   search_which_child_cmp_with_bound(&db, ft, node, lo - 1, search, &pivotkey) >= 0) {
                // searching right to left, same argument as just above
                // (but we had to pass lo - 1 because the pivot between lo
                // and the thing just less than it is at that position in
                // the childkeys array)
                lo--;
            }
        }
    }

    return lo;
}

static inline void
init_next_ancestors(ANCESTORS n_ancs, FTNODE node, int childnum, ANCESTORS ancs)
{
    n_ancs->node = node;
    n_ancs->childnum = childnum;
    n_ancs->is_goto = false;
    toku_init_dbt(&n_ancs->goto_lift);
    toku_init_dbt(&n_ancs->goto_not_lifted);
    n_ancs->goto_msn = {0};
    n_ancs->next = ancs;
}

static int
ft_search_leaf(FT_HANDLE brt, FTNODE node, ft_search_t *search, int childnum,
               FT_GET_CALLBACK_FUNCTION getf, void *getf_v, bool multipath,
               bool *doprefetch, FT_CURSOR ftcursor, ANCESTORS ancestors,
               struct pivot_bounds *bounds, bool can_bulk_fetch)
{
    int r;
    FTNODE shadow;
    DBT old_pivot_bound;
    toku_init_dbt(&old_pivot_bound);

    assert_zero(node->height);
    if (!multipath) {
        shadow = node;
    } else {
        DBT version, alloc;
        toku_init_dbt(&alloc);
        if (!bounds->lk) {
            toku_init_dbt(&version);
        } else {
            toku_copy_dbt(&version, *bounds->lk);
            toku_ft_recover_key_by_ancs(brt->ft, ancestors, &version, &alloc);
        }
        shadow = toku_ft_get_shadow(node, &version);
        assert(shadow != NULL);
        toku_cleanup_dbt(&alloc);
    }
    assert(childnum >= 0);
    assert(childnum < shadow->n_children);
    assert(BP_STATE(shadow, childnum) == PT_AVAIL);
    r = ft_search_basement_node(
            brt->ft, BLB(shadow, childnum), search, getf, getf_v,
            doprefetch, ftcursor, ancestors, bounds, can_bulk_fetch);

    if (r == DB_NOTFOUND) {
        // We didn't find a valid key in this node, but there can be a valid
        //   key in the sibling. So we setup searhc->pivot_bound and the
        //   next search will use that as the search key
        DBT pb;
        if (search->pivot_bound.size > 0) {
            toku_memdup_dbt(&old_pivot_bound, search->pivot_bound.data, search->pivot_bound.size);
        }
        toku_cleanup_dbt(&search->pivot_bound);

        if (search->direction == FT_SEARCH_LEFT) {
            if (bounds->uk == NULL) {
                if (childnum == shadow->n_children - 1) {
                    // this is the rightmost
                    goto out;
                }
                toku_copy_dbt(&pb, shadow->childkeys[childnum]);
            } else if (childnum < shadow->n_children - 1 &&
                       compare_k_x(brt->ft, bounds->uk, &shadow->childkeys[childnum]) > 0)
            {
                toku_copy_dbt(&pb, shadow->childkeys[childnum]);
            } else {
                toku_copy_dbt(&pb, *bounds->uk);
            }
        } else {
            if (bounds->lk == NULL) {
                if (childnum == 0) {
                    goto out;
                }
                toku_copy_dbt(&pb, shadow->childkeys[childnum - 1]);
            } else if (childnum > 0 &&
                       compare_k_x(brt->ft, bounds->lk, &shadow->childkeys[childnum - 1]) < 0)
            {
                toku_copy_dbt(&pb, shadow->childkeys[childnum - 1]);
            } else {
                toku_copy_dbt(&pb, *bounds->lk);
            }
        }
        // now next search key is in pb,
        {
            DBT alloc;
            toku_init_dbt(&alloc);
            toku_ft_recover_key_by_ancs(brt->ft, ancestors, &pb, &alloc);
            if (pb.data == alloc.data) {
                assert(pb.size == alloc.size);
                toku_copy_dbt(&search->pivot_bound, alloc);
            } else {
                if (old_pivot_bound.size > 0 && (memcmp(pb.data, old_pivot_bound.data, old_pivot_bound.size) == 0)){
                    goto out; //we already tried this pivot_bound
                }
                toku_clone_dbt(&search->pivot_bound, pb);
                toku_cleanup_dbt(&alloc);
            }
        }

        // this getf locks range
        r = getf(search->pivot_bound.size, search->pivot_bound.data, 0, nullptr, getf_v, true);
        if (!r) {
            search->use_pivot_bound = true;
            r = TOKUDB_TRY_AGAIN;
        }
    }

out:
    toku_destroy_dbt(&old_pivot_bound);
    return r;
}

static int
ft_search_node(FT_HANDLE brt, FTNODE node, ft_search_t *search, int childnum,
               FT_GET_CALLBACK_FUNCTION getf, void *getf_v, bool multipath,
               bool *doprefetch, FT_CURSOR ftcursor, ANCESTORS ancestors,
               struct pivot_bounds *bounds, bool can_bulk_fetch,
               UNLOCKERS unlockers)
{
    if (!node->height) {
        return ft_search_leaf(brt, node, search, childnum, getf, getf_v,
                              multipath, doprefetch, ftcursor, ancestors,
                              bounds, can_bulk_fetch);
    }

    int r;
    assert(childnum >= 0);
    assert(childnum < node->n_children);
    // prepare next ancestors
    struct ancestors next_ancs;
    init_next_ancestors(&next_ancs, node, childnum, ancestors);

    NONLEAF_CHILDINFO bnc = BNC(node, childnum);
    // update bounds
    DBT next_lk, next_uk;
    if (bounds->lk || childnum != 0) {
        if (childnum == 0) {
            toku_copy_dbt(&next_lk, *bounds->lk);
        } else if (bounds->lk == NULL) {

            toku_copy_dbt(&next_lk, node->childkeys[childnum - 1]);
        } else if (compare_k_x(brt->ft, bounds->lk, &node->childkeys[childnum - 1]) >= 0) {
            toku_copy_dbt(&next_lk, *bounds->lk);
        } else {
            toku_copy_dbt(&next_lk, node->childkeys[childnum - 1]);
        }
        bounds->lk = &next_lk;
    }
    if (bounds->uk || childnum != node->n_children - 1) {
        if (childnum == node->n_children - 1) {
            toku_copy_dbt(&next_uk, *bounds->uk);
        } else if (bounds->uk == NULL) {
            toku_copy_dbt(&next_uk, node->childkeys[childnum]);
        } else if (compare_k_x(brt->ft, bounds->uk, &node->childkeys[childnum]) <= 0) {
            toku_copy_dbt(&next_uk, *bounds->uk);
        } else {
            toku_copy_dbt(&next_uk, node->childkeys[childnum]);
        }
        bounds->uk = &next_uk;
    }

    // process BNC lift and not_lifted
    DBT alloc_lk, alloc_uk, alloc_k;
    toku_init_dbt(&alloc_lk);
    toku_init_dbt(&alloc_uk);
    toku_init_dbt(&alloc_k);
    if (bounds->lk) {
        ft_process_key(brt->ft, &BP_LIFT(node, childnum),
                       &BP_NOT_LIFTED(node, childnum), &next_lk, &alloc_lk);
    }
    if (bounds->uk) {
        ft_process_key(brt->ft, &BP_LIFT(node, childnum),
                       &BP_NOT_LIFTED(node, childnum), &next_uk, &alloc_uk);
    }
    ft_process_key(brt->ft, &BP_LIFT(node, childnum),
                   &BP_NOT_LIFTED(node, childnum), &search->k, &alloc_k);

    // the child in the tree
    BLOCKNUM next_blocknum = BP_BLOCKNUM(node, childnum);
    bool next_node_is_nonleaf = node->height > 1;

    // check smart_pivot
    if (bnc->goto_list.size() > 0) {
        // we want the lastest GOTO into which our search falls
        // unfortunately, goto_list sorts GOTO msgs by key
        // grab all GOTO msgs
        const int buffer_size = bnc->goto_list.size();
        int32_t *XMALLOC_N(buffer_size, offsets);
        struct store_fifo_offset_extra sfo_extra = {.offsets = offsets, .i = 0};
        r = bnc->goto_list.iterate
                <struct store_fifo_offset_extra, store_fifo_offset>
                (&sfo_extra);
        assert_zero(r);

        // sort by MSN
        assert(sfo_extra.i == buffer_size);
        r = toku::sort
                <int32_t, FIFO, fifo_offset_msn_cmp>::mergesort_r
                (offsets, buffer_size, bnc->buffer);
        assert_zero(r);

        // search for the latest GOTO msg that covers the search
        for (int i = buffer_size - 1; i >= 0; --i) {
            struct fifo_entry *entry = toku_fifo_get_entry(bnc->buffer, offsets[i]);
            FT_MSG_S cmd;
            DBT k, m, v;
            fifo_entry_get_msg(&cmd, entry, &k, &v, &m);
            assert(ft_msg_get_type(&cmd) == FT_GOTO);
            if (ft_msg_goto_extra(&cmd)->blocknum.b == 0 ||
                    compare_k_x(brt->ft, &(search->k), &k) < 0 ||
                    compare_k_x(brt->ft, &(search->k), &m) >= 0) {
                // 1. GOTO is range delete
                // 2. out of range
                continue;
            }
            // found a good one
            next_ancs.is_goto = true;
            r = toku_ft_lift(brt->ft, &next_ancs.goto_lift, &k, &m);
            assert_zero(r);
            toku_copy_dbt(&next_ancs.goto_not_lifted, v);
            next_ancs.goto_msn = ft_msg_get_msn(&cmd);
            next_blocknum = ft_msg_goto_extra(&cmd)->blocknum;
            next_node_is_nonleaf = ft_msg_goto_extra(&cmd)->height > 0;

            // update bounds if necessary
            if (!bounds->lk || compare_k_x(brt->ft, bounds->lk, &k) < 0) {
                if (!bounds->lk) {
                    bounds->lk = &next_lk;
                }
                toku_cleanup_dbt(&alloc_lk);
                toku_copy_dbt(&next_lk, k);
            }
            if (!bounds->uk || compare_k_x(brt->ft, bounds->uk, &m) > 0) {
                if (!bounds->uk) {
                    bounds->uk = &next_uk;
                }
                toku_cleanup_dbt(&alloc_uk);
                toku_copy_dbt(&next_uk, m);
            }
            // process lift and not_lifted in goto
            assert(bounds->lk == &next_lk);
            ft_process_key(brt->ft, &next_ancs.goto_lift,
                           &next_ancs.goto_not_lifted, &next_lk, &alloc_lk);
            assert(bounds->uk == &next_uk);
            ft_process_key(brt->ft, &next_ancs.goto_lift,
                           &next_ancs.goto_not_lifted, &next_uk, &alloc_uk);
            ft_process_key(brt->ft, &next_ancs.goto_lift,
                           &next_ancs.goto_not_lifted, &search->k, &alloc_k);
            break;
        }
        // cleanup
        toku_free(offsets);
    }

    bool next_multipath = multipath || toku_blocknum_multi_copy(brt->ft->blocktable, next_blocknum);
    struct ftnode_fetch_extra bfe;
    // read_all_partitions = next_node_is_nonleaf
    fill_bfe_for_subset_read(
        &bfe,
        brt->ft,
        search,
        &ftcursor->range_lock_left_key,
        &ftcursor->range_lock_right_key,
        ftcursor->left_is_neg_infty,
        ftcursor->right_is_pos_infty,
        ftcursor->disable_prefetching,
        next_node_is_nonleaf
    );
    uint32_t fullhash = toku_cachetable_hash(brt->ft->cf, next_blocknum);
    FTNODE child = NULL;
    bool msgs_applied = false;
    r = toku_pin_ftnode_for_search(brt, next_blocknum, fullhash, unlockers,
                                   next_multipath, &next_ancs, &bfe, bounds,
                                   &child, &msgs_applied);
    struct unlock_ftnode_extra unlock_extra = {brt, child, msgs_applied};
    struct unlockers next_unlockers = {true, unlock_ftnode_fun, (void *)&unlock_extra, unlockers};
    if (r == TOKUDB_TRY_AGAIN) {
        goto out;
    }
    assert_zero(r);

    r = ft_search_node(brt, child, search, bfe.child_to_read, getf, getf_v,
                       next_multipath, doprefetch, ftcursor, &next_ancs,
                       bounds, can_bulk_fetch, &next_unlockers);

    if (r != TOKUDB_TRY_AGAIN) {
        if (r == 0 && node->height == 1) {
            ft_node_maybe_prefetch(brt, node, childnum, ftcursor, doprefetch);
        }
        assert(next_unlockers.locked);
        if (msgs_applied) {
            toku_unpin_ftnode(brt->ft, child);
        } else {
            toku_unpin_ftnode_read_only(brt->ft, child);
        }
    } else {
        // try again.

        // there are two cases where we get TOKUDB_TRY_AGAIN
        //  case 1 is when some later call to toku_pin_ftnode returned
        //  that value and unpinned all the nodes anyway. case 2
        //  is when ft_search_node had to stop its search because
        //  some piece of a node that it needed was not in memory. In this case,
        //  the node was not unpinned, so we unpin it here
        if (next_unlockers.locked) {
            if (msgs_applied) {
                toku_unpin_ftnode(brt->ft, child);
            } else {
                toku_unpin_ftnode_read_only(brt->ft, child);
            }
        }
    }

out:
    toku_cleanup_dbt(&next_ancs.goto_lift);
    toku_cleanup_dbt(&alloc_lk);
    toku_cleanup_dbt(&alloc_uk);
    toku_cleanup_dbt(&alloc_k);

    return r;
}

static int
toku_ft_search(
    FT_HANDLE brt,
    ft_search_t *search,
    FT_GET_CALLBACK_FUNCTION getf,
    void *getf_v,
    FT_CURSOR ftcursor,
    bool can_bulk_fetch)
// Effect: Perform a search.  Associate cursor with a leaf if possible.
// All searches are performed through this function.
{
    int r;
    // How many tries did it take to get the result?
    uint trycount = 0;
    FT ft = brt->ft;

try_again:
    trycount++;

    //
    // Here is how searches work
    // At a high level, we descend down the tree, using the search parameter
    // to guide us towards where to look. But the search parameter is not
    // used here to determine which child of a node to read (regardless
    // of whether that child is another node or a basement node)
    // The search parameter is used while we are pinning the node into
    // memory, because that is when the system needs to ensure that
    // the appropriate partition of the child we are using is in memory.
    // So, here are the steps for a search (and this applies to this function
    // as well as ft_search_child:
    //  - Take the search parameter, and create a ftnode_fetch_extra, that will be used by toku_pin_ftnode(_holding_lock)
    //  - Call toku_pin_ftnode(_holding_lock) with the bfe as the extra for the fetch callback (in case the node is not at all in memory)
    //       and the partial fetch callback (in case the node is perhaps partially in memory) to the fetch the node
    //  - This eventually calls either toku_ftnode_fetch_callback or  toku_ftnode_pf_req_callback depending on whether the node is in
    //     memory at all or not.
    //  - Within these functions, the "ft_search_t search" parameter is used to evaluate which child the search is interested in.
    //     If the node is not in memory at all, toku_ftnode_fetch_callback will read the node and decompress only the partition for the
    //     relevant child, be it a message buffer or basement node. If the node is in memory, then toku_ftnode_pf_req_callback
    //     will tell the cachetable that a partial fetch is required if and only if the relevant child is not in memory. If the relevant child
    //     is not in memory, then toku_ftnode_pf_callback is called to fetch the partition.
    //  - These functions set bfe->child_to_read so that the search code does not need to reevaluate it.
    //  - Just to reiterate, all of the last item happens within toku_ftnode_pin(_holding_lock)
    //  - At this point, toku_ftnode_pin_holding_lock has returned, with bfe.child_to_read set,
    //  - ft_search_node is called, assuming that the node and its relevant partition are in memory.
    //
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_subset_read(
        &bfe,
        ft,
        search,
        &ftcursor->range_lock_left_key,
        &ftcursor->range_lock_right_key,
        ftcursor->left_is_neg_infty,
        ftcursor->right_is_pos_infty,
        ftcursor->disable_prefetching,
        true // We may as well always read the whole root into memory, if it's a leaf node it's a tiny tree anyway.
        );
    FTNODE root = NULL;
    {
        uint32_t fullhash;
        CACHEKEY root_key;
        toku_calculate_root_offset_pointer(ft, &root_key, &fullhash);
        toku_pin_ftnode_off_client_thread_batched(
            ft,
            root_key,
            fullhash,
            &bfe,
            PL_READ, // may_modify_node set to false, because root cannot change during search
            0,
            NULL,
            &root
            );
    }
    // How high is the tree?
    // This is the height of the root node plus one (leaf is at height 0).
    // Only for accounting
    uint tree_height = root->height + 1;

    struct unlock_ftnode_extra unlock_extra = {brt, root, false};
    struct unlockers unlockers = {true, unlock_ftnode_fun,
                                  (void *)&unlock_extra, (UNLOCKERS)NULL};

    {
        bool doprefetch = false;
        // infinite bounds
        struct pivot_bounds bounds = {.lk = NULL, .uk = NULL};
        r = ft_search_node(brt, root, search, bfe.child_to_read, getf, getf_v,
                           false, &doprefetch, ftcursor, (ANCESTORS)NULL,
                           &bounds, can_bulk_fetch, &unlockers);
        if (r == TOKUDB_TRY_AGAIN) {
            // there are two cases where we get TOKUDB_TRY_AGAIN
            //  case 1 is when some later call to toku_pin_ftnode returned
            //  that value and unpinned all the nodes anyway. case 2
            //  is when ft_search_node had to stop its search because
            //  some piece of a node that it needed was not in memory.
            //  In this case, the node was not unpinned, so we unpin it here
            if (search->use_pivot_bound) {
                toku_copy_dbt(&search->k, search->pivot_bound);
            } else if (search->user_key) {
                toku_copy_dbt(&search->k, *search->user_key);
            }
            if (unlockers.locked) {
                toku_unpin_ftnode_read_only(brt->ft, root);
            }
            goto try_again;
        }
    }

    assert(unlockers.locked);
    toku_unpin_ftnode_read_only(brt->ft, root);

    //Heaviside function (+direction) queries define only a lower or upper
    //bound.  Some queries require both an upper and lower bound.
    //They do this by wrapping the FT_GET_CALLBACK_FUNCTION with another
    //test that checks for the other bound.  If the other bound fails,
    //it returns TOKUDB_FOUND_BUT_REJECTED which means not found, but
    //stop searching immediately, as opposed to DB_NOTFOUND
    //which can mean not found, but keep looking in another leaf.
    if (r == TOKUDB_FOUND_BUT_REJECTED) {
        r = DB_NOTFOUND;
    } else if (r == DB_NOTFOUND) {
        //We truly did not find an answer to the query.
        //Therefore, the FT_GET_CALLBACK_FUNCTION has NOT been called.
        //The contract specifies that the callback function must be called
        //for 'r= (0|DB_NOTFOUND|TOKUDB_FOUND_BUT_REJECTED)'
        //TODO: #1378 This is not the ultimate location of this call to the
        //callback.  It is surely wrong for node-level locking, and probably
        //wrong for the STRADDLE callback for heaviside function(two sets of key/vals)
        int r2 = getf(0, NULL, 0, NULL, getf_v, false);
        if (r2 != 0)
            r = r2;
    }
    {
        // accounting (to detect and measure thrashing)
        // how many retries were needed?
        uint retrycount = trycount - 1;
        if (retrycount) {
            STATUS_INC(FT_TOTAL_RETRIES, retrycount);
        }
        // if at least one node was read from disk more than once
        if (retrycount > tree_height) {
            STATUS_INC(FT_SEARCH_TRIES_GT_HEIGHT, 1);
            if (retrycount > (tree_height + 3))
                STATUS_INC(FT_SEARCH_TRIES_GT_HEIGHTPLUS3, 1);
        }
    }
    return r;
}

struct ft_cursor_search_struct {
    FT_GET_CALLBACK_FUNCTION getf;
    void *getf_v;
    FT_CURSOR cursor;
    ft_search_t *search;
};

/* search for the first kv pair that matches the search object */
static int
ft_cursor_search(FT_CURSOR cursor, ft_search_t *search, FT_GET_CALLBACK_FUNCTION getf, void *getf_v, bool can_bulk_fetch)
{
    int r = toku_ft_search(cursor->ft_handle, search, getf, getf_v, cursor, can_bulk_fetch);
    return r;
}

static int
ft_cursor_compare_one(const ft_search_t &search __attribute__((__unused__)), const DBT *x __attribute__((__unused__)))
{
    return 1;
}

static int
ft_cursor_current_getf(ITEMLEN keylen,                 bytevec key,
                        ITEMLEN vallen,                 bytevec val,
                        void *v, bool lock_only) {
    struct ft_cursor_search_struct *CAST_FROM_VOIDP(bcss, v);
    int r;
    if (key==NULL) {
        r = bcss->getf(0, NULL, 0, NULL, bcss->getf_v, lock_only);
    } else {
        FT_CURSOR cursor = bcss->cursor;
        DBT newkey;
        toku_fill_dbt(&newkey, key, keylen);
        if (compare_k_x(cursor->ft_handle->ft, &cursor->key, &newkey) != 0) {
            r = bcss->getf(0, NULL, 0, NULL, bcss->getf_v, lock_only); // This was once DB_KEYEMPTY
            if (r==0) r = TOKUDB_FOUND_BUT_REJECTED;
        }
        else
            r = bcss->getf(keylen, key, vallen, val, bcss->getf_v, lock_only);
    }
    return r;
}

void ft_search_finish(ft_search_t *so)
{
    toku_cleanup_dbt(&so->pivot_bound);
}

static int ft_cursor_compare_set_range(const ft_search_t &search, const DBT *x) {
    FT_HANDLE CAST_FROM_VOIDP(brt, search.context);
    if (search.use_pivot_bound) {
        // if we are search using pivot_bound set_range should act the same as
        //   next: kv < xy
        return compare_k_x(brt->ft, &search.k, x) < 0;
    }
    return compare_k_x(brt->ft, &search.k, x) <= 0; /* return kv <= xy */
}

int
toku_ft_cursor_current(FT_CURSOR cursor, int op, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    if (ft_cursor_not_set(cursor))
        return EINVAL;
    cursor->direction = 0;
    if (op == DB_CURRENT) {
        struct ft_cursor_search_struct bcss = {getf, getf_v, cursor, 0};
        ft_search_t search; ft_search_init(&search, ft_cursor_compare_set_range, FT_SEARCH_LEFT, &cursor->key, cursor->ft_handle);
        int r = toku_ft_search(cursor->ft_handle, &search, ft_cursor_current_getf, &bcss, cursor, false);
        ft_search_finish(&search);
        return r;
    }
    return getf(cursor->key.size, cursor->key.data, cursor->val.size, cursor->val.data, getf_v, false); // ft_cursor_copyout(cursor, outkey, outval);
}

int
toku_ft_cursor_first(FT_CURSOR cursor, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = 0;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_one, FT_SEARCH_LEFT, 0, cursor->ft_handle);
    int r = ft_cursor_search(cursor, &search, getf, getf_v, false);
    ft_search_finish(&search);
    return r;
}

int
toku_ft_cursor_last(FT_CURSOR cursor, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = 0;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_one, FT_SEARCH_RIGHT, 0, cursor->ft_handle);
    int r = ft_cursor_search(cursor, &search, getf, getf_v, false);
    ft_search_finish(&search);
    return r;
}

static int ft_cursor_compare_next(const ft_search_t &search, const DBT *x) {
    FT_HANDLE CAST_FROM_VOIDP(brt, search.context);
    return compare_k_x(brt->ft, &search.k, x) < 0; /* return min xy: kv < xy */
}

int
toku_ft_cursor_next(FT_CURSOR cursor, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = +1;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_next, FT_SEARCH_LEFT, &cursor->key, cursor->ft_handle);
    int r = ft_cursor_search(cursor, &search, getf, getf_v, true);
    ft_search_finish(&search);
    if (r == 0) ft_cursor_set_prefetching(cursor);
    return r;
}

static int
ft_cursor_search_eq_k_x_getf(ITEMLEN keylen,               bytevec key,
                              ITEMLEN vallen,               bytevec val,
                              void *v, bool lock_only) {
    struct ft_cursor_search_struct *CAST_FROM_VOIDP(bcss, v);
    int r;
    if (key==NULL) {
        r = bcss->getf(0, NULL, 0, NULL, bcss->getf_v, false);
    } else {
        FT_CURSOR cursor = bcss->cursor;
        DBT newkey;
        toku_fill_dbt(&newkey, key, keylen);
        if (compare_k_x(cursor->ft_handle->ft, bcss->search->user_key, &newkey) == 0) {
            r = bcss->getf(keylen, key, vallen, val, bcss->getf_v, lock_only);
        } else {
            r = bcss->getf(0, NULL, 0, NULL, bcss->getf_v, lock_only);
            if (r==0) r = TOKUDB_FOUND_BUT_REJECTED;
        }
    }
    return r;
}

/* search for the kv pair that matches the search object and is equal to k */
static int
ft_cursor_search_eq_k_x(FT_CURSOR cursor, ft_search_t *search, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    struct ft_cursor_search_struct bcss = {getf, getf_v, cursor, search};
    int r = toku_ft_search(cursor->ft_handle, search, ft_cursor_search_eq_k_x_getf, &bcss, cursor, false);
    return r;
}

static int ft_cursor_compare_prev(const ft_search_t &search, const DBT *x) {
    FT_HANDLE CAST_FROM_VOIDP(brt, search.context);
    return compare_k_x(brt->ft, &search.k, x) > 0; /* return max xy: kv > xy */
}

int
toku_ft_cursor_prev(FT_CURSOR cursor, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = -1;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_prev, FT_SEARCH_RIGHT, &cursor->key, cursor->ft_handle);
    int r = ft_cursor_search(cursor, &search, getf, getf_v, true);
    ft_search_finish(&search);
    return r;
}

int
toku_ft_cursor_set(FT_CURSOR cursor, DBT *key, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = 0;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_set_range, FT_SEARCH_LEFT, key, cursor->ft_handle);
    int r = ft_cursor_search_eq_k_x(cursor, &search, getf, getf_v);
    ft_search_finish(&search);
    return r;
}

int
toku_ft_cursor_set_range(FT_CURSOR cursor, DBT *key, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = 0;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_set_range, FT_SEARCH_LEFT, key, cursor->ft_handle);
    int r = ft_cursor_search(cursor, &search, getf, getf_v, false);
    ft_search_finish(&search);
    return r;
}

static int ft_cursor_compare_set_range_reverse(const ft_search_t &search, const DBT *x) {
    FT_HANDLE CAST_FROM_VOIDP(brt, search.context);
    if (search.use_pivot_bound) {
        // if we are search using pivot_bound set_range should act the same as
        //   next: kv > xy
        return compare_k_x(brt->ft, &search.k, x) > 0;
    }
    return compare_k_x(brt->ft, &search.k, x) >= 0; /* return kv >= xy */
}

int
toku_ft_cursor_set_range_reverse(FT_CURSOR cursor, DBT *key, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    cursor->direction = 0;
    ft_search_t search; ft_search_init(&search, ft_cursor_compare_set_range_reverse, FT_SEARCH_RIGHT, key, cursor->ft_handle);
    int r = ft_cursor_search(cursor, &search, getf, getf_v, false);
    ft_search_finish(&search);
    return r;
}


//TODO: When tests have been rewritten, get rid of this function.
//Only used by tests.
int
toku_ft_cursor_get (FT_CURSOR cursor, DBT *key, FT_GET_CALLBACK_FUNCTION getf, void *getf_v, int get_flags)
{
    int op = get_flags & DB_OPFLAGS_MASK;
    if (get_flags & ~DB_OPFLAGS_MASK)
        return EINVAL;

    switch (op) {
    case DB_CURRENT:
    case DB_CURRENT_BINDING:
        return toku_ft_cursor_current(cursor, op, getf, getf_v);
    case DB_FIRST:
        return toku_ft_cursor_first(cursor, getf, getf_v);
    case DB_LAST:
        return toku_ft_cursor_last(cursor, getf, getf_v);
    case DB_NEXT:
        if (ft_cursor_not_set(cursor)) {
            return toku_ft_cursor_first(cursor, getf, getf_v);
        } else {
            return toku_ft_cursor_next(cursor, getf, getf_v);
        }
    case DB_PREV:
        if (ft_cursor_not_set(cursor)) {
            return toku_ft_cursor_last(cursor, getf, getf_v);
        } else {
            return toku_ft_cursor_prev(cursor, getf, getf_v);
        }
    case DB_SET:
        return toku_ft_cursor_set(cursor, key, getf, getf_v);
    case DB_SET_RANGE:
        return toku_ft_cursor_set_range(cursor, key, getf, getf_v);
    default: ;// Fall through
    }
    return EINVAL;
}

void
toku_ft_cursor_peek(FT_CURSOR cursor, const DBT **pkey, const DBT **pval)
// Effect: Retrieves a pointer to the DBTs for the current key and value.
// Requires:  The caller may not modify the DBTs or the memory at which they points.
// Requires:  The caller must be in the context of a
// FT_GET_(STRADDLE_)CALLBACK_FUNCTION
{
    *pkey = &cursor->key;
    *pval = &cursor->val;
}

//We pass in toku_dbt_fake to the search functions, since it will not pass the
//key(or val) to the heaviside function if key(or val) is NULL.
//It is not used for anything else,
//the actual 'extra' information for the heaviside function is inside the
//wrapper.
static const DBT __toku_dbt_fake = {};
static const DBT* const toku_dbt_fake = &__toku_dbt_fake;

bool toku_ft_cursor_uninitialized(FT_CURSOR c) {
    return ft_cursor_not_set(c);
}


/* ********************************* lookup **************************************/

int
toku_ft_lookup (FT_HANDLE brt, DBT *k, FT_GET_CALLBACK_FUNCTION getf, void *getf_v)
{
    int r, rr;
    FT_CURSOR cursor;

    rr = toku_ft_cursor(brt, &cursor, NULL, false, false);
    if (rr != 0) return rr;

    int op = DB_SET;
    r = toku_ft_cursor_get(cursor, k, getf, getf_v, op);

    toku_ft_cursor_close(cursor);

    return r;
}

/* ********************************* delete **************************************/
static int
getf_nothing (ITEMLEN UU(keylen), bytevec UU(key), ITEMLEN UU(vallen), bytevec UU(val), void *UU(pair_v), bool UU(lock_only)) {
    return 0;
}

int
toku_ft_cursor_delete(FT_CURSOR cursor, int flags, TOKUTXN txn) {
    int r;

    int unchecked_flags = flags;
    bool error_if_missing = (bool) !(flags&DB_DELETE_ANY);
    unchecked_flags &= ~DB_DELETE_ANY;
    if (unchecked_flags!=0) r = EINVAL;
    else if (ft_cursor_not_set(cursor)) r = EINVAL;
    else {
        r = 0;
        if (error_if_missing) {
            r = toku_ft_cursor_current(cursor, DB_CURRENT, getf_nothing, NULL);
        }
        if (r == 0) {
            toku_ft_delete(cursor->ft_handle, &cursor->key, txn);
        }
    }
    return r;
}

/* ********************* keyrange ************************ */


struct keyrange_compare_s {
    FT ft;
    const DBT *key;
};

static int
keyrange_compare (DBT const &kdbt, const struct keyrange_compare_s &s) {
    // TODO: maybe put a const fake_db in the header
    FAKE_DB(db, &s.ft->cmp_descriptor);
    return s.ft->key_ops.keycmp(&db, &kdbt, s.key);
}

static void
keysrange_in_leaf_partition (FT_HANDLE brt, FTNODE node,
                             DBT* key_left, DBT* key_right,
                             int left_child_number, int right_child_number, uint64_t estimated_num_rows,
                             uint64_t *less, uint64_t* equal_left, uint64_t* middle,
                             uint64_t* equal_right, uint64_t* greater, bool* single_basement_node)
// If the partition is in main memory then estimate the number
// Treat key_left == NULL as negative infinity
// Treat key_right == NULL as positive infinity
{
    paranoid_invariant(node->height == 0); // we are in a leaf
    paranoid_invariant(!(key_left == NULL && key_right != NULL));
    paranoid_invariant(left_child_number <= right_child_number);
    bool single_basement = left_child_number == right_child_number;
    paranoid_invariant(!single_basement || (BP_STATE(node, left_child_number) == PT_AVAIL));
    if (BP_STATE(node, left_child_number) == PT_AVAIL) {
        int r;
        // The partition is in main memory then get an exact count.
        struct keyrange_compare_s s_left = {brt->ft, key_left};
        BASEMENTNODE bn = BLB(node, left_child_number);
        uint32_t idx_left = 0;
        // if key_left is NULL then set r==-1 and idx==0.
        r = key_left ? bn->data_buffer.find_zero<decltype(s_left), keyrange_compare>(s_left, nullptr, nullptr, nullptr, &idx_left) : -1;
        *less = idx_left;
        *equal_left = (r==0) ? 1 : 0;

        uint32_t size = bn->data_buffer.omt_size();
        uint32_t idx_right = size;
        r = -1;
        if (single_basement && key_right) {
            struct keyrange_compare_s s_right = {brt->ft, key_right};
            r = bn->data_buffer.find_zero<decltype(s_right), keyrange_compare>(s_right, nullptr, nullptr, nullptr, &idx_right);
        }
        *middle = idx_right - idx_left - *equal_left;
        *equal_right = (r==0) ? 1 : 0;
        *greater = size - idx_right - *equal_right;
    } else {
        paranoid_invariant(!single_basement);
        uint32_t idx_left = estimated_num_rows / 2;
        if (!key_left) {
            //Both nullptr, assume key_left belongs before leftmost entry, key_right belongs after rightmost entry
            idx_left = 0;
            paranoid_invariant(!key_right);
        }
        // Assume idx_left and idx_right point to where key_left and key_right belong, (but are not there).
        *less = idx_left;
        *equal_left = 0;
        *middle = estimated_num_rows - idx_left;
        *equal_right = 0;
        *greater = 0;
    }
    *single_basement_node = single_basement;
}

static int
toku_ft_keysrange_internal(
    FT_HANDLE brt,
    FTNODE node,
    DBT *key_left,
    DBT *key_right,
    bool may_find_right,
    uint64_t *less,
    uint64_t *equal_left,
    uint64_t *middle,
    uint64_t *equal_right,
    uint64_t *greater,
    bool *single_basement_node,
    uint64_t estimated_num_rows,
    struct ftnode_fetch_extra *min_bfe, // set up to read a minimal read.
    struct ftnode_fetch_extra *match_bfe, // set up to read a basement node iff both keys in it
    struct unlockers *unlockers,
    ANCESTORS ancestors)
// Implementation note: Assign values to less, equal, and greater, and then on the way out (returning up the stack) we add more values in.
{
    int r = 0;
    // if KEY is NULL then use the leftmost key.
    int left_child_number;
    if (key_left)
        left_child_number = toku_ftnode_which_child(
                                node, key_left,
                                &brt->ft->cmp_descriptor,
                                brt->ft->key_ops.keycmp);
    else
        left_child_number = 0;
    int right_child_number = node->n_children;  // Sentinel that does not equal left_child_number.
    if (may_find_right) {
        if (key_right) {
            right_child_number = toku_ftnode_which_child(
                                    node, key_right,
                                    &brt->ft->cmp_descriptor,
                                    brt->ft->key_ops.keycmp);
        } else {
            right_child_number = node->n_children - 1;
        }
    } else {
        right_child_number = node->n_children;
    }

    uint64_t rows_per_child = estimated_num_rows / node->n_children;
    if (node->height == 0) {
        keysrange_in_leaf_partition(
            brt, node, key_left, key_right,
            left_child_number, right_child_number, rows_per_child,
            less, equal_left, middle, equal_right, greater,
            single_basement_node);

        *less += rows_per_child * left_child_number;
        if (*single_basement_node) {
            *greater += rows_per_child * (node->n_children - left_child_number - 1);
        } else {
            *middle += rows_per_child * (node->n_children - left_child_number - 1);
        }
    } else {
        // do the child.
        struct ancestors next_ancestors;
        init_next_ancestors(&next_ancestors, node, left_child_number, ancestors);
        BLOCKNUM childblocknum = BP_BLOCKNUM(node, left_child_number);
        uint32_t fullhash = compute_child_fullhash(brt->ft->cf, node, left_child_number);
        FTNODE childnode;
        bool msgs_applied = false;
        bool child_may_find_right = may_find_right && left_child_number == right_child_number;
        r = toku_pin_ftnode_for_search(
            brt,
            childblocknum,
            fullhash,
            unlockers,
            false,
            &next_ancestors,
            child_may_find_right ? match_bfe : min_bfe,
            NULL,
            &childnode,
            &msgs_applied
            );
        paranoid_invariant(!msgs_applied);
        if (r != TOKUDB_TRY_AGAIN) {
            assert_zero(r);
            struct unlock_ftnode_extra unlock_extra = {brt,childnode,false};
            struct unlockers next_unlockers = {true, unlock_ftnode_fun, (void*)&unlock_extra, unlockers};

            r = toku_ft_keysrange_internal(
                    brt, childnode, key_left, key_right, child_may_find_right,
                    less, equal_left, middle, equal_right, greater, single_basement_node,
                    rows_per_child, min_bfe, match_bfe, &next_unlockers, &next_ancestors);
            if (r != TOKUDB_TRY_AGAIN) {
                assert_zero(r);
                *less += rows_per_child * left_child_number;
                if (*single_basement_node) {
                    *greater += rows_per_child * (node->n_children - left_child_number - 1);
                } else {
                    *middle += rows_per_child * (node->n_children - left_child_number - 1);
                }

                assert(unlockers->locked);
                toku_unpin_ftnode_read_only(brt->ft, childnode);
            }
        }
    }
    return r;
}

void toku_ft_keysrange(
        FT_HANDLE brt,
        DBT *key_left,
        DBT *key_right,
        uint64_t *less_p,
        uint64_t *equal_left_p,
        uint64_t *middle_p,
        uint64_t *equal_right_p,
        uint64_t *greater_p,
        bool *middle_3_exact_p)
// Effect: Return an estimate  of the number of keys to the left, the number equal (to left key), number between keys, number equal to right key, and the number to the right of both keys.
//   The values are an estimate.
//   If you perform a keyrange on two keys that are in the same basement, equal_less, middle, and equal_right will be exact.
//   4184: What to do with a NULL key?
//   key_left==NULL is treated as -infinity
//   key_right==NULL is treated as +infinity
//   If KEY is NULL then the system picks an arbitrary key and returns it.
//   key_right can be non-null only if key_left is non-null;
{
    if (!key_left && key_right) {
        // Simplify internals by only supporting key_right != null when key_left != null
        // If key_right != null and key_left == null, then swap them and fix up numbers.
        uint64_t less = 0, equal_left = 0, middle = 0, equal_right = 0, greater = 0;
        toku_ft_keysrange(brt, key_right, nullptr,
                          &less, &equal_left, &middle, &equal_right, &greater,
                          middle_3_exact_p);
        *less_p = 0;
        *equal_left_p = 0;
        *middle_p = less;
        *equal_right_p = equal_left;
        *greater_p = middle;
        invariant_zero(equal_right);
        invariant_zero(greater);
        return;
    }
    paranoid_invariant(!(!key_left && key_right));
    struct ftnode_fetch_extra min_bfe;
    struct ftnode_fetch_extra match_bfe;
    // read pivot keys but not message buffers
    fill_bfe_for_min_read(&min_bfe, brt->ft);
    // read basement node only if both keys in it.
    fill_bfe_for_keymatch(&match_bfe, brt->ft, key_left, key_right, false, false);
try_again:
    {
        uint64_t less = 0, equal_left = 0, middle = 0, equal_right = 0, greater = 0;
        bool single_basement_node = false;
        FTNODE node = NULL;
        {
            uint32_t fullhash;
            CACHEKEY root_key;
            toku_calculate_root_offset_pointer(brt->ft, &root_key, &fullhash);
            toku_pin_ftnode_off_client_thread_batched(
                brt->ft,
                root_key,
                fullhash,
                &match_bfe,
                PL_READ, // may_modify_node, cannot change root during keyrange
                0,
                NULL,
                &node
                );
        }

        struct unlock_ftnode_extra unlock_extra = {brt,node,false};
        struct unlockers unlockers = {true, unlock_ftnode_fun, (void*)&unlock_extra, (UNLOCKERS)NULL};

        {
            int r;
            int64_t numrows = brt->ft->in_memory_stats.numrows;
            // prevent appearance of a negative number
            if (numrows < 0)
                numrows = 0;
            r = toku_ft_keysrange_internal(brt, node, key_left, key_right, true,
                                           &less, &equal_left, &middle,
                                           &equal_right, &greater,
                                           &single_basement_node, numrows,
                                           &min_bfe, &match_bfe,
                                           &unlockers, (ANCESTORS)NULL);
            assert(r == 0 || r == TOKUDB_TRY_AGAIN);
            if (r == TOKUDB_TRY_AGAIN) {
                assert(!unlockers.locked);
                goto try_again;
            }
            // May need to do a second query.
            if (!single_basement_node && key_right != nullptr) {
                // "greater" is stored in "middle"
                invariant_zero(equal_right);
                invariant_zero(greater);
                uint64_t less2 = 0, equal_left2 = 0, middle2 = 0, equal_right2 = 0, greater2 = 0;
                bool ignore;
                r = toku_ft_keysrange_internal(brt, node, key_right, NULL, false,
                                               &less2, &equal_left2, &middle2,
                                               &equal_right2, &greater2,
                                               &ignore, numrows,
                                               &min_bfe, &match_bfe,
                                               &unlockers, (ANCESTORS)NULL);
                assert(r == 0 || r == TOKUDB_TRY_AGAIN);
                if (r == TOKUDB_TRY_AGAIN) {
                    assert(!unlockers.locked);
                    goto try_again;
                }
                invariant_zero(equal_right2);
                invariant_zero(greater2);
                // Update numbers.
                // less is already correct.
                // equal_left is already correct.

                // "middle" currently holds everything greater than left_key in first query
                // 'middle2' currently holds everything greater than right_key in second query
                // 'equal_left2' is how many match right_key

                // Prevent underflow.
                if (middle >= equal_left2 + middle2) {
                    middle -= equal_left2 + middle2;
                } else {
                    middle = 0;
                }
                equal_right = equal_left2;
                greater = middle2;
            }
        }
        assert(unlockers.locked);
        toku_unpin_ftnode_read_only(brt->ft, node);
        if (!key_right) {
            paranoid_invariant_zero(equal_right);
            paranoid_invariant_zero(greater);
        }
        if (!key_left) {
            paranoid_invariant_zero(less);
            paranoid_invariant_zero(equal_left);
        }
        *less_p           = less;
        *equal_left_p     = equal_left;
        *middle_p         = middle;
        *equal_right_p    = equal_right;
        *greater_p        = greater;
        *middle_3_exact_p = single_basement_node;
    }
}

struct get_key_after_bytes_iterate_extra {
    uint64_t skip_len;
    uint64_t *skipped;
    void (*callback)(const DBT *, uint64_t, void *);
    void *cb_extra;
};

static int get_key_after_bytes_iterate(const void* key, const uint32_t keylen, const LEAFENTRY & le, const uint32_t UU(idx), struct get_key_after_bytes_iterate_extra * const e) {
    // only checking the latest val, mvcc will make this inaccurate
    uint64_t pairlen = keylen + le_latest_vallen(le);
    if (*e->skipped + pairlen > e->skip_len) {
        // found our key!
        DBT end_key;
        toku_fill_dbt(&end_key, key, keylen);
        e->callback(&end_key, *e->skipped, e->cb_extra);
        return 1;
    } else {
        *e->skipped += pairlen;
        return 0;
    }
}

static int get_key_after_bytes_in_basementnode(FT ft, BASEMENTNODE bn, const DBT *start_key, uint64_t skip_len, void (*callback)(const DBT *, uint64_t, void *), void *cb_extra, uint64_t *skipped) {
    int r;
    uint32_t idx_left = 0;
    if (start_key != nullptr) {
        struct keyrange_compare_s cmp = {ft, start_key};
        r = bn->data_buffer.find_zero<decltype(cmp), keyrange_compare>(cmp, nullptr, nullptr, nullptr, &idx_left);
        assert(r == 0 || r == DB_NOTFOUND);
    }
    struct get_key_after_bytes_iterate_extra iter_extra = {skip_len, skipped, callback, cb_extra};
    r = bn->data_buffer.omt_iterate_on_range<get_key_after_bytes_iterate_extra, get_key_after_bytes_iterate>(idx_left, bn->data_buffer.omt_size(), &iter_extra);

    // Invert the sense of r == 0 (meaning the iterate finished, which means we didn't find what we wanted)
    if (r == 1) {
        r = 0;
    } else {
        r = DB_NOTFOUND;
    }
    return r;
}

static int get_key_after_bytes_in_subtree(FT_HANDLE ft_h, FT ft, FTNODE node, UNLOCKERS unlockers, ANCESTORS ancestors, FTNODE_FETCH_EXTRA bfe, ft_search_t *search, uint64_t subtree_bytes, const DBT *start_key, uint64_t skip_len, void (*callback)(const DBT *, uint64_t, void *), void *cb_extra, uint64_t *skipped);

static int get_key_after_bytes_in_child(FT_HANDLE ft_h, FT ft, FTNODE node, UNLOCKERS unlockers, ANCESTORS ancestors, FTNODE_FETCH_EXTRA bfe, ft_search_t *search, int childnum, uint64_t subtree_bytes, const DBT *start_key, uint64_t skip_len, void (*callback)(const DBT *, uint64_t, void *), void *cb_extra, uint64_t *skipped) {
    int r;
    struct ancestors next_ancestors;
    init_next_ancestors(&next_ancestors, node, childnum, ancestors);
    BLOCKNUM childblocknum = BP_BLOCKNUM(node, childnum);
    uint32_t fullhash = compute_child_fullhash(ft->cf, node, childnum);
    FTNODE child;
    bool msgs_applied = false;
    r = toku_pin_ftnode_for_search(ft_h, childblocknum, fullhash, unlockers, false, &next_ancestors, bfe, NULL, &child, &msgs_applied);
    paranoid_invariant(!msgs_applied);
    if (r == TOKUDB_TRY_AGAIN) {
        return r;
    }
    assert_zero(r);
    struct unlock_ftnode_extra unlock_extra = {ft_h, child, false};
    struct unlockers next_unlockers = {true, unlock_ftnode_fun, (void *) &unlock_extra, unlockers};
    DBT lk, uk;
    toku_init_dbt(&lk);
    toku_init_dbt(&uk);
    r = get_key_after_bytes_in_subtree(ft_h, ft, child, &next_unlockers, &next_ancestors, bfe, search, subtree_bytes, start_key, skip_len, callback, cb_extra, skipped);
    toku_cleanup_dbt(&lk);
    toku_cleanup_dbt(&uk);
    return r;
}

static int get_key_after_bytes_in_subtree(FT_HANDLE ft_h, FT ft, FTNODE node, UNLOCKERS unlockers, ANCESTORS ancestors, FTNODE_FETCH_EXTRA bfe, ft_search_t *search, uint64_t subtree_bytes, const DBT *start_key, uint64_t skip_len, void (*callback)(const DBT *, uint64_t, void *), void *cb_extra, uint64_t *skipped) {
    int r;
    int childnum = toku_ft_search_which_child(ft, node, search);
    const uint64_t child_subtree_bytes = subtree_bytes / node->n_children;
    if (node->height == 0) {
        r = DB_NOTFOUND;
        for (int i = childnum; r == DB_NOTFOUND && i < node->n_children; ++i) {
            // The theory here is that a leaf node could only be very
            // unbalanced if it's dirty, which means all its basements are
            // available.  So if a basement node is available, we should
            // check it as carefully as possible, but if it's compressed
            // or on disk, then it should be fairly well balanced so we
            // can trust the fanout calculation.
            if (BP_STATE(node, i) == PT_AVAIL) {
                r = get_key_after_bytes_in_basementnode(ft, BLB(node, i), (i == childnum) ? start_key : nullptr, skip_len, callback, cb_extra, skipped);
            } else {
                *skipped += child_subtree_bytes;
                if (*skipped >= skip_len && i < node->n_children - 1) {
                    callback(&node->childkeys[i], *skipped, cb_extra);
                    r = 0;
                }
                // Otherwise, r is still DB_NOTFOUND.  If this is the last
                // basement node, we'll return DB_NOTFOUND and that's ok.
                // Some ancestor in the call stack will check the next
                // node over and that will call the callback, or if no
                // such node exists, we're at the max key and we should
                // return DB_NOTFOUND up to the top.
            }
        }
    } else {
        r = get_key_after_bytes_in_child(ft_h, ft, node, unlockers, ancestors, bfe, search, childnum, child_subtree_bytes, start_key, skip_len, callback, cb_extra, skipped);
        for (int i = childnum + 1; r == DB_NOTFOUND && i < node->n_children; ++i) {
            if (*skipped + child_subtree_bytes < skip_len) {
                *skipped += child_subtree_bytes;
            } else {
                r = get_key_after_bytes_in_child(ft_h, ft, node, unlockers, ancestors, bfe, search, i, child_subtree_bytes, nullptr, skip_len, callback, cb_extra, skipped);
            }
        }
    }

    if (r != TOKUDB_TRY_AGAIN) {
        assert(unlockers->locked);
        toku_unpin_ftnode_read_only(ft, node);
        unlockers->locked = false;
    }
    return r;
}

int toku_ft_get_key_after_bytes(FT_HANDLE ft_h, const DBT *start_key, uint64_t skip_len, void (*callback)(const DBT *end_key, uint64_t actually_skipped, void *extra), void *cb_extra)
// Effect:
//  Call callback with end_key set to the largest key such that the sum of the sizes of the key/val pairs in the range [start_key, end_key) is <= skip_len.
//  Call callback with actually_skipped set to the sum of the sizes of the key/val pairs in the range [start_key, end_key).
// Notes:
//  start_key == nullptr is interpreted as negative infinity.
//  end_key == nullptr is interpreted as positive infinity.
//  Only the latest val is counted toward the size, in the case of MVCC data.
// Implementation:
//  This is an estimated calculation.  We assume for a node that each of its subtrees have equal size.  If the tree is a single basement node, then we will be accurate, but otherwise we could be quite off.
// Returns:
//  0 on success
//  an error code otherwise
{
    FT ft = ft_h->ft;
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_min_read(&bfe, ft);
    while (true) {
        FTNODE root;
        {
            uint32_t fullhash;
            CACHEKEY root_key;
            toku_calculate_root_offset_pointer(ft, &root_key, &fullhash);
            toku_pin_ftnode_off_client_thread_batched(ft, root_key, fullhash, &bfe, PL_READ, 0, nullptr, &root);
        }
        struct unlock_ftnode_extra unlock_extra = {ft_h, root, false};
        struct unlockers unlockers = {true, unlock_ftnode_fun, (void*)&unlock_extra, (UNLOCKERS) nullptr};
        ft_search_t search;
        ft_search_init(&search, (start_key == nullptr ? ft_cursor_compare_one : ft_cursor_compare_set_range), FT_SEARCH_LEFT, start_key, ft_h);

        int r;
        // We can't do this because of #5768, there may be dictionaries in the wild that have negative stats.  This won't affect mongo so it's ok:
        //paranoid_invariant(ft->in_memory_stats.numbytes >= 0);
        int64_t numbytes = ft->in_memory_stats.numbytes;
        if (numbytes < 0) {
            numbytes = 0;
        }
        uint64_t skipped = 0;
        r = get_key_after_bytes_in_subtree(ft_h, ft, root, &unlockers, nullptr, &bfe, &search, (uint64_t) numbytes, start_key, skip_len, callback, cb_extra, &skipped);
        assert(!unlockers.locked);
        if (r != TOKUDB_TRY_AGAIN) {
            if (r == DB_NOTFOUND) {
                callback(nullptr, skipped, cb_extra);
                r = 0;
            }
            return r;
        }
    }
}

//Test-only wrapper for the old one-key range function
void toku_ft_keyrange(FT_HANDLE brt, DBT *key, uint64_t *less,  uint64_t *equal,  uint64_t *greater) {
    uint64_t zero_equal_right, zero_greater;
    bool ignore;
    toku_ft_keysrange(brt, key, nullptr, less, equal, greater, &zero_equal_right, &zero_greater, &ignore);
    invariant_zero(zero_equal_right);
    invariant_zero(zero_greater);
}

void toku_ft_handle_stat64 (FT_HANDLE brt, TOKUTXN UU(txn), struct ftstat64_s *s) {
    toku_ft_stat64(brt->ft, s);
}

void toku_ft_handle_get_fractal_tree_info64(FT_HANDLE ft_h, struct ftinfo64 *s) {
    toku_ft_get_fractal_tree_info64(ft_h->ft, s);
}

int toku_ft_handle_iterate_fractal_tree_block_map(FT_HANDLE ft_h, int (*iter)(uint64_t,int64_t,int64_t,int64_t,int64_t,void*), void *iter_extra) {
    return toku_ft_iterate_fractal_tree_block_map(ft_h->ft, iter, iter_extra);
}

static int
iterate_dump_ftnode(FT_MSG UU(msg), bool UU(is_fresh), void *UU(args))
{
    // Yang: disable this func
    return 0;
}

static int
toku_dump_ftnode (FILE *file, FT_HANDLE brt, BLOCKNUM blocknum, int depth, const DBT *lorange, const DBT *hirange) {
    int result=0;
    FTNODE node;
//    toku_get_node_for_verify(blocknum, brt, &node);
 //   result=toku_verify_ftnode(brt, brt->ft->h->max_msn_in_ft, brt->ft->h->max_msn_in_ft, false, node, -1, lorange, hirange, NULL, NULL, 0, 1, 0);
    uint32_t fullhash = toku_cachetable_hash(brt->ft->cf, blocknum);
    struct ftnode_fetch_extra bfe;
    fill_bfe_for_full_read(&bfe, brt->ft);
    toku_pin_ftnode_off_client_thread(
        brt->ft,
        blocknum,
        fullhash,
        &bfe,
        PL_WRITE_EXPENSIVE,
        0,
        NULL,
        &node
        );
    assert(node->fullhash==fullhash);
    fprintf(file, "%*sNode=%p\n", depth, "", node);

    fprintf(file, "%*sNode %" PRId64 " height=%d n_children=%d  keyrange=%s %s\n",
            depth, "", blocknum.b, node->height, node->n_children, (char*)(lorange ? lorange->data : 0), (char*)(hirange ? hirange->data : 0));

    {
        int i;
        for (i=0; i+1< node->n_children; i++) {
            fprintf(file, "%*spivotkey %d =", depth+1, "", i);
            //toku_print_BYTESTRING(file, node->childkeys[i].size, (char *) node->childkeys[i].data);
            printf_slice_key_1(&brt->ft->key_ops, &node->childkeys[i]);
            fprintf(file, " \n");
        }
//        printf("%s:%d\n", __func__,__LINE__);
        for (i=0; i< node->n_children; i++) {
            if (node->height > 0) {
                NONLEAF_CHILDINFO bnc = BNC(node, i);
                fprintf(file, "%*schild %d buffered (%d entries):", depth+1, "", i, toku_bnc_n_entries(bnc));
                //FIFO_ITERATE(bnc->buffer, key, keylen, data, datalen, type, msn, xids, UU(is_fresh),
                toku_fifo_iterate(bnc->buffer, iterate_dump_ftnode, &depth);
            }
            else {
                int size = BLB_DATA(node, i)->omt_size();
                BASEMENTNODE bn = BLB(node, i);
                fprintf(file, "%*schild %d msn =0x%" PRIu64 "\n", depth+1, "", i, bn->max_msn_applied.msn);
                if (1)
                    for (int j=0; j<size; j++) {
                        LEAFENTRY le;
                        void* keyp = NULL;
                        uint32_t keylen = 0;
                        int r = BLB_DATA(node,i)->fetch_klpair(j, &le, &keylen, &keyp);
                        assert_zero(r);
                        fprintf(file, " [%d]=", j);
                        //DBT k={.data=keyp, .size=keylen};
                        //printf_slice_key_1(&brt->ft->key_ops, &k);
                        fprintf(file, " \n");
                    }
                fprintf(file, " \n");
            }
        }
        if (node->height > 0) {
            for (i=0; i<node->n_children; i++) {
                fprintf(file, "%*schild %d\n", depth, "", i);
                if (i>0) {
                    char *CAST_FROM_VOIDP(key, node->childkeys[i-1].data);
                    fprintf(file, "%*spivot %d len=%u %u\n", depth+1, "", i-1, node->childkeys[i-1].size, (unsigned)toku_dtoh32(*(int*)key));
                }
                toku_dump_ftnode(file, brt, BP_BLOCKNUM(node, i), depth+4,
                                  (i==0) ? lorange : &node->childkeys[i-1],
                                  (i==node->n_children-1) ? hirange : &node->childkeys[i]);
            }
        }
    }

    toku_unpin_ftnode_off_client_thread(brt->ft, node);
    return result;
}

int toku_dump_ft (FILE *f, FT_HANDLE brt) {
    int r;
    assert(brt->ft);
    toku_dump_translation_table(f, brt->ft->blocktable);
    {
        uint32_t fullhash = 0;
        CACHEKEY root_key;
        toku_calculate_root_offset_pointer(brt->ft, &root_key, &fullhash);
        r = toku_dump_ftnode(f, brt, root_key, 0, 0, 0);
    }
    return r;
}

static int toku_dump_ft_size(FT_HANDLE brt)
{
    assert(brt->ft);
    uint32_t fullhash = 0;
    CACHEKEY root_key;
    toku_calculate_root_offset_pointer(brt->ft, &root_key, &fullhash);
    //return toku_dump_ftnode_size_only(stderr, brt, root_key, 0, NULL);
    return 0;
}

int toku_ft_dump_ftnode(FT_HANDLE brt, BLOCKNUM b)
{
    if (b.b >= 0) {
        return toku_dump_ftnode(stderr, brt, b, 0, 0, 0);
    } else {
        return toku_dump_ft_size(brt);
    }
}

int toku_ft_layer_init(void) {
    int r = 0;
    //Portability must be initialized first
    r = toku_portability_init();
    if (r) { goto exit; }
    r = db_env_set_toku_product_name("tokudb");
    if (r) { goto exit; }

    partitioned_counters_init();
    status_init();
    txn_status_init();
    toku_checkpoint_init();
    toku_ft_serialize_layer_init();
    toku_mutex_init(&ft_open_close_lock, NULL);
exit:
    return r;
}

#ifdef TOKU_LINUX_MODULE
int toku_ft_layer_init_with_panicenv(void) {
    int r = 0;
    //Portability must be initialized first
    r = toku_portability_init();
    if (r) { goto exit; }
    r = db_env_set_toku_product_name("tokudb");
     if (r == ENAMETOOLONG)
     { goto exit; }
     else if(r == EINVAL) {
         printf("\nWARNING!!! though the tests can proceed, the zombie env is going to fail rmmod, please shut down the last test gracefully\n");
         r = 0;
     }

//if r == EINVAL there is only one possibility -- the tokudb_num_envs > 0
// which means some env crashed without cleaning up the txn and state...but
// toku_product_name should have been setup properly...we can skip.
// we shall let the test proceed for our back-to-back tests.

    partitioned_counters_init();
    status_init();
    txn_status_init();
    toku_checkpoint_init();
    toku_ft_serialize_layer_init();
    toku_mutex_init(&ft_open_close_lock, NULL);
exit:
    return r;
}
#endif

void toku_ft_layer_destroy(void) {
    toku_mutex_destroy(&ft_open_close_lock);
    toku_ft_serialize_layer_destroy();
    toku_checkpoint_destroy();
    status_destroy();
    txn_status_destroy();
    partitioned_counters_destroy();
    //Portability must be cleaned up last
    toku_portability_destroy();
}

// This lock serializes all opens and closes because the cachetable requires that clients do not try to open or close a cachefile in parallel.  We made
// it coarser by not allowing any cachefiles to be open or closed in parallel.
void toku_ft_open_close_lock(void) {
    toku_mutex_lock(&ft_open_close_lock);
}

void toku_ft_open_close_unlock(void) {
    toku_mutex_unlock(&ft_open_close_lock);
}

// Prepare to remove a dictionary from the database when this transaction is committed:
//  - mark transaction as NEED fsync on commit
//  - make entry in rollback log
//  - make fdelete entry in recovery log
//
// Effect: when the txn commits, the ft's cachefile will be marked as unlink
//         on close. see toku_commit_fdelete and how unlink on close works
//         in toku_cachefile_close();
// Requires: serialized with begin checkpoint
//           this does not need to take the open close lock because
//           1.) the ft/cf cannot go away because we have a live handle.
//           2.) we're not setting the unlink on close bit _here_. that
//           happens on txn commit (as the name suggests).
//           3.) we're already holding the multi operation lock to
//           synchronize with begin checkpoint.
// Contract: the iname of the ft should never be reused.
void toku_ft_unlink_on_commit(FT_HANDLE handle, TOKUTXN txn) {
    assert(txn);

    CACHEFILE cf = handle->ft->cf;
    FT CAST_FROM_VOIDP(ft, toku_cachefile_get_userdata(cf));

    toku_txn_maybe_note_ft(txn, ft);

    // If the txn commits, the commit MUST be in the log before the file is actually unlinked
    toku_txn_force_fsync_on_commit(txn);
    // make entry in rollback log
    FILENUM filenum = toku_cachefile_filenum(cf);
    toku_logger_save_rollback_fdelete(txn, filenum);
    // make entry in recovery log
    toku_logger_log_fdelete(txn, filenum);
}

// Non-transactional version of fdelete
//
// Effect: The ft file is unlinked when the handle closes and it's ft is not
//         pinned by checkpoint. see toku_remove_ft_ref() and how unlink on
//         close works in toku_cachefile_close();
// Requires: serialized with begin checkpoint
void toku_ft_unlink(FT_HANDLE handle) {
    CACHEFILE cf;
    cf = handle->ft->cf;
}

int
toku_ft_get_fragmentation(FT_HANDLE brt, TOKU_DB_FRAGMENTATION report) {
    int r;

    int fd = toku_cachefile_get_fd(brt->ft->cf);
    toku_ft_lock(brt->ft);

    int64_t file_size;
    r = toku_os_get_file_size(fd, &file_size);
    if (r==0) {
        report->file_size_bytes = file_size;
        toku_block_table_get_fragmentation_unlocked(brt->ft->blocktable, report);
    }
    toku_ft_unlock(brt->ft);
    return r;
}

static bool is_empty_fast_iter (FT_HANDLE brt, FTNODE node) {
    if (node->height > 0) {
        for (int childnum=0; childnum<node->n_children; childnum++) {
            if (toku_bnc_nbytesinbuf(BNC(node, childnum)) != 0) {
                return 0; // it's not empty if there are bytes in buffers
            }
            FTNODE childnode;
            {
                BLOCKNUM childblocknum = BP_BLOCKNUM(node,childnum);
                uint32_t fullhash =  compute_child_fullhash(brt->ft->cf, node, childnum);
                struct ftnode_fetch_extra bfe;
                fill_bfe_for_full_read(&bfe, brt->ft);
                // don't need to pass in dependent nodes as we are not
                // modifying nodes we are pinning
                toku_pin_ftnode_off_client_thread(
                    brt->ft,
                    childblocknum,
                    fullhash,
                    &bfe,
                    PL_READ, // may_modify_node set to false, as nodes not modified
                    0,
                    NULL,
                    &childnode
                    );
            }
            int child_is_empty = is_empty_fast_iter(brt, childnode);
            toku_unpin_ftnode(brt->ft, childnode);
            if (!child_is_empty) return 0;
        }
        return 1;
    } else {
        // leaf:  If the omt is empty, we are happy.
        for (int i = 0; i < node->n_children; i++) {
            if (BLB_DATA(node, i)->omt_size()) {
                return false;
            }
        }
        return true;
    }
}

bool toku_ft_is_empty_fast (FT_HANDLE brt)
// A fast check to see if the tree is empty.  If there are any messages or leafentries, we consider the tree to be nonempty.  It's possible that those
// messages and leafentries would all optimize away and that the tree is empty, but we'll say it is nonempty.
{
    uint32_t fullhash;
    FTNODE node;
    {
        CACHEKEY root_key;
        toku_calculate_root_offset_pointer(brt->ft, &root_key, &fullhash);
        struct ftnode_fetch_extra bfe;
        fill_bfe_for_full_read(&bfe, brt->ft);
        toku_pin_ftnode_off_client_thread(
            brt->ft,
            root_key,
            fullhash,
            &bfe,
            PL_READ, // may_modify_node set to false, node does not change
            0,
            NULL,
            &node
            );
    }
    bool r = is_empty_fast_iter(brt, node);
    toku_unpin_ftnode(brt->ft, node);
    return r;
}

// test-only
int toku_ft_strerror_r(int error, char *buf, size_t buflen)
{
    if (error>=0) {
        return (long) strerror_r(error, buf, buflen);
    } else {
        switch (error) {
        case DB_KEYEXIST:
            snprintf(buf, buflen, "Key exists");
            return 0;
        case TOKUDB_CANCELED:
            snprintf(buf, buflen, "User canceled operation");
            return 0;
        default:
            snprintf(buf, buflen, "Unknown error %d", error);
            return EINVAL;
        }
    }
}

#include <toku_race_tools.h>
void __attribute__((__constructor__)) toku_ft_helgrind_ignore(void);
void
toku_ft_helgrind_ignore(void) {
    TOKU_VALGRIND_HG_DISABLE_CHECKING(&ft_status, sizeof ft_status);
}

#undef STATUS_INC
