/* This will be a empty file if FT_INDIRECT is not defined */
#ifdef FT_INDIRECT
#include "ft-internal.h"
#include "log-internal.h"
#include <compress.h>
#include <portability/toku_atomic.h>
#include <util/sort.h>
#include <util/threadpool.h>
#include "ft.h"
#include <util/status.h>

#include "ule-internal.h"

// There are two layouts for the on-disk format of a node:
// Martin's layout is used if MARTIN_LAYOUT is defined.
// Martin's layout is like below:
// ________________________________________________________________________
// | header | partition 1 | partition 2 | partition 3 | ... | partition n |
// |________|_____________|_____________|_____________|_____|_____________|
//
// where each partition is a collection of keys and values like this:
// ______________________________________________________________
// |<key, indirect_value> pairs | 4kb values | odd-sized values |
// |____________________________|____________|__________________|
//
// Yizheng's layout:
// ___________________________________________________________________________________________________________
// | header | <key, indirect value> pairs for all partitions | p1's data | p2's data | p3's | ... | pn's data |
// |________|________________________________________________|___________|___________|______|_____|___________|
//
// where each partition's is like this:
// _________________________________
// | 4kb values | odd-sized values |
// |____________|__________________|
// The odd-sized values are packed together and the total length is round
// up to multiple of 4 kb.
//
//

// encode start offset and length and checksum of each partition's page data
#define PT_PAGE_DATA_LOCATION_SIZE 8
uint32_t serialize_node_header_increased_size_with_indirect(FTNODE node) {
    return node->n_children * PT_PAGE_DATA_LOCATION_SIZE + 4;
}

void serialize_node_header_page_data(FTNODE node, FTNODE_DISK_DATA ndd, struct wbuf *wbuf) {
    for (int i=0; i<node->n_children; i++) {
        // save the beginning of page data of the partition
        wbuf_nocrc_uint32_t(wbuf, BP_PAGE_DATA_START(ndd, i));
        // and the size
        wbuf_nocrc_uint32_t(wbuf, BP_PAGE_DATA_SIZE (ndd, i));
    }
}

uint32_t serialize_ftnode_partition_size_no_pages(FTNODE node, int i)
{
    return serialize_ftnode_partition_size(node, i);
}

#define FTNODE_PARTITION_OMT_LEAVES 0xaa
#define FTNODE_PARTITION_FIFO_MSG 0xbb

int serialize_ubi_msg(FT_MSG msg, bool UU(is_fresh), struct wbuf *wb) {
    assert(ft_msg_get_type(msg) == FT_UNBOUND_INSERT);
    assert(wb->ind_data);
    struct ftfs_indirect_val *ind_val_ptr = (struct ftfs_indirect_val*)ft_msg_get_val(msg);
    /* copy pfn from message value */
    unsigned int size = ind_val_ptr->size;
    unsigned long pfn = ind_val_ptr->pfn;
    assert(size <= FTFS_PAGE_SIZE);
    WB_COPY_PFN(wb, pfn);
    WB_PFN_CNT(wb) += 1;
    return 0;
}

uint32_t get_leafentry_ind_data_size(LEAFENTRY le) {
    uint32_t ind_data_size = 0;
    int num_ubi = le->num_indirect_inserts;

    for (int i = 0 ; num_ubi > 0 && i < LE_NUM_VALS(le); i++) {
        unsigned int size;
        if (le->indirect_insert_offsets[i] > 0) {
            struct ftfs_indirect_val* val_ptr = (struct ftfs_indirect_val *)((uint8_t *)le + le->indirect_insert_offsets[i]);
            size = val_ptr->size;
            ind_data_size += size;
            num_ubi--;
        }
    }
    return ind_data_size;
}

/////////////////////////////////////////////
/////////////////////////////////////////////
/////////////////////////////////////////////
void compress_ftnode_sub_block_with_indirect(struct sub_block *sb,
                                             enum toku_compression_method method,
                                             bool is_locked, int height)
{
    assert(sb->compressed_ptr == NULL);
    uint32_t index_size = sb->uncompressed_size;
    uint32_t total_size = index_size;
    unsigned int nr_pages = 0;
    unsigned char *uncompressed_data_buffer;
    uint32_t pos = 0;

    if (sb->ind_data) {
        nr_pages = sb->ind_data->pfn_cnt;
        total_size = index_size + nr_pages * FTFS_PAGE_SIZE;
        XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, total_size, uncompressed_data_buffer);
        //uncompressed_data_buffer = (unsigned char *)toku_xmalloc(total_size);
        memcpy(uncompressed_data_buffer, sb->uncompressed_ptr, index_size);
    } else {
        uncompressed_data_buffer = (unsigned char *)sb->uncompressed_ptr;
    }

    pos += index_size;

    if (sb->ind_data) {
        for (int i=0; i < nr_pages; i++) {
            unsigned long pfn = sb->ind_data->pfns[i];
            // is_locked is passed to tell that the page is already locked
            // by itself. Don't wait for it being unlocked
            void *src = (void*)sb_map_page_atomic(pfn, is_locked);
            memcpy(uncompressed_data_buffer+pos, src, FTFS_PAGE_SIZE);
            sb_unmap_page_atomic(src);
            // unlock the page when the copy is done
            if (is_locked) {
                if (height == 0) {
                    ftfs_bn_put_page_list(&pfn, 1);
                } else {
                    ftfs_fifo_put_page_list(&pfn, 1);
                }
                ftfs_unlock_page_list_for_clone(&pfn, 1);
            }
            pos  += FTFS_PAGE_SIZE;
        }
    }

    sb->compressed_size_bound = toku_compress_bound(method, total_size);
    unsigned char *tmp = NULL;
    XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, sb->compressed_size_bound+16, tmp);
    sb->compressed_ptr = tmp;
    ///////////////////////////////////////////////////////////
    //////////////////// Compress /////////////////////////////
    ///////////////////////////////////////////////////////////
    Bytef *uncompressed_ptr = (Bytef *) uncompressed_data_buffer;
    Bytef *compressed_ptr = (Bytef *) sb->compressed_ptr + 16;
    uLongf uncompressed_len = total_size;
    uLongf real_compressed_len = sb->compressed_size_bound;
    toku_compress(method,
                  compressed_ptr, &real_compressed_len,
                  uncompressed_ptr, uncompressed_len);
    sb->compressed_size = real_compressed_len;
    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    uint32_t* extra = (uint32_t *)(sb->compressed_ptr);
    extra[0] = toku_htod32(sb->compressed_size);
    extra[1] = toku_htod32(sb->uncompressed_size);
    extra[2] = toku_htod32(nr_pages);
    extra[3] = toku_htod32(0);
    // now checksum the entire thing
    sb->compressed_size += 16; // now add the eight bytes that we saved for the sizes
    sb->xsum = x1764_memory(sb->compressed_ptr, sb->compressed_size);
    if (sb->ind_data) {
        toku_free(uncompressed_data_buffer);
    }
}

int toku_read_bp_page_from_uncompressed_buf(int nr_pages, int index_size,
                                            FTNODE node, sub_block *curr_sb,
                                            char *bp_data);

int ft_ind_decompress_sub_block(FTNODE UU(node), struct rbuf *rb,
                         struct sub_block *sb, int *page_data_len) {
    int r = 0;
    uint32_t nr_pages = 0;
    uint32_t odd_size = 0;
    uint32_t index_size = 0;
    uint32_t compressed_size = 0;
    unsigned char *compressed_ptr;

    compressed_size = rbuf_int(rb);
    index_size = rbuf_int(rb);
    nr_pages = rbuf_int(rb);
    odd_size = rbuf_int(rb);
    assert(odd_size == 0);
    // When decompress_sub_block is called from toku_deserialize_bp_from_compressed
    // we don't want to the code below because sb->compressed_ptr is the same with
    // rb->buf.
    if (sb->compressed_ptr != rb->buf) {
        bytevec* cp = (bytevec*)&sb->compressed_ptr;
        rbuf_literal_bytes(rb, cp, compressed_size);
        compressed_ptr = (unsigned char*)sb->compressed_ptr;
    } else {
        compressed_ptr = (uint8_t*) sb->compressed_ptr + 16;
    }

    uint32_t total_size = index_size + nr_pages * FTFS_PAGE_SIZE;
    char *bp_data = (char *) sb_malloc_sized(total_size, true);

    toku_decompress(
        (Bytef *) bp_data,
        total_size,
        (Bytef *) compressed_ptr,
        compressed_size
        );

    assert(index_size > 0);
    sb->uncompressed_ptr = sb_malloc_sized(index_size, true);
    memcpy(sb->uncompressed_ptr, bp_data, index_size);
    sb->uncompressed_size = index_size;

    if (nr_pages) {
        *page_data_len = toku_read_bp_page_from_uncompressed_buf(
                                                nr_pages,
                                                index_size,
                                                node,
                                                sb,
                                                bp_data);
    } else {
        sb->read_offset = 0;
        sb->fd = 0;
        sb->ind_data = NULL;
    }

    sb_free_sized(bp_data, total_size);
    return r;
}

static void serialize_partitions_serially(FTNODE node, int npartitions,
                                          enum toku_compression_method UU(compression_method),
                                          struct sub_block sb[], uint8_t *buffer) {
    for (int i = 0; i < npartitions; i++) {
        sb[i].uncompressed_ptr = buffer;
        serialize_ftnode_partition(node, i, &sb[i]);
#ifdef MARTIN_LAYOUT
        buffer += roundup_to_multiple(BLOCK_ALIGNMENT, sb[i].uncompressed_size);
        if (toku_need_compression())
            compress_ftnode_sub_block_with_indirect(&sb[i], compression_method, node->is_cloned, node->height);
#else
        buffer += sb[i].uncompressed_size;
#endif
	}
}

static inline 
void ftnode_add_bp_indirect_info(FTNODE node, int i) {
    if (node->height > 0) {
        node->indirect_insert_full_size += BNC(node,i)->indirect_insert_full_size;
        node->indirect_insert_odd_size += BNC(node,i)->indirect_insert_odd_size;
        node->indirect_insert_full_count += BNC(node,i)->indirect_insert_full_count;
        node->indirect_insert_odd_count += BNC(node,i)->indirect_insert_odd_count;
        node->nonleaf_fifo_entries += toku_bnc_n_entries(BNC(node, i));
    } else {
        node->indirect_insert_full_size += BLB(node,i)->indirect_insert_full_size;
        node->indirect_insert_odd_size += BLB(node,i)->indirect_insert_odd_size;
        node->indirect_insert_full_count += BLB(node,i)->indirect_insert_full_count;
        node->indirect_insert_odd_count += BLB(node,i)->indirect_insert_odd_count;
        node->leaf_le_num += BLB_DATA(node,i)->omt_size();
    }
}

static void
fill_ftnode_serialize_page_info(FTNODE node, int npartitions, uint32_t *est_bp_size) {
    assert(node != NULL);
    node->indirect_insert_full_size = 0;
    node->indirect_insert_odd_size = 0;
    node->indirect_insert_full_count = 0;
    node->indirect_insert_odd_count = 0;
    node->leaf_le_num = 0;
    node->nonleaf_fifo_entries = 0;

    for (int i = 0; i < npartitions; i++) {
        ftnode_add_bp_indirect_info(node, i);
        uint32_t bp_size = serialize_ftnode_partition_size_no_pages(node, i);
#ifdef MARTIN_LAYOUT
        *est_bp_size += roundup_to_multiple(BLOCK_ALIGNMENT, bp_size);
#else
        *est_bp_size += bp_size;
#endif
	}
}

void ftfs_print_count(FTNODE node, const char *func, int line) {
    printf("%s(%d):node=%p, node->height=%d, node->thisnodename.b=%lu\n", func, line, node, node->height, node->thisnodename.b);
    for (int i=0; i < node->n_children; i++) {
        if (node->height == 0) {
            BASEMENTNODE bn = BLB(node, i);
            printf("indirect_full_insert_cnt=%d\n", bn->indirect_insert_full_count);
            printf("indirect_full_insert_size=%d\n", bn->indirect_insert_full_size);
            printf("indirect_odd_insert_cnt=%d\n", bn->indirect_insert_odd_count);
            printf("indirect_odd_insert_size=%d\n", bn->indirect_insert_odd_size);
        } else {
            NONLEAF_CHILDINFO bnc = BNC(node, i);
            printf("indirect_full_insert_cnt=%d\n", bnc->indirect_insert_full_count);
            printf("indirect_full_insert_size=%d\n", bnc->indirect_insert_full_size);
            printf("indirect_odd_insert_cnt=%d\n", bnc->indirect_insert_odd_count);
            printf("indirect_odd_insert_size=%d\n", bnc->indirect_insert_odd_size);
        }
    }
}

static void alloc_ftnode_serialize_buf(FTNODE node, struct bp_serialize_extra *extra) {
    assert(node != NULL);
    assert(extra != NULL);
    node->pfn_array = NULL;
    // allocate space for pfns
    
    node->ind_data_arr = NULL;
    if (node->indirect_insert_full_size > 0 || node->indirect_insert_odd_size > 0) {
        XCALLOC_N(node->n_children, extra->bp_pfns_arr);
        XCALLOC_N(node->n_children, extra->bp_cnt_arr);
        XCALLOC_N(node->n_children, node->ind_data_arr);

        if (node->height == 0) {
            XCALLOC_N(node->leaf_le_num * MAX_PAGE_PER_ENTRY, node->pfn_array);
        } else {
            XCALLOC_N(node->nonleaf_fifo_entries * MAX_PAGE_PER_ENTRY, node->pfn_array);
        }
    }

    if (toku_need_compression()) {
        XCALLOC_N(node->n_children+1, extra->bp_compressed_ptr_arr);
        XCALLOC_N(node->n_children+1, extra->bp_compressed_size_arr);
    }

#ifdef MARTIN_LAYOUT
    XCALLOC_N(node->n_children, extra->bp_index_pfns_arr);
    XCALLOC_N(node->n_children, extra->bp_index_cnt_arr);
#endif
}

static void fill_sub_block_for_serialization(FTNODE node, int npartitions,
                                             struct sub_block *sb)
{
    unsigned long *pfn_array_ptr = node->pfn_array;

    for (int i = 0; i < npartitions; i++) {
        sub_block_init(&sb[i]);
        if (node->indirect_insert_full_size > 0 || node->indirect_insert_odd_size > 0) {
            assert(node->ind_data_arr);
            sb[i].ind_data = &node->ind_data_arr[i];
            sb[i].ind_data->pfns = pfn_array_ptr;
            if (node->height > 0) {
                sb[i].ind_data->num_items = toku_bnc_n_entries(BNC(node, i));
            } else {
                sb[i].ind_data->num_items = BLB_DATA(node,i)->omt_size();
            }
            pfn_array_ptr += sb[i].ind_data->num_items * MAX_PAGE_PER_ENTRY;
        }
    }
}

#ifdef MARTIN_LAYOUT
static void update_ndd_for_serialization(
                             FTNODE node, FTNODE_DISK_DATA *ndd,
                             struct sub_block *sb,
                             struct sub_block *sb_node_info,
                             uint32_t *total_node_size_no_ind,
                             uint32_t *total_node_size,
                             struct bp_serialize_extra *extra)
{
    int npartitions = node->n_children;
    uint32_t node_size_so_far = 0;

    node_size_so_far += serialize_node_header_size(node);
    node_size_so_far += sb_node_info->uncompressed_size;
    *total_node_size_no_ind = roundup_to_multiple(BLOCK_ALIGNMENT, node_size_so_far);
    node_size_so_far = *total_node_size_no_ind;

    if (toku_need_compression()) {
        for (int i = 0; i < npartitions; i++) {
            BP_SIZE(*ndd,i) = sb[i].compressed_size; // size
            BP_START(*ndd,i) = node_size_so_far; // offset
            uint32_t aligned_size = roundup_to_multiple(BLOCK_ALIGNMENT, sb[i].compressed_size);
            node_size_so_far += aligned_size;
            extra->bp_compressed_ptr_arr[i+1] = (uint8_t*)sb[i].compressed_ptr;
            extra->bp_compressed_size_arr[i+1] = aligned_size;
        }
    } else {
        for (int i = 0; i < npartitions; i++) {
            BP_SIZE (*ndd,i) = sb[i].uncompressed_size; // size 
            BP_START(*ndd,i) = node_size_so_far; // offset
            uint32_t aligned_size = roundup_to_multiple(BLOCK_ALIGNMENT, sb[i].uncompressed_size);
            node_size_so_far += aligned_size;
            extra->bp_index_cnt_arr[i] = aligned_size / BLOCK_ALIGNMENT;
            *total_node_size_no_ind += aligned_size;

            uint32_t page_size = 0;
            if (sb[i].ind_data) {
                page_size = sb[i].ind_data->pfn_cnt * FTFS_PAGE_SIZE;
            }
            BP_PAGE_DATA_SIZE (*ndd,i) = page_size;
            BP_PAGE_DATA_START(*ndd,i) = node_size_so_far;
            node_size_so_far += page_size;

            if (page_size > 0) {
                extra->bp_pfns_arr[i] = sb[i].ind_data->pfns;
                extra->bp_cnt_arr[i] = sb[i].ind_data->pfn_cnt;
            }
        }
    }
    *total_node_size = roundup_to_multiple(BLOCK_ALIGNMENT, node_size_so_far);
}

#else // MARTIN_LAYOUT

static void update_ndd_for_serialization(FTNODE node, FTNODE_DISK_DATA *ndd,
                                         struct sub_block *sb,
                                         struct sub_block *sb_node_info,
                                         uint32_t *total_node_size_no_ind,
                                         uint32_t *total_node_size,
                                         struct bp_serialize_extra *extra)
{
    int npartitions = node->n_children;

    uint32_t node_size_no_ind = 0;
    node_size_no_ind += serialize_node_header_size(node);
    node_size_no_ind += sb_node_info->uncompressed_size;

    for (int i = 0; i < npartitions; i++) {
        BP_SIZE (*ndd,i) = sb[i].uncompressed_size; // size 
        BP_START(*ndd,i) = node_size_no_ind; // offset
        node_size_no_ind += sb[i].uncompressed_size;
    }
    uint32_t node_size_no_ind_aligned = roundup_to_multiple(BLOCK_ALIGNMENT, node_size_no_ind);
    uint32_t node_size_so_far = node_size_no_ind_aligned;
    // Store offset and len of ubi_vals
    for (int i = 0; i < npartitions; i++) {
        uint32_t page_cnt = 0;
        if (sb[i].ind_data) {
            page_cnt = sb[i].ind_data->pfn_cnt;
        }

        BP_PAGE_DATA_SIZE (*ndd,i) = page_cnt * FTFS_PAGE_SIZE;
        BP_PAGE_DATA_START(*ndd,i) = node_size_so_far;
        node_size_so_far += page_cnt * FTFS_PAGE_SIZE;

        if (page_cnt > 0) {
            extra->bp_pfns_arr[i] = sb[i].ind_data->pfns;
            extra->bp_cnt_arr[i] = sb[i].ind_data->pfn_cnt;
        }
    }
    *total_node_size_no_ind = node_size_no_ind;
    *total_node_size = roundup_to_multiple(BLOCK_ALIGNMENT, node_size_so_far);
}
#endif // MARTIN_LAYOUT

static void serialize_header(FTNODE node, uint8_t *curr_ptr, FTNODE_DISK_DATA *ndd)
{
    struct wbuf wb;
    wbuf_init(&wb, curr_ptr, serialize_node_header_size(node));
    serialize_node_header(node, *ndd, &wb);
    assert(wb.ndone == wb.size);
}


static int toku_serialize_ftnode_to_memory_with_indirect(
      FT h,
      FTNODE node,
      FTNODE_DISK_DATA* ndd,
      unsigned int UU(basementnodesize),
      enum toku_compression_method UU(compression_method),
      bool do_rebalancing,
      bool in_parallel, /* for loader is true; for flush_callback is false */
      size_t *n_bytes_to_write_no_ind, /*out*/
      size_t *n_total_bytes, /*out*/
      char  **bytes_to_write, /*out*/
      struct bp_serialize_extra *extra /*out*/)
{
    toku_assert_entire_node_in_memory(node);
    if (do_rebalancing && node->height == 0) {
        rebalance_ftnode_leaf(h, node, basementnodesize);
    }
    const int npartitions = node->n_children;
    // (1)
    uint32_t est_bp_index_size = 0;
    fill_ftnode_serialize_page_info(node, npartitions, &est_bp_index_size);
    // (2)  
    struct sub_block *XMALLOC_N(npartitions, sb);

    // This code does not require realloc, except as a convenient
    // way to handle being passed a null pointer.
    // Everything in *ndd from 0..npartitions will be overwritten below
    if (*ndd) {
        toku_free(*ndd);
    }
    XMALLOC_N(npartitions, *ndd);

    extra->num_bp = npartitions;
    alloc_ftnode_serialize_buf(node, extra);
    fill_sub_block_for_serialization(node, npartitions, sb);
    // (3) Allocate a contiguous buffer for all partition for header, nodeinfo, partitions.
    uint32_t header_size = serialize_node_header_size(node);
    uint32_t info_size = serialize_ftnode_info_size(node);
    uint32_t est_node_size_no_ind = est_bp_index_size + header_size + info_size;
    uint32_t total_buffer_size_no_ind = roundup_to_multiple(BLOCK_ALIGNMENT, est_node_size_no_ind);
    unsigned char *XMALLOC_N_ALIGNED(BLOCK_ALIGNMENT, total_buffer_size_no_ind, node_buf_no_ind_ptr);
    // (4) Serialize all partition without pages first.
    assert (!in_parallel);
    assert((unsigned long)node_buf_no_ind_ptr % BLOCK_ALIGNMENT == 0);
    uint8_t *partition_start_ptr = node_buf_no_ind_ptr;
#ifdef MARTIN_LAYOUT
    partition_start_ptr += roundup_to_multiple(BLOCK_ALIGNMENT, header_size + info_size);
#else
    partition_start_ptr += (header_size + info_size);
#endif
    serialize_partitions_serially(node, npartitions, compression_method, sb, partition_start_ptr);
    // (5) Serialize nodeinfo. Initial the pointer to the node info
    struct sub_block sb_node_info;
    sub_block_init(&sb_node_info);
    sb_node_info.uncompressed_ptr = node_buf_no_ind_ptr + header_size;
    serialize_ftnode_info(node, &sb_node_info);
    // (6) Update ndd information.
    uint32_t total_node_size_no_ind = 0, total_node_size = 0;
    update_ndd_for_serialization(node, ndd, sb,
                                     &sb_node_info,
                                     &total_node_size_no_ind,
                                     &total_node_size,
                                     extra);
    uint32_t total_node_size_no_ind_aligned = roundup_to_multiple(BLOCK_ALIGNMENT, total_node_size_no_ind);
    assert(total_node_size_no_ind_aligned <= total_buffer_size_no_ind);
    // (7) Serialize the header to the begining of the buffer.
    node->node_info_size = info_size;
    assert(info_size == sb_node_info.uncompressed_size);
    serialize_header(node, node_buf_no_ind_ptr, ndd);
    // Zero out remaining bytes
    for (uint32_t i=total_node_size_no_ind; i<total_node_size_no_ind_aligned; i++) {
        *(node_buf_no_ind_ptr + i) = 0;
    }
    // (8) Set the output arguments
    // start pointer of serialized node
    *bytes_to_write = (char *)node_buf_no_ind_ptr;
    // node header + node info + partitions (page aligned size)
    *n_bytes_to_write_no_ind = total_node_size_no_ind_aligned;
    // total serialized size includeing page data
    *n_total_bytes = roundup_to_multiple(BLOCK_ALIGNMENT, total_node_size);
    assert(0 == (*n_total_bytes) % BLOCK_ALIGNMENT);
    assert(0 == (*n_bytes_to_write_no_ind) % BLOCK_ALIGNMENT);
    toku_free(sb);
    return 0;
}

int
toku_serialize_ftnode_to_with_indirect(int fd, BLOCKNUM blocknum,
                                       FTNODE node, FTNODE_DISK_DATA* ndd,
                                       bool do_rebalancing, FT h,
                                       bool for_checkpoint,
                                       DISKOFF *p_size, DISKOFF *p_offset,
                                       bool UU(is_blocking)) {

    size_t n_to_write_no_pages;
    size_t n_bytes_with_pages;
    char *ftnode_buf_no_pages = nullptr;
    struct bp_serialize_extra bp_extra;

    memset(&bp_extra, 0, sizeof(bp_extra));
    bp_extra.fd = fd;

    int r = toku_serialize_ftnode_to_memory_with_indirect(
	h,
        node,
        ndd,
        h->h->basementnodesize,
        h->h->compression_method,
        do_rebalancing,
        false, // in_parallel
        &n_to_write_no_pages,
        &n_bytes_with_pages,
        &ftnode_buf_no_pages,
        &bp_extra);
    if (r != 0) {
        return r;
    }

    // If the node has never been written, then write the whole buffer, including the zeros
    invariant(blocknum.b>=0);
    DISKOFF offset;
    size_t n_to_write_with_pages = n_bytes_with_pages;
    toku_blocknum_realloc_on_disk(h->blocktable, blocknum, n_to_write_with_pages, &offset, h, fd, for_checkpoint); //dirties h
    assert(blocknum.b != RESERVED_BLOCKNUM_TRANSLATION);
    assert(offset % BLOCK_ALIGNMENT == 0);

    unsigned int header_and_info_size =  node->node_info_size + serialize_node_header_size(node);
    unsigned int aligned_size = roundup_to_multiple(BLOCK_ALIGNMENT, header_and_info_size);
    if (toku_need_compression()) {
        int pos = 0;
        assert(n_to_write_with_pages % BLOCK_ALIGNMENT == 0);
        char *node_data = (char *) sb_malloc_sized(n_to_write_with_pages, true);

        // copy the header and free the original buffer
        {
            memcpy(node_data, ftnode_buf_no_pages, n_to_write_no_pages);
            pos += n_to_write_no_pages;
            toku_free(ftnode_buf_no_pages);
        }
        // copy each partition and free the original buffer
        for (int i = 0; i < node->n_children && pos < n_bytes_with_pages; i++) {
            memcpy(node_data+pos, bp_extra.bp_compressed_ptr_arr[i+1], bp_extra.bp_compressed_size_arr[i+1]);
            pos += bp_extra.bp_compressed_size_arr[i+1];
            toku_free(bp_extra.bp_compressed_ptr_arr[i+1]);
        }
        // write the node and free the node buffer
        assert(n_bytes_with_pages == pos);
        toku_os_full_pwrite(fd, node_data, n_to_write_with_pages, offset, true);
        sb_free_sized(node_data, n_to_write_with_pages);

        toku_free(node->pfn_array);
        toku_free(node->ind_data_arr);
        toku_free(bp_extra.bp_compressed_ptr_arr);
        toku_free(bp_extra.bp_compressed_size_arr);
        toku_free(bp_extra.bp_index_pfns_arr);
        toku_free(bp_extra.bp_index_cnt_arr);
        toku_free(bp_extra.bp_pfns_arr);
        toku_free(bp_extra.bp_cnt_arr);
        // Must set the node to be clean after serializing it
        // so that it doesn't get written again on the next checkpoint or eviction.
        node->dirty = 0;
        if (p_size && p_offset) {
            *p_size = n_to_write_with_pages;
            *p_offset = offset;
        }
        return 0;
    }

    unsigned int nr_pages = n_to_write_no_pages / BLOCK_ALIGNMENT;
    unsigned long * XMALLOC_N(nr_pages, pfn_arr);
    for (int i = 0; i < nr_pages; i++) {
         unsigned long addr = (unsigned long)ftnode_buf_no_pages + i * BLOCK_ALIGNMENT;
         pfn_arr[i] = get_kernfs_pfn(addr);
    }
    unsigned int nr_pages_header_info = 0;
#ifdef MARTIN_LAYOUT
    nr_pages_header_info = aligned_size / BLOCK_ALIGNMENT;
    // Part 1: node header pfns
    bp_extra.begin_pfns = pfn_arr;
    bp_extra.begin_pfn_cnt = nr_pages_header_info;
    // Part 2: k-v part pfns
    uint32_t nr_pages_so_far = nr_pages_header_info;
    uint32_t bp_index_pages = 0;
    for (int i = 0; i < node->n_children; i++) {
         bp_extra.bp_index_pfns_arr[i] = pfn_arr + nr_pages_so_far;
         bp_index_pages += bp_extra.bp_index_cnt_arr[i];
         nr_pages_so_far += bp_extra.bp_index_cnt_arr[i];
    }
    assert(bp_index_pages + nr_pages_header_info == nr_pages);
#else // MARTIN_LAYOUT
    bp_extra.begin_pfns = pfn_arr;
    bp_extra.begin_pfn_cnt = nr_pages;
#endif // MARTIN_LAYOUT

    if (node->height == 0) {
        ftfs_write_ft_leaf(&bp_extra, offset, node->is_cloned);
    } else {
        ftfs_write_ft_nonleaf(&bp_extra, offset, node->is_cloned);
    }

    for (int i = 0; i < nr_pages; i++) {
        ftfs_wait_reserved_page(pfn_arr[i]);
    }

    toku_free(pfn_arr);
    toku_free(ftnode_buf_no_pages);

    // YZJ: Wait for the IO of pages to complete
    if (bp_extra.bp_pfns_arr && bp_extra.bp_cnt_arr) {
        for (int p=0; p < node->n_children; p++) {
             unsigned long *pfns = bp_extra.bp_pfns_arr[p];
             int num_pages = bp_extra.bp_cnt_arr[p];
             for (int j=0; j < num_pages; j++) {
                ftfs_wait_reserved_page(pfns[j]);
             }
        }
    }

    toku_free(node->pfn_array);
    toku_free(node->ind_data_arr);
    toku_free(bp_extra.bp_index_pfns_arr);
    toku_free(bp_extra.bp_index_cnt_arr);
    toku_free(bp_extra.bp_pfns_arr);
    toku_free(bp_extra.bp_cnt_arr);
    // Must set the node to be clean after serializing it
    // so that it doesn't get written again on the next checkpoint or eviction.
    node->dirty = 0;
    if (p_size && p_offset) {
        *p_size = n_to_write_with_pages;
        *p_offset = offset;
    }
    return 0;
}

//////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////
struct bn_free_args {
    int count;
    bool is_cloned;
}; 

static int
free_leafentry_page(const void* UU(key), const uint32_t UU(keylen),
                    const LEAFENTRY &le, const uint32_t UU(idx),
                    struct bn_free_args *args) {
    if (le->num_indirect_inserts == 0) {
        return 0;
    }

    struct ftfs_indirect_val *msg;
    unsigned int size;
    unsigned long pfn;
    int num_ubi = le->num_indirect_inserts;
    bool is_cloned = args->is_cloned;

    /* XXX: Only handle the pages when the node is not a cloned one */
    for (int i = 0 ; num_ubi > 0 && i < LE_NUM_VALS(le) && !is_cloned; i++) {
        if (le->indirect_insert_offsets[i] > 0) {
            msg = (struct ftfs_indirect_val *) ((uint8_t *)le + le->indirect_insert_offsets[i]);
            size = msg->size;
            pfn = msg->pfn;
            assert(size <= FTFS_PAGE_SIZE);
            if (!is_cloned) {
                /* The bn may be the last user */
                ftfs_set_page_list_private(&pfn, 1, FT_MSG_VAL_BN_PAGE_FREE);
                ftfs_free_page_list(&pfn, 1, true);
            }
            args->count += 1;
            num_ubi--;
         }
    }
    /* XXX: For cloned and non-cloned node, we need to the buffer for offset */
    if (le->num_indirect_inserts > 0) {
         toku_free(le->indirect_insert_offsets);
    }
    return 0;
}

void basement_node_free_pages(BASEMENTNODE bn, bool is_cloned) {
    bool has_ind_data = (bn->indirect_insert_full_size > 0 || bn->indirect_insert_odd_size > 0);
    if (has_ind_data && bn->data_buffer.omt_size() > 0) {
        struct bn_free_args args = { .count = 0, .is_cloned = is_cloned };
        bn->data_buffer.omt_iterate<struct bn_free_args, free_leafentry_page>(&args);
    }
}

struct ftfs_fifo_iter_page {
     int count; /* output */
};

/* Only called by for node that is not cloned. The page is already
 * unlocked when entering into this function. For cloned node,
 * this function is not called because the flag and the ref count
 * is handled in the block IO callback function --- Check ftnode_page_end_io
 */
static int destroy_msg_fn(FT_MSG msg, bool UU(is_fresh), void* _args) {
    struct ftfs_fifo_iter_page* args = (struct ftfs_fifo_iter_page*) _args;
    if (ft_msg_get_type(msg) == FT_UNBOUND_INSERT) {
        struct ftfs_indirect_val *val = (struct ftfs_indirect_val *)ft_msg_get_val(msg);
        assert(val->size <= FTFS_PAGE_SIZE);
        ftfs_set_page_list_private(&val->pfn, 1, FT_MSG_VAL_NL_PAGE_FREE);
        ftfs_free_page_list(&val->pfn, 1, false);
        args->count += 1;
    }
    return 0;
}

/* Only called by in clone_callback and ftnode_page_end_io handles unlock
 * and reset the flag (Private2)
 */
static int get_page_msg_fn(FT_MSG msg, bool UU(is_fresh), void* _args) {
    struct ftfs_fifo_iter_page* args = (struct ftfs_fifo_iter_page*) _args;
    if (ft_msg_get_type(msg) == FT_UNBOUND_INSERT) {
        struct ftfs_indirect_val *val = (struct ftfs_indirect_val *)ft_msg_get_val(msg);
        assert(val->size <= FTFS_PAGE_SIZE);
        ftfs_lock_page_list_for_clone(&val->pfn, 1);
        ftfs_set_page_list_private(&val->pfn, 1, FT_MSG_VAL_FOR_CLONE);
        ftfs_fifo_get_page_list(&val->pfn, 1);
        args->count += 1;
    }
    return 0;
}

/* for cloned node during flush callback*/
void toku_nonleaf_get_page_list(NONLEAF_CHILDINFO nl) {
    struct ftfs_fifo_iter_page get_page_args;
    get_page_args.count = 0;
    toku_fifo_iterate(nl->buffer, get_page_msg_fn, &get_page_args);
}

/* For destroy a normal node */
void destroy_nonleaf_ubi_vals(NONLEAF_CHILDINFO nl) {
    struct ftfs_fifo_iter_page destroy_args;
    destroy_args.count = 0;
    toku_fifo_iterate(nl->buffer, destroy_msg_fn, &destroy_args);
}

int toku_read_bp_page(FTNODE node, FTNODE_DISK_DATA ndd, int childnum,
                      sub_block *curr_sb, DISKOFF node_offset, int fd)
{
    uint32_t nr_pages = BP_PAGE_DATA_SIZE(ndd, childnum) >> FTFS_PAGE_SIZE_SHIFT;
    if (nr_pages > 0) {
        XMALLOC_N(1, curr_sb->ind_data);
        curr_sb->ind_data->pfn_cnt = nr_pages;
        XMALLOC_N(nr_pages, curr_sb->ind_data->pfns);
        for (int p = 0; p < nr_pages; p++) {
            if (node->height == 0)
                curr_sb->ind_data->pfns[p] =  sb_alloc_leaf_page();
            else 
                curr_sb->ind_data->pfns[p] =  sb_alloc_nonleaf_page();
        }
        uint32_t curr_ubi_offset = BP_PAGE_DATA_START(ndd, childnum);
        unsigned long long pt_ubi_vals_offset = node_offset + curr_ubi_offset;
        assert(pt_ubi_vals_offset % FTFS_PAGE_SIZE == 0);
        curr_sb->read_offset = pt_ubi_vals_offset;
        curr_sb->fd = fd;
        sb_async_read_pages(curr_sb->fd, curr_sb->ind_data->pfns, nr_pages, curr_sb->read_offset);
    } else {
        curr_sb->read_offset = 0;
        curr_sb->fd = 0;
        curr_sb->ind_data = NULL;
    }
    return nr_pages << FTFS_PAGE_SIZE_SHIFT;
}

int toku_read_bp_page_from_uncompressed_buf(int nr_pages, int index_size,
                                            FTNODE node, sub_block *curr_sb,
                                            char *bp_data)
{
    if (nr_pages > 0) {
        XMALLOC_N(1, curr_sb->ind_data);
        curr_sb->ind_data->pfn_cnt = nr_pages;
        XMALLOC_N(nr_pages, curr_sb->ind_data->pfns);
        for (int p = 0; p < nr_pages; p++) {
            if (node->height == 0)
                curr_sb->ind_data->pfns[p] = sb_alloc_leaf_page();
            else
                curr_sb->ind_data->pfns[p] = sb_alloc_nonleaf_page();
        }
        char *page_data_ptr = bp_data + index_size;
        for (int i = 0; i < nr_pages; i++) {
            char *buf = sb_map_page_atomic(curr_sb->ind_data->pfns[i], false);
            memcpy(buf, page_data_ptr, FTFS_PAGE_SIZE);
            sb_unmap_page_atomic(buf);
            ftfs_set_page_up_to_date(curr_sb->ind_data->pfns[i]);
            page_data_ptr += FTFS_PAGE_SIZE;
        }
    } else {
        curr_sb->read_offset = 0;
        curr_sb->fd = 0;
        curr_sb->ind_data = NULL;
    }
    return nr_pages << FTFS_PAGE_SIZE_SHIFT;
}

int get_node_info_from_rbuf(struct sub_block *sb_node_info, struct rbuf *rb, uint32_t node_info_size) {
    sb_node_info->uncompressed_ptr = rb->buf + rb->ndone;
    sb_node_info->uncompressed_size = node_info_size;
    if (rb->size - rb->ndone < sb_node_info->uncompressed_size) {
        printf("%s: cannot read node info because it is too big:%d\n", __func__, node_info_size);
        assert(false);
    }
    // move to the location of checksum and read it
    rb->ndone += (node_info_size -4);
    sb_node_info->xsum = rbuf_int(rb);
    // let's check the checksum
    uint32_t actual_xsum;
    // Reason for minus 4 -- xsum itself should not included
    actual_xsum = x1764_memory((char *)sb_node_info->uncompressed_ptr, node_info_size - 4);
    if (sb_node_info->xsum != actual_xsum) {
        printf("%s: Checksum is wrong\n", __func__);
        assert(false);
    }
    return 0;
}
#endif /* FT_INDIRECT */
