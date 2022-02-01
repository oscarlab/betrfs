/*
 * fs/f2fs/node.h
 *
 * Copyright (c) 2012 Samsung Electronics Co., Ltd.
 *             http://www.samsung.com/
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
/* start node id of a node block dedicated to the given node id */
#define	START_NID(nid) ((nid / NAT_ENTRY_PER_BLOCK) * NAT_ENTRY_PER_BLOCK)

/* node block offset on the NAT area dedicated to the given start node id */
#define	NAT_BLOCK_OFFSET(start_nid) (start_nid / NAT_ENTRY_PER_BLOCK)

/* # of pages to perform readahead before building free nids */
#define FREE_NID_PAGES 4

/* maximum # of free node ids to produce during build_free_nids */
#define MAX_FREE_NIDS (NAT_ENTRY_PER_BLOCK * FREE_NID_PAGES)

/* maximum readahead size for node during getting data blocks */
#define MAX_RA_NODE		128

/* maximum cached nat entries to manage memory footprint */
#define NM_WOUT_THRESHOLD	(64 * NAT_ENTRY_PER_BLOCK)

/* vector size for gang look-up from nat cache that consists of radix tree */
#define NATVEC_SIZE	64

/* return value for read_node_page */
#define LOCKED_PAGE	1

/*
 * For node information
 */
struct node_info {
	nid_t nid;		/* node id */
	nid_t ino;		/* inode number of the node's owner */
	block_t	blk_addr;	/* block address of the node */
	unsigned char version;	/* version of the node */
};

struct nat_entry {
	struct list_head list;	/* for clean or dirty nat list */
	bool checkpointed;	/* whether it is checkpointed or not */
	struct node_info ni;	/* in-memory node information */
};

#define nat_get_nid(nat)		(nat->ni.nid)
#define nat_set_nid(nat, n)		(nat->ni.nid = n)
#define nat_get_blkaddr(nat)		(nat->ni.blk_addr)
#define nat_set_blkaddr(nat, b)		(nat->ni.blk_addr = b)
#define nat_get_ino(nat)		(nat->ni.ino)
#define nat_set_ino(nat, i)		(nat->ni.ino = i)
#define nat_get_version(nat)		(nat->ni.version)
#define nat_set_version(nat, v)		(nat->ni.version = v)

#define __set_nat_cache_dirty(nm_i, ne)					\
	list_move_tail(&ne->list, &nm_i->dirty_nat_entries);
#define __clear_nat_cache_dirty(nm_i, ne)				\
	list_move_tail(&ne->list, &nm_i->nat_entries);
#define inc_node_version(version)	(++version)

static inline void node_info_from_raw_nat(struct node_info *ni,
						struct f2fs_nat_entry *raw_ne)
{
	ni->ino = le32_to_cpu(raw_ne->ino);
	ni->blk_addr = le32_to_cpu(raw_ne->block_addr);
	ni->version = raw_ne->version;
}

/*
 * For free nid mangement
 */
enum nid_state {
	NID_NEW,	/* newly added to free nid list */
	NID_ALLOC	/* it is allocated */
};

struct free_nid {
	struct list_head list;	/* for free node id list */
	nid_t nid;		/* node id */
	int state;		/* in use or not: NID_NEW or NID_ALLOC */
};

static inline int next_free_nid(struct f2fs_sb_info *sbi, nid_t *nid)
{
	struct f2fs_nm_info *nm_i = NM_I(sbi);
	struct free_nid *fnid;

	if (nm_i->fcnt <= 0)
		return -1;
	spin_lock(&nm_i->free_nid_list_lock);
	fnid = list_entry(nm_i->free_nid_list.next, struct free_nid, list);
	*nid = fnid->nid;
	spin_unlock(&nm_i->free_nid_list_lock);
	return 0;
}

/*
 * inline functions
 */
static inline void get_nat_bitmap(struct f2fs_sb_info *sbi, void *addr)
{
	struct f2fs_nm_info *nm_i = NM_I(sbi);
	memcpy(addr, nm_i->nat_bitmap, nm_i->bitmap_size);
}

static inline pgoff_t current_nat_addr(struct f2fs_sb_info *sbi, nid_t start)
{
	struct f2fs_nm_info *nm_i = NM_I(sbi);
	pgoff_t block_off;
	pgoff_t block_addr;
	int seg_off;

	block_off = NAT_BLOCK_OFFSET(start);
	seg_off = block_off >> sbi->log_blocks_per_seg;

	block_addr = (pgoff_t)(nm_i->nat_blkaddr +
		(seg_off << sbi->log_blocks_per_seg << 1) +
		(block_off & ((1 << sbi->log_blocks_per_seg) - 1)));

	if (f2fs_test_bit(block_off, nm_i->nat_bitmap))
		block_addr += sbi->blocks_per_seg;

	return block_addr;
}

static inline pgoff_t next_nat_addr(struct f2fs_sb_info *sbi,
						pgoff_t block_addr)
{
	struct f2fs_nm_info *nm_i = NM_I(sbi);

	block_addr -= nm_i->nat_blkaddr;
	if ((block_addr >> sbi->log_blocks_per_seg) % 2)
		block_addr -= sbi->blocks_per_seg;
	else
		block_addr += sbi->blocks_per_seg;

	return block_addr + nm_i->nat_blkaddr;
}

static inline void set_to_next_nat(struct f2fs_nm_info *nm_i, nid_t start_nid)
{
	unsigned int block_off = NAT_BLOCK_OFFSET(start_nid);

	if (f2fs_test_bit(block_off, nm_i->nat_bitmap))
		f2fs_clear_bit(block_off, nm_i->nat_bitmap);
	else
		f2fs_set_bit(block_off, nm_i->nat_bitmap);
}

static inline void fill_node_footer(struct page *page, nid_t nid,
				nid_t ino, unsigned int ofs, bool reset)
{
	void *kaddr = page_address(page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	if (reset)
		memset(rn, 0, sizeof(*rn));
	rn->footer.nid = cpu_to_le32(nid);
	rn->footer.ino = cpu_to_le32(ino);
	rn->footer.flag = cpu_to_le32(ofs << OFFSET_BIT_SHIFT);
}

static inline void copy_node_footer(struct page *dst, struct page *src)
{
	void *src_addr = page_address(src);
	void *dst_addr = page_address(dst);
	struct f2fs_node *src_rn = (struct f2fs_node *)src_addr;
	struct f2fs_node *dst_rn = (struct f2fs_node *)dst_addr;
	memcpy(&dst_rn->footer, &src_rn->footer, sizeof(struct node_footer));
}

static inline void fill_node_footer_blkaddr(struct page *page, block_t blkaddr)
{
	struct f2fs_sb_info *sbi = F2FS_SB(page->mapping->host->i_sb);
	struct f2fs_checkpoint *ckpt = F2FS_CKPT(sbi);
	void *kaddr = page_address(page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	rn->footer.cp_ver = ckpt->checkpoint_ver;
	rn->footer.next_blkaddr = cpu_to_le32(blkaddr);
}

static inline nid_t ino_of_node(struct page *node_page)
{
	void *kaddr = page_address(node_page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	return le32_to_cpu(rn->footer.ino);
}

static inline nid_t nid_of_node(struct page *node_page)
{
	void *kaddr = page_address(node_page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	return le32_to_cpu(rn->footer.nid);
}

static inline unsigned int ofs_of_node(struct page *node_page)
{
	void *kaddr = page_address(node_page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	unsigned flag = le32_to_cpu(rn->footer.flag);
	return flag >> OFFSET_BIT_SHIFT;
}

static inline unsigned long long cpver_of_node(struct page *node_page)
{
	void *kaddr = page_address(node_page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	return le64_to_cpu(rn->footer.cp_ver);
}

static inline block_t next_blkaddr_of_node(struct page *node_page)
{
	void *kaddr = page_address(node_page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	return le32_to_cpu(rn->footer.next_blkaddr);
}

/*
 * f2fs assigns the following node offsets described as (num).
 * N = NIDS_PER_BLOCK
 *
 *  Inode block (0)
 *    |- direct node (1)
 *    |- direct node (2)
 *    |- indirect node (3)
 *    |            `- direct node (4 => 4 + N - 1)
 *    |- indirect node (4 + N)
 *    |            `- direct node (5 + N => 5 + 2N - 1)
 *    `- double indirect node (5 + 2N)
 *                 `- indirect node (6 + 2N)
 *                       `- direct node (x(N + 1))
 */
static inline bool IS_DNODE(struct page *node_page)
{
	unsigned int ofs = ofs_of_node(node_page);
	if (ofs == 3 || ofs == 4 + NIDS_PER_BLOCK ||
			ofs == 5 + 2 * NIDS_PER_BLOCK)
		return false;
	if (ofs >= 6 + 2 * NIDS_PER_BLOCK) {
		ofs -= 6 + 2 * NIDS_PER_BLOCK;
		if (!((long int)ofs % (NIDS_PER_BLOCK + 1)))
			return false;
	}
	return true;
}

static inline void set_nid(struct page *p, int off, nid_t nid, bool i)
{
	struct f2fs_node *rn = (struct f2fs_node *)page_address(p);

	wait_on_page_writeback(p);

	if (i)
		rn->i.i_nid[off - NODE_DIR1_BLOCK] = cpu_to_le32(nid);
	else
		rn->in.nid[off] = cpu_to_le32(nid);
	set_page_dirty(p);
}

static inline nid_t get_nid(struct page *p, int off, bool i)
{
	struct f2fs_node *rn = (struct f2fs_node *)page_address(p);
	if (i)
		return le32_to_cpu(rn->i.i_nid[off - NODE_DIR1_BLOCK]);
	return le32_to_cpu(rn->in.nid[off]);
}

/*
 * Coldness identification:
 *  - Mark cold files in f2fs_inode_info
 *  - Mark cold node blocks in their node footer
 *  - Mark cold data pages in page cache
 */
static inline int is_file(struct inode *inode, int type)
{
	return F2FS_I(inode)->i_advise & type;
}

static inline void set_file(struct inode *inode, int type)
{
	F2FS_I(inode)->i_advise |= type;
}

static inline void clear_file(struct inode *inode, int type)
{
	F2FS_I(inode)->i_advise &= ~type;
}

#define file_is_cold(inode)	is_file(inode, FADVISE_COLD_BIT)
#define file_wrong_pino(inode)	is_file(inode, FADVISE_LOST_PINO_BIT)
#define file_set_cold(inode)	set_file(inode, FADVISE_COLD_BIT)
#define file_lost_pino(inode)	set_file(inode, FADVISE_LOST_PINO_BIT)
#define file_clear_cold(inode)	clear_file(inode, FADVISE_COLD_BIT)
#define file_got_pino(inode)	clear_file(inode, FADVISE_LOST_PINO_BIT)

static inline int is_cold_data(struct page *page)
{
	return PageChecked(page);
}

static inline void set_cold_data(struct page *page)
{
	SetPageChecked(page);
}

static inline void clear_cold_data(struct page *page)
{
	ClearPageChecked(page);
}

static inline int is_node(struct page *page, int type)
{
	void *kaddr = page_address(page);
	struct f2fs_node *rn = (struct f2fs_node *)kaddr;
	return le32_to_cpu(rn->footer.flag) & (1 << type);
}

#define is_cold_node(page)	is_node(page, COLD_BIT_SHIFT)
#define is_fsync_dnode(page)	is_node(page, FSYNC_BIT_SHIFT)
#define is_dent_dnode(page)	is_node(page, DENT_BIT_SHIFT)

static inline void set_cold_node(struct inode *inode, struct page *page)
{
	struct f2fs_node *rn = (struct f2fs_node *)page_address(page);
	unsigned int flag = le32_to_cpu(rn->footer.flag);

	if (S_ISDIR(inode->i_mode))
		flag &= ~(0x1 << COLD_BIT_SHIFT);
	else
		flag |= (0x1 << COLD_BIT_SHIFT);
	rn->footer.flag = cpu_to_le32(flag);
}

static inline void set_mark(struct page *page, int mark, int type)
{
	struct f2fs_node *rn = (struct f2fs_node *)page_address(page);
	unsigned int flag = le32_to_cpu(rn->footer.flag);
	if (mark)
		flag |= (0x1 << type);
	else
		flag &= ~(0x1 << type);
	rn->footer.flag = cpu_to_le32(flag);
}
#define set_dentry_mark(page, mark)	set_mark(page, mark, DENT_BIT_SHIFT)
#define set_fsync_mark(page, mark)	set_mark(page, mark, FSYNC_BIT_SHIFT)
