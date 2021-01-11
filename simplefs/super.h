#ifndef __SUPER_HEADER__
#define __SUPER_HEADER__

/* super block definition */
struct sfs_super_block {
	uint64_t version;
	uint64_t magic;
	uint64_t sfs_block_size;
	uint64_t inodes_count;
	uint64_t nr_sector_per_blk;
	uint64_t reserved;
	char padding[4048];
};

#ifdef __KERNEL__
struct sfs_dio_data {
	struct list_head data;
	int nr_items;
	struct mutex lock;
};
#endif /* __KERNEL__ */

struct sfs_inode {
	void *dio;
	uint64_t start_block_number;
	uint64_t block_count;
	mode_t mode;
	uint8_t dir_children_count;
	uint8_t inode_no;
};

/* The dir entry struct */
#define SFS_FILENAME_MAXLEN 28
struct sfs_dir_entry {
	char filename[SFS_FILENAME_MAXLEN];
	uint32_t inode_no;
};

#ifdef __KERNEL__
/* Used by kernel only */
static inline struct sfs_super_block *SFS_SB(struct super_block *sb)
{
	return sb->s_fs_info;
}

static inline struct sfs_inode *SFS_INODE(struct inode *inode)
{
	return inode->i_private;
}
#endif /* __KERNEL__ */

#endif /* __SUPER_HEADER__ */
