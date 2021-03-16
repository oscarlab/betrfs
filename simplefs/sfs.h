#ifndef __SFS_HEADER__
#define __SFS_HEADER__

#define SFS_MAGIC 0x20180130
#define SFS_DEFAULT_BLOCK_SIZE 4096
#define SFS_DEFAULT_BLOCK_BITS 12
#define SFS_DEFAULT_MAX_BYTES (512*1024*1024*1024L)
#define SFS_NAME_LEN 255

/* Hard-coded inode number for the root dir */
const uint64_t SFS_ROOTDIR_INODE_NUMBER = 1;

/* The disk block for the super block */
const uint64_t SFS_SUPERBLOCK_BLOCK_NUMBER = 0;

/* The disk block for the inode table */
const uint64_t SFS_INODETABLE_BLOCK_NUMBER = 1;

/* The disk block where the dir entry of root directory */
#define SFS_ROOTDIR_DATABLOCK_NUMBER 2

/* The Start Point of Unreserved Blocks */
#define SFS_LAST_RESERVED_BLOCK SFS_ROOTDIR_DATABLOCK_NUMBER

/* sfs inode flags */
#define SFS_INODE_CREATE 1

#ifdef __KERNEL__
#include <linux/aio.h>
#include <linux/pagevec.h>
ssize_t sfs_in_kernel_write(struct kiocb *iocb,
		const struct iovec *iov,
		unsigned long nr_segs, loff_t pos);

int sfs_generic_write_end(struct file *file,
		struct address_space *mapping,
		loff_t pos, unsigned len, unsigned copied,
		struct page *page, void *fsdata);

int sfs_block_write_begin(struct address_space *mapping,
		loff_t pos, unsigned len, unsigned flags,
		struct page **pagep, get_block_t *get_block);

bool sfs_is_inode_log(struct inode *inode);
void sfs_print_inode_iblock(struct inode *inode, sector_t iblock);
void sfs_dump_write_begin_page(struct page *page);
bool sfs_is_dictionary_file(struct file *filp);
#endif

#endif /* __SFS_HEADER__ */
