#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <stdbool.h>
#include <assert.h>
#include <linux/fs.h>

#include "sfs.h"
#include "super.h"
#include "alloc_table.h"

#define ROOT_OBJ_COUNT 3
#define DB_OBJ_COUNT 14
#define DB_INODE_COUNT 14
#define DEV_OBJ_COUNT 1

static int write_superblock(int fd, int total_inodes)
{
	struct sfs_super_block sb = {
		.version = 0x1,
		.magic = SFS_MAGIC,
		.sfs_block_size = SFS_DEFAULT_BLOCK_SIZE,
		.inodes_count = total_inodes,
		.nr_sector_per_blk = 0,
	};

	ssize_t ret;

	ret = write(fd, &sb, sizeof(sb));
	if (ret != SFS_DEFAULT_BLOCK_SIZE) {
		printf("bytes written are not equal\n");
		assert(false);
	}
	printf("(1) super block written successfully\n");
	return 0;
}

static int write_root_inode(int fd, int *total_inodes)
{
	ssize_t ret;
	struct sfs_inode root_inode;

	root_inode.mode = S_IFDIR;
	root_inode.inode_no = SFS_ROOTDIR_INODE_NUMBER;
	root_inode.start_block_number = SFS_ROOTDIR_DATABLOCK_NUMBER;
	root_inode.dir_children_count = ROOT_OBJ_COUNT;
	root_inode.block_count = 1;
	root_inode.dio = 0;
	
	off_t off = lseek(fd, 0, SEEK_CUR);
	ret = write(fd, &root_inode, sizeof(root_inode));

	if (ret != sizeof(root_inode)) {
		printf("the root inode was not written properly\n");
		assert(false);
	}

	*total_inodes += 1;
	printf("(2) root dir inode written successfully\n");
	return 0;
}

static int write_db_dir_inode(int fd, int *total_inodes)
{
	ssize_t ret;
	struct sfs_inode db_inode;

	db_inode.mode = S_IFDIR;
	db_inode.inode_no = FT_DB_DIR_INODE_NUMBER;
	db_inode.start_block_number = FT_DB_DIR_DATABLOCK_NUMBER;
	db_inode.dir_children_count = DB_OBJ_COUNT;
	db_inode.block_count = 1;

	ret = write(fd, &db_inode, sizeof(db_inode));

	if (ret != sizeof(db_inode)) {
		printf("The root inode was not written properly\n");
		assert(false);
	}

	*total_inodes += 1;
	printf("(3) db dir inode written successfully\n");
	return 0;
}

static int write_temp_dir_inode(int fd, int *total_inodes)
{
	ssize_t ret;
	struct sfs_inode temp_inode;

	temp_inode.mode = S_IFDIR;
	temp_inode.inode_no = FT_TEMP_DIR_INODE_NUMBER;
	temp_inode.start_block_number = FT_TEMP_DIR_DATABLOCK_NUMBER;
	temp_inode.dir_children_count = 0;
	temp_inode.block_count = 1;

	ret = write(fd, &temp_inode, sizeof(temp_inode));

	if (ret != sizeof(temp_inode)) {
		printf("The root inode was not written properly\n");
		assert(false);
	}

	*total_inodes += 1;
	printf("(4) temp dir inode written successfully\n");
	return 0;
}

static int write_db_files_inode(int fd, const struct sfs_inode *i,
				int count, int *total_inodes)
{
	off_t nbytes;
	ssize_t ret;

	ret = write(fd, i, sizeof(*i) * count);
	if (ret != sizeof(*i) * count) {
		printf("The DB files inode was not written properly\n");
		assert(false);
	}
	
	*total_inodes += count;
	printf("(5) db files inode written successfully\n");
	return 0;
}

static int sfs_inode_table_lseek(int fd, int total_inode) {

	off_t nbytes = SFS_DEFAULT_BLOCK_SIZE - sizeof(struct sfs_inode) * total_inode;
	int ret;

	ret = lseek(fd, nbytes, SEEK_CUR);
	if (ret == (off_t) -1) {
		printf("The padding bytes are not written properly\n");
		perror("lseek:");
		assert(false);
	}
	printf("(6) inode table padding bytes written sucessfully\n");
	return 0;
}

int write_dirent(int fd, const struct sfs_dir_entry *entry,
		int count, off_t expected_blk_nr)
{
	ssize_t nbytes = sizeof(*entry) * count;
	char buffer[SFS_DEFAULT_BLOCK_SIZE] = {0};
	ssize_t ret;
	off_t offset;

	if (nbytes > SFS_DEFAULT_BLOCK_SIZE) {
		printf("The dentry array is too large\n");
		assert(false);
	}

	offset = lseek(fd, 0, SEEK_CUR);
	if ((offset >> SFS_DEFAULT_BLOCK_BITS) != expected_blk_nr) {
		printf("wrong block=%ld\n", offset >> 12);
		printf("expected_blk_nr=%ld\n", expected_blk_nr);
		assert(false);
	}

	/* copy data to buffer and write it */
	memcpy(buffer, entry, nbytes);
	ret = write(fd, buffer, SFS_DEFAULT_BLOCK_SIZE);
	if (ret != SFS_DEFAULT_BLOCK_SIZE) {
		printf("Writing the root dir entry has failed\n");
		assert(false);
	}
	printf("(7) dir entry written successfully\n");
	return 0;
}

static void zero_out_blocks(int fd, uint64_t block_number, int nr_blocks)
{
	int i = 0;
	uint64_t off = block_number * 4096;
	char buf[4096];
	uint64_t r;

	memset(buf, 0, 4096);
	r = lseek(fd, off, SEEK_SET);
	if (r != off) {
		printf("r=%ld, off=%ld\n", r, off);
		perror("lseek:");
		assert(false);
	}
	for (i = 0; i < nr_blocks; i++) {
		r = write(fd, buf, 4096);
		assert(r == 4096);
	}
}

#define FILL_OUT_INODE(NAME)						\
		{.mode = S_IFREG,					\
		 .inode_no = NAME ## _INODE_NUMBER,			\
		 .start_block_number = NAME ## _DATABLOCK_NUMBER,	\
		 .block_count = NAME ## _BLOCK_COUNT,			\
		}

static void format_device(int fd)
{
	int error;
	int total_inodes;
	struct sfs_inode db_inodes[DB_INODE_COUNT] = {
		FILL_OUT_INODE(FT_DATA),
		FILL_OUT_INODE(FT_META),
		FILL_OUT_INODE(FT_LOG),
		FILL_OUT_INODE(TOKU_DIR),
		FILL_OUT_INODE(TOKU_ENV),
		FILL_OUT_INODE(TOKU_LOCK_DATA),
		FILL_OUT_INODE(TOKU_LOCK_ENV),
		FILL_OUT_INODE(TOKU_LOCK_LOGS),
		FILL_OUT_INODE(TOKU_LOCK_RECV),
		FILL_OUT_INODE(TOKU_LOCK_TEMP),
		FILL_OUT_INODE(TOKU_ROLLBACK),
		FILL_OUT_INODE(TOKU_TEST_ONE),
		FILL_OUT_INODE(TOKU_TEST_TWO),
		FILL_OUT_INODE(TOKU_TEST_THREE)
	};
	struct sfs_dir_entry root_entries[ROOT_OBJ_COUNT] = {
		{.filename = "db",	.inode_no = FT_DB_DIR_INODE_NUMBER},
		{.filename = "tmp",	.inode_no = FT_TEMP_DIR_INODE_NUMBER}
	};
	struct sfs_dir_entry db_entries[DB_OBJ_COUNT] = {
		{.filename = "ftfs_data_2_1_19.tokudb", 	.inode_no = FT_DATA_INODE_NUMBER},
		{.filename = "ftfs_meta_2_1_19.tokudb", 	.inode_no = FT_META_INODE_NUMBER},
		{.filename = "log000000000000.tokulog25", 	.inode_no = FT_LOG_INODE_NUMBER},
		{.filename = "tokudb.directory", 		.inode_no = TOKU_DIR_INODE_NUMBER},
		{.filename = "tokudb.environment", 		.inode_no = TOKU_ENV_INODE_NUMBER},
		{.filename = "data",				.inode_no = TOKU_LOCK_DATA_INODE_NUMBER},
		{.filename = "environment",			.inode_no = TOKU_LOCK_ENV_INODE_NUMBER},
		{.filename = "logs", 				.inode_no = TOKU_LOCK_LOGS_INODE_NUMBER},
		{.filename = "recovery",			.inode_no = TOKU_LOCK_RECV_INODE_NUMBER},
		{.filename = "temp",				.inode_no = TOKU_LOCK_TEMP_INODE_NUMBER},
		{.filename = "tokudb.rollback", 		.inode_no = TOKU_ROLLBACK_INODE_NUMBER},
		{.filename = "test_one_2_1_19.tokudb", 		.inode_no = TOKU_TEST_ONE_INODE_NUMBER},
		{.filename = "test_two_2_1_19.tokudb", 		.inode_no = TOKU_TEST_TWO_INODE_NUMBER},
		{.filename = "test_three_2_1_19.tokudb", 	.inode_no = TOKU_TEST_THREE_INODE_NUMBER}
	};

	total_inodes = 0;
	error = -1;
	do {
		/* blk_nr=0 */
		if (write_superblock(fd, TOTAL_INODE_COUNT))
			break;
		/* blk_nr=1 */
		if (write_root_inode(fd, &total_inodes))
			break;
		if (write_db_dir_inode(fd, &total_inodes))
			break;
		if (write_temp_dir_inode(fd, &total_inodes))
			break;
		if (write_db_files_inode(fd, db_inodes, DB_INODE_COUNT, &total_inodes))
			break;
		if (total_inodes != TOTAL_INODE_COUNT) {
			printf("total_inodes(%d) is not expected\n", total_inodes);
			break;
		}
		/* Skip the remaining part of inode table */
		if (sfs_inode_table_lseek(fd, total_inodes))
			break;
		/* Root dir blk_nr=2 */
		if (write_dirent(fd, root_entries, ROOT_OBJ_COUNT, 2))
			break;
		/* DB dir blk_nr=4 */
		if (write_dirent(fd, db_entries, DB_OBJ_COUNT, 3))
			break;
		error = 0;
	} while (0);

	if (error != 0) {
		printf("!!!!!!!!!Something wrong just happend\n");
		assert(false);
	}

	/* Zero out important blocks */
	zero_out_blocks(fd, TOKU_ROLLBACK_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, TOKU_DIR_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, FT_LOG_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, FT_META_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, FT_DATA_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, TOKU_TEST_ONE_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, TOKU_TEST_TWO_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, TOKU_TEST_THREE_DATABLOCK_NUMBER, 5);
	zero_out_blocks(fd, TOKU_ENV_DATABLOCK_NUMBER, 5);

	fsync(fd);
	printf("====== format sfs done! ======\n");
}

int main(int argc, char *argv[])
{
	int fd;
	ssize_t ret;
	int total_inodes = 0;
	uint64_t total_blk_num, file_size_in_bytes;

	if (argc != 2) {
		printf("Usage: mkfs-sfs <device> \n");
		return -1;
	}

	fd = open(argv[1], O_RDWR);
	if (fd == -1) {
		perror("Error opening the device");
		return -1;
	}

	ret = ioctl(fd, BLKGETSIZE64, &file_size_in_bytes);
	if (ret != 0) {
		perror("Error opening the device");
		return -1;
	}
	total_blk_num = file_size_in_bytes >> SFS_DEFAULT_BLOCK_BITS;
	printf("device size is %"PRIu64"\n", file_size_in_bytes);
	printf("total block num is %"PRIu64"\n", total_blk_num);

	ret = block_allocation(total_blk_num);
	if (ret != 0) {
		printf("Error in block_allocation for %s", argv[1]);
		return -1;
	}
	format_device(fd);
	close(fd);
	return 0;
}
