/*
 * for inode allocation: 1 is for root dir, 2 is reserved.
 */
enum sfs_inode_number {
	FT_DB_DIR_INODE_NUMBER   = 3, /* for /db */
	FT_TEMP_DIR_INODE_NUMBER = 4, /* for /temp */
	FT_DATA_INODE_NUMBER     = 5, /* for datadb */
	FT_META_INODE_NUMBER     = 6, /* for metadb */
	FT_LOG_INODE_NUMBER      = 7, /* for log */
	TOKU_DIR_INODE_NUMBER    = 8, /* for directory file */
	TOKU_ENV_INODE_NUMBER    = 9, /* for env file */
	TOKU_LOCK_DATA_INODE_NUMBER = 10,  /* for data lock file */
	TOKU_LOCK_LOGS_INODE_NUMBER = 11,  /* for log lock file */
	TOKU_LOCK_ENV_INODE_NUMBER  = 12,  /* for env lock file */
	TOKU_LOCK_RECV_INODE_NUMBER = 13,  /* for recovery lock file */
	TOKU_LOCK_TEMP_INODE_NUMBER = 14,  /* for temp lock file */
	TOKU_ROLLBACK_INODE_NUMBER  = 15,  /* for rollback file */
	TOKU_TEST_ONE_INODE_NUMBER   = 16, /* for test db files */
	TOKU_TEST_TWO_INODE_NUMBER   = 17,
	TOKU_TEST_THREE_INODE_NUMBER = 18
};

/* For verification */
#define TOTAL_INODE_COUNT (TOKU_TEST_THREE_INODE_NUMBER - 1)

/* variables for block allocation */
uint64_t FT_DB_DIR_DATABLOCK_NUMBER, FT_DB_DIR_BLOCK_COUNT; /* for /db dir */
uint64_t FT_TEMP_DIR_DATABLOCK_NUMBER, FT_TEMP_DIR_BLOCK_COUNT; /* for /temp dir */
uint64_t FT_META_DATABLOCK_NUMBER, FT_META_BLOCK_COUNT; /* for metadb file */
uint64_t FT_DATA_DATABLOCK_NUMBER, FT_DATA_BLOCK_COUNT; /* for datadb file */
uint64_t FT_LOG_DATABLOCK_NUMBER, FT_LOG_BLOCK_COUNT; /* for the log file */ 
uint64_t TOKU_DIR_DATABLOCK_NUMBER, TOKU_DIR_BLOCK_COUNT; /* for directory file  */ 
uint64_t TOKU_ENV_DATABLOCK_NUMBER, TOKU_ENV_BLOCK_COUNT; /* for env file */
uint64_t TOKU_LOCK_DATA_DATABLOCK_NUMBER, TOKU_LOCK_DATA_BLOCK_COUNT; /* for data lock file */
uint64_t TOKU_LOCK_ENV_DATABLOCK_NUMBER, TOKU_LOCK_ENV_BLOCK_COUNT; /* for env lock file */
uint64_t TOKU_LOCK_LOGS_DATABLOCK_NUMBER, TOKU_LOCK_LOGS_BLOCK_COUNT; /* for logs lock file */
uint64_t TOKU_LOCK_RECV_DATABLOCK_NUMBER, TOKU_LOCK_RECV_BLOCK_COUNT; /* for recovery lock file */
uint64_t TOKU_LOCK_TEMP_DATABLOCK_NUMBER, TOKU_LOCK_TEMP_BLOCK_COUNT; /* for temp lock file */
uint64_t TOKU_ROLLBACK_DATABLOCK_NUMBER, TOKU_ROLLBACK_BLOCK_COUNT; /* for rollback file */
uint64_t TOKU_TEST_ONE_DATABLOCK_NUMBER, TOKU_TEST_ONE_BLOCK_COUNT; /* for test one files */
uint64_t TOKU_TEST_TWO_DATABLOCK_NUMBER, TOKU_TEST_TWO_BLOCK_COUNT; /* for test two files */
uint64_t TOKU_TEST_THREE_DATABLOCK_NUMBER, TOKU_TEST_THREE_BLOCK_COUNT; /* for test three files */

#define SFS_INVALID_BLOCK_NUM 0xffffffffffffffff

/*
 * Always assume block size is 4 kib
 *
 * Decide the size of each file first and then decide
 * their location
 */
int block_allocation(uint64_t total_blk_num) {
	uint64_t reserved_blk_num = 0;
	uint64_t rem_blk_num = 0;

	/* for fixed-size files */
	FT_DB_DIR_BLOCK_COUNT = 1;  /* for /db */
	FT_TEMP_DIR_BLOCK_COUNT = 1;/* for /temp */ 
	FT_LOG_BLOCK_COUNT = 1024*256*2; /* for log */
	TOKU_DIR_BLOCK_COUNT = 256*4; /* for directory file */
	TOKU_ENV_BLOCK_COUNT = 256*4; /* for env file */
	TOKU_LOCK_DATA_BLOCK_COUNT = 0; /* for lock files */
	TOKU_LOCK_ENV_BLOCK_COUNT  = 0;
	TOKU_LOCK_LOGS_BLOCK_COUNT = 0;
	TOKU_LOCK_RECV_BLOCK_COUNT = 0;
	TOKU_LOCK_TEMP_BLOCK_COUNT = 0;
	TOKU_ROLLBACK_BLOCK_COUNT  = 256*256; /* for rollback file */
	TOKU_TEST_ONE_BLOCK_COUNT = 256*256; /* for test one file */
	TOKU_TEST_TWO_BLOCK_COUNT = 256*256; /* for test two file */
	TOKU_TEST_THREE_BLOCK_COUNT = 256*256; /* for test three file */
	/*
	 * SFS_LAST_RESERVED_BLOCK include super block, inode table, root dir entry
	 */
	reserved_blk_num = (SFS_LAST_RESERVED_BLOCK + 1) +
			FT_DB_DIR_BLOCK_COUNT +
			FT_TEMP_DIR_BLOCK_COUNT +
			FT_LOG_BLOCK_COUNT +
			TOKU_DIR_BLOCK_COUNT +
			TOKU_ENV_BLOCK_COUNT +
			TOKU_ROLLBACK_BLOCK_COUNT +
			TOKU_TEST_ONE_BLOCK_COUNT +
			TOKU_TEST_TWO_BLOCK_COUNT +
			TOKU_TEST_THREE_BLOCK_COUNT;

	rem_blk_num = total_blk_num - reserved_blk_num;
	/* At least 1 GiB for meta and data db file */
	if (rem_blk_num < 1024*256) {
#ifdef __KERNEL__
		printk("The device is too small\n");
#else
		printf("The device is too small\n");
#endif
		return -1;
	}

 	/* meta db file gets 10% */
	FT_META_BLOCK_COUNT = (rem_blk_num) / 10;
 	/* data db file gets 90% */
	FT_DATA_BLOCK_COUNT = (rem_blk_num - FT_META_BLOCK_COUNT);
	/*
	 * Already assigned the size for each file
 	 * Still need to decide the location of the files
	 */
	FT_DB_DIR_DATABLOCK_NUMBER   = SFS_LAST_RESERVED_BLOCK + 1;
	FT_TEMP_DIR_DATABLOCK_NUMBER = FT_DB_DIR_DATABLOCK_NUMBER + FT_DB_DIR_BLOCK_COUNT;
	FT_META_DATABLOCK_NUMBER     = FT_TEMP_DIR_DATABLOCK_NUMBER + FT_TEMP_DIR_BLOCK_COUNT;
	FT_LOG_DATABLOCK_NUMBER      = FT_META_DATABLOCK_NUMBER + FT_META_BLOCK_COUNT;
	FT_DATA_DATABLOCK_NUMBER     = FT_LOG_DATABLOCK_NUMBER + FT_LOG_BLOCK_COUNT;
	TOKU_DIR_DATABLOCK_NUMBER    = FT_DATA_DATABLOCK_NUMBER + FT_DATA_BLOCK_COUNT;
        TOKU_ENV_DATABLOCK_NUMBER    = TOKU_DIR_DATABLOCK_NUMBER + TOKU_DIR_BLOCK_COUNT;
	TOKU_ROLLBACK_DATABLOCK_NUMBER  = TOKU_ENV_DATABLOCK_NUMBER + TOKU_ENV_BLOCK_COUNT;
	TOKU_TEST_ONE_DATABLOCK_NUMBER  = TOKU_ROLLBACK_DATABLOCK_NUMBER + TOKU_ROLLBACK_BLOCK_COUNT;
	TOKU_TEST_TWO_DATABLOCK_NUMBER  = TOKU_TEST_ONE_DATABLOCK_NUMBER + TOKU_TEST_ONE_BLOCK_COUNT;
	TOKU_TEST_THREE_DATABLOCK_NUMBER = TOKU_TEST_TWO_DATABLOCK_NUMBER + TOKU_TEST_TWO_BLOCK_COUNT;

	/* lock file does not have data blocks */
	TOKU_LOCK_DATA_DATABLOCK_NUMBER = SFS_INVALID_BLOCK_NUM;
	TOKU_LOCK_ENV_DATABLOCK_NUMBER  = SFS_INVALID_BLOCK_NUM;
	TOKU_LOCK_LOGS_DATABLOCK_NUMBER = SFS_INVALID_BLOCK_NUM;
	TOKU_LOCK_RECV_DATABLOCK_NUMBER = SFS_INVALID_BLOCK_NUM;
	TOKU_LOCK_TEMP_DATABLOCK_NUMBER = SFS_INVALID_BLOCK_NUM;

	/* Verify the last block is is equal to total_blk_num - 1 */
#ifdef __KERNEL__
	BUG_ON(TOKU_TEST_THREE_DATABLOCK_NUMBER + TOKU_TEST_THREE_BLOCK_COUNT - 1 != total_blk_num - 1);
#else
	//printf("TOKU_TEST_THREE_DATABLOCK_NUMBER + TOKU_TEST_THREE_BLOCK_COUNT - 1=%" PRIu64 "\n",
	//	TOKU_TEST_THREE_DATABLOCK_NUMBER + TOKU_TEST_THREE_BLOCK_COUNT - 1);
	assert(TOKU_TEST_THREE_DATABLOCK_NUMBER + TOKU_TEST_THREE_BLOCK_COUNT - 1 == total_blk_num - 1);
#endif
	return 0;
}
