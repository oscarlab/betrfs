// test seq writes for the south bound file system
// write micro data in a loop with a single thread initially
// use the ftfs_fs/ftfs_bstore_put functionality to create the db, env and db_put
// no need to create a txn to write to the file system

// modify the code to write in multi-threaded way

#include "test.h"
#include <db.h>
#include <toku_portability.h>
#include <toku_os.h>
#include <memory.h>
#include <stdint.h>
#include <stdlib.h>

#include <inttypes.h>

#define KERN_ALERT "\0011"
extern "C" int printk(const char *fmt, ...);

#define DB_ELEMENTS 100000
#define TXN_FREQ 32

#define KiB (1024)
#define MiB (1024*1024)
#define GiB (1024*1024*1024)

#define PAGE_SIZE 4096
#define BLOCK_SIZE PAGE_SIZE
#define RAND_BLOCKS 40
#define TXN_MAY_WRITE DB_SERIALIZABLE
#define TXN_READONLY (DB_TXN_SNAPSHOT|DB_TXN_READ_ONLY)
#define DB_PUT_FLAGS 0
#define DB_GET_FLAGS 0
#define DB_CURSOR_FLAGS 0

// max size of uint64_t as a string {2^64 - 1, +1 for null}
#define KEYLEN 21
#define KEYFMT "%09"

static void initialize_perf_test(DB_ENV **my_env, DB **my_db,
				 const char *db_env_dir,
				 const char *db_filename)
{
	int r;
	uint32_t db_env_flags;
	uint32_t db_flags;
	uint32_t gigab, bytes;
	DB_TXN *txn = NULL;
	DB_ENV *db_env = NULL;
	DB *db = NULL;

	db_env_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL |
		DB_INIT_LOCK | DB_RECOVER | DB_INIT_LOG | DB_INIT_TXN;
	db_flags = DB_CREATE | DB_THREAD;


	/* init ft-layer */

	r = toku_ft_layer_init_with_panicenv();
	assert(r == 0);


	/* setup env */

	r = db_env_create(&db_env, 0);
	assert(r == 0);

	gigab = 1;
	bytes = 0;
	r = db_env->set_cachesize(db_env, gigab, bytes, 1);


	r = db_env->open(db_env, db_env_dir, db_env_flags, 0755);
	if (r != 0)
		printk(KERN_ALERT "db_env->open failed: %d\n", r);
	assert(r == 0);


	/* setup db */

	r = db_env->txn_begin(db_env, NULL, &txn, 0);
	assert(r == 0);

	r = db_create(&db, db_env, 0);
	assert(r == 0);

	r = db->open(db, NULL, db_filename, NULL, DB_BTREE, db_flags, 0644);
	assert(r == 0);

	txn->commit(txn, DB_TXN_SYNC);

	db_env_flags = 60; /* 60 s */
	db_env->checkpointing_set_period(db_env, db_env_flags);
	db_env_flags = 1; /* 1 s */
	db_env->cleaner_set_period(db_env, db_env_flags);
	db_env_flags = 1000; /* 1000 ms */
	db_env->change_fsync_log_period(db_env, db_env_flags);

	*my_env = db_env;
	*my_db = db;
}

static void end_perf_test(DB_ENV *db_env, DB *db)
{
	int r;

	// close env
	r = db->close(db, 0);
	assert(r == 0);

	r = db_env->close(db_env, 0);
	assert(r == 0);

	toku_ydb_destroy();
}

static int init_random_data(unsigned int block_size,
			    unsigned int rand_blocks,
			    unsigned int seed,
			    char ***blocks)
{
	int ret;
	int i, j;
	char **array;

	srandom(seed);

	array = (char **)toku_malloc(rand_blocks * sizeof(*array));
	if (!array)
		return -ENOMEM;

	/* we want some page aligned buffers with incompressible data */
	for (i = 0; i < rand_blocks; i++) {
		ret = posix_memalign((void**)&array[i], PAGE_SIZE, block_size);
		if (ret) {
			fprintf(stderr, "memalign failed: %d\n", ret);
			return ret;
		}
		for (j = 0; j < block_size; j++)
			array[i][j] = (char) random();
	}

	*blocks = array;
	return 0;
}

static int sequential_writes(uint64_t num_inserts,
			     const char *db_env_dir,
			     const char *db_filename,
			     uint32_t txn_freq)
{
	int r = -EINVAL;
	int txn_uncommitted = 0;
	uint64_t i = 0;
	DB_ENV *env = NULL;
	DB *db = NULL;
	DB_TXN *txn = NULL;
	char kstr[KEYLEN];
	DBT key;
	DBT val;
	char **array = NULL;
	struct timeval t1, t2, tsub;

	init_random_data(BLOCK_SIZE, RAND_BLOCKS, time(NULL), &array);

	initialize_perf_test(&env, &db, db_env_dir, db_filename);


	printk(KERN_ALERT "starting perf test\n");

	gettimeofday(&t1, NULL);

	// use db put to write to the data
	for(i = 0; i < num_inserts; i++) {
		memset(kstr, 0, KEYLEN);
		sprintf(kstr, KEYFMT PRIu64, i);
		key = { .data = kstr,
			.size = KEYLEN,
			.ulen = KEYLEN,
			.flags = DB_DBT_USERMEM};
		val = { .data = array[i % RAND_BLOCKS],
			.size = BLOCK_SIZE,
			.ulen = BLOCK_SIZE,
			.flags = DB_DBT_USERMEM};

		if (txn_uncommitted == 0) {
			r = env->txn_begin(env, NULL, &txn, TXN_MAY_WRITE);
			assert(r == 0);
		}

		r = db->put(db, txn, &key, &val, 0);
		assert(r == 0);

		if (++txn_uncommitted == txn_freq) {
			r = txn->commit(txn, DB_TXN_NOSYNC);
			assert(r == 0);
			txn_uncommitted = 0;
		}
	}

	if (txn_uncommitted) {
		r = txn->commit(txn, DB_TXN_NOSYNC);
		assert(r == 0);
	}

	gettimeofday(&t2, NULL);
	end_perf_test(env, db);

	toku_free(array);

	timersub(&t2, &t1, &tsub);
	printk(KERN_ALERT "Test end: %ld.%06ld\n",
	       (long int) tsub.tv_sec, (long int) tsub.tv_usec);
	return r;
}

extern "C" int perf_test_sequential_writes(void);
int perf_test_sequential_writes(void)
{
	const char *db_env_dir = "/tmp/";
	const char *db_filename = "test.db";
	return sequential_writes(DB_ELEMENTS, db_env_dir, db_filename, TXN_FREQ);
}

struct read_cb_info {
	uint64_t range_end; /* prefetch up to this block */
	uint64_t last_read;
	int do_continue;
};

static char block[BLOCK_SIZE];

static int read_cb(DBT const *key, DBT const *val, void *extra)
{
	uint64_t k;
	int err;

	err = sscanf((char *)key->data, "%" SCNu64, &k);
	if (err != 1) {
		printf("could not retreive key (read_cb): err=%d", err);
		return -ENOENT;
	}

	struct read_cb_info *info = (read_cb_info *) extra;

	if (k < info->range_end) {
		memcpy(block, val->data, val->size);
		info->last_read = k;
		info->do_continue = 1;
		return TOKUDB_CURSOR_CONTINUE;
	} else if (k == info->range_end) {
		memcpy(block, val->data, val->size);
		info->last_read = k;
	} else {
		printf("read out of range: %" PRIu64 " (%" PRIu64 ")\n",
		       k, info->range_end);
		return -ENOENT;
	}

	info->do_continue = 0;
	return 0;
}

static int sequential_reads(uint32_t num_searches,
			    const char *db_env_dir,
			    const char *db_filename,
			    uint32_t txn_freq)
{
	int r = -EINVAL;
	uint64_t k1 = 0;
	char kstr1[KEYLEN];
	uint64_t k2 = 0;
	char kstr2[KEYLEN];
	DB_ENV *env = NULL;
	DB *db = NULL;
	DB_TXN *txn = NULL;
	DBT key;
	DBT max;
	DBC *cursor;
	struct read_cb_info info;
	struct timeval t1, t2, tsub;

	info.range_end = 0;
	info.last_read = 0;
	info.do_continue = 0;

	initialize_perf_test(&env, &db, db_env_dir, db_filename);

	printk(KERN_ALERT "starting perf test\n");

	gettimeofday(&t1, NULL);

	while(k1 < num_searches) {

		r = env->txn_begin(env, NULL, &txn, TXN_READONLY);
		assert(r == 0);

		r = db->cursor(db, txn, &cursor, DB_CURSOR_FLAGS);
		assert(r == 0);

		memset(kstr1, 0, KEYLEN);
		sprintf(kstr1, KEYFMT PRIu64, k1);
		key = { .data = kstr1,
			.size = KEYLEN,
			.ulen = KEYLEN,
			.flags = DB_DBT_USERMEM};

		k2 = k1 + txn_freq;

		memset(kstr2, 0, KEYLEN);
		sprintf(kstr2, KEYFMT PRIu64, k2);
		max = { .data = kstr2,
			.size = KEYLEN,
			.ulen = KEYLEN,
			.flags = DB_DBT_USERMEM};

		r = cursor->c_set_bounds(cursor, &key, &max, true, 0);
		assert(r == 0);
	
		info.range_end = k2;
		info.do_continue = 0;

		r = cursor->c_getf_set_range(cursor, 0, &key,
					     read_cb, &info);
		assert(r == 0 || r == TOKUDB_CURSOR_CONTINUE);

		while (r == 0 && info.do_continue) {
			r = cursor->c_getf_next(cursor, 0, read_cb, &info);
		}

		if (r == -ENOENT) {
			cursor->c_close(cursor);
			txn->commit(txn, DB_TXN_NOSYNC);
			goto end_test;
		}

		r = cursor->c_close(cursor);
		assert(r == 0);

		txn->commit(txn, DB_TXN_NOSYNC);
		assert(r == 0);

		k1 = info.last_read + 1;
	}

end_test:
	gettimeofday(&t2, NULL);

	end_perf_test(env, db);

	timersub(&t2, &t1, &tsub);
	printk(KERN_ALERT "Test end: %ld.%06ld\n",
	       (long int) tsub.tv_sec, (long int) tsub.tv_usec);

	/*
	 * usecs = tsub.tv_sec * 1000000 + tsub.tv_usec;
	 * mb = (k1 * PAGE_SIZE) / MiB;
	 * mbps = mb / secs;
	 */

	printk(KERN_ALERT "\tread %" PRIu64 "pages\n", k1);
	printk(KERN_ALERT "\t -> %" PRIu64 "MB/s\n",
	       (k1 * PAGE_SIZE * 1000000) /
	       (MiB * (tsub.tv_sec * 1000000 + tsub.tv_usec)));
	
	return r;
}

extern "C" int perf_test_sequential_reads(void);
int perf_test_sequential_reads(void)
{
	const char *db_env_dir = "/tmp/";
	const char *db_filename = "test.db";
	return sequential_reads(DB_ELEMENTS, db_env_dir, db_filename, TXN_FREQ);
}
