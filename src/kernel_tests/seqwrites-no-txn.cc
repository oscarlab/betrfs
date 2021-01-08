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

static const size_t nodesize = 128 << 10;
static const size_t keysize = 8;
static const size_t valsize = 92;
static const size_t rowsize = keysize + valsize;
static const int max_degree = 16;
static const size_t numleaves = max_degree * 3; // want height 2, this should be good enough
static const size_t numrows = (numleaves * nodesize + rowsize) / rowsize;

static void seqwrite_no_txn(bool asc) {
	int r;
	r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU+S_IRWXG+S_IRWXO);
	CKERR(r);

	DB_ENV *env;
	r = db_env_create(&env, 0);
	CKERR(r);
	r = env->open(env, TOKU_TEST_ENV_DIR_NAME, DB_INIT_LOCK|DB_INIT_MPOOL|DB_INIT_LOG|DB_INIT_TXN|DB_CREATE|DB_PRIVATE, S_IRWXU+S_IRWXG+S_IRWXO);
	CKERR(r);

	DB *db;
	r = db_create(&db, env, 0);
	CKERR(r);
	r = db->set_pagesize(db, nodesize);
	CKERR(r);
	r = db->open(db, NULL, TOKU_TEST_DATA_DB_NAME, NULL, DB_BTREE, DB_CREATE, 0666);
	CKERR(r);

        char v[valsize];
        ZERO_ARRAY(v);
	uint64_t k;
	DBT key, val;
	dbt_init(&key, &k, sizeof k);
	dbt_init(&val, v, valsize);
	for (size_t i = 0; i < numrows; ++i) {
		k = toku_htod64(numrows + (asc ? i : -i));
		r = db->put(db, NULL, &key, &val, 0);
		CKERR(r);
	}

	r = db->close(db, 0);
	CKERR(r);

	r = env->close(env, 0);
	CKERR(r);
}

extern "C" int test_seqwrite_no_txn(void);
int test_seqwrite_no_txn(void) {
    pre_setup();
    seqwrite_no_txn(true);
    seqwrite_no_txn(false);
    post_teardown();
    return 0;
}

/*
void perf_test_micro_writes() {
	// create an environment
	// use the environment to create the db
	// open db
	// create two dbt's key and value
	// generate the key and then write a function to genearte the value from the key
	// db-put in a loop.
	// single threaded.
	// db-put with db, null, key, value, 0

	int r;
	int i = 0;

	DB_ENV *env = NULL;
	DB *db = NULL;

	const char *db_env_dir = TOKU_TEST_FILENAME;
	const char *db_filename = "seq-write.db";
	int db_env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;

	uint64_t k = 0;
	DBT key;
	DBT val;

	pre_setup();
	// setup env
	r = db_env_create(&db_env, 0); assert(r == 0);
	r = db_env->open(db_env, db_env_dir, db_env_open_flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); assert(r == 0);

	// setup db
	DB *db = NULL;
	r = db_create(&db, db_env, 0); assert(r == 0);
	if (pagesize) {
		r = db->set_pagesize(db, pagesize); assert(r == 0);
	}

	r = db->open(db, NULL, db_filename, NULL, DB_BTREE, DB_CREATE|DB_AUTO_COMMIT|DB_THREAD, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); assert(r == 0);

	// use db put to write to the data
	for(; i < 100000; i++, k++) {
		key = { .data = &k, .size = sizeof k};
		val = { .data = &k, .size = sizeof k};
		r = db->put(db, NULL, &key, &val, 0); assert(r == 0);
	}

	// close env
	r = db->close(db, 0); assert(r == 0); db = NULL;
	r = db_env->close(db_env, 0); assert(r == 0); db_env = NULL;
	post_teardown();
}
*/
