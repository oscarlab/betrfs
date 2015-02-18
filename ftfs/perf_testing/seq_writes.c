// test seq writes for the south bound file system
// write micro data in a loop with a single thread initially
// use the ftfs_fs/ftfs_bstore_put functionality to create the db, env and db_put
// no need to create a txn to write to the file system

// modify the code to write in multi-threaded way

#include <linux/kernel.h>
#include <linux/types.h>
#include <linux/errno.h>
#include <linux/string.h>
#include <linux/slab.h>
#include <asm/uaccess.h>


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
    	const char *db_filename = "test.db";
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

