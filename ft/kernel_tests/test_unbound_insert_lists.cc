/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:

#include "test.h"
#include <logger.h>
#include <util/list.h>

//append unbound_insert entries to the logger's lists, update them,
//read them back, and remove them
#define LSIZE 16 /* units are MB */

static int setup_logger(TOKULOGGER *logger_p) {
    int r;
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r = toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU);    assert(r==0);
    if (r!=0) {
        int er = get_error_errno(r);
        return er;
    }

    r = toku_logger_create(logger_p);
    assert(r == 0);

    r = toku_logger_open(TOKU_TEST_FILENAME, *logger_p);
    return r;
}


static int verify_a_bunch_of_entries(TOKULOGGER logger, uint64_t a_bunch)
{
    int correct = 0;
    struct hlist_head *bucket;
    struct unbound_insert_entry *entry;
    for (uint64_t i = 1; i <= a_bunch; i++) {
        
        bucket = &logger->ubi_hash[UBI_WHICH_BUCKET(i)];
        for (entry = bucket->first ?
                 hlist_entry(bucket->first, struct unbound_insert_entry, hash) :
                 NULL;
             entry;
             entry = entry->hash.next ?
                 hlist_entry(entry->hash.next, struct unbound_insert_entry, hash) :
                 NULL) {
            // we inserted values with (DISKOFF:start==DISKOFF:len==NODENUM)
            if (entry->msn.msn == i) {
                assert(entry->diskoff == entry->size);
                assert(entry->diskoff == i);
                correct++;
            }
        }

    }

    return correct;
}

static int add_a_bunch_of_entries(TOKULOGGER logger, uint64_t a_bunch)
{
	BYTESTRING keybs = { .len = 0, .data = NULL };

	for (uint64_t i = 1; i <= a_bunch; i++) {
            toku_logger_append_unbound_insert_entry(logger, keybs, (LSN) {i}, NULL, (MSN){i});
	}
	return 0;
}

static int bind_a_bunch_of_entries(TOKULOGGER logger, uint64_t a_bunch)
{
	struct ft_msg msg;
	msg.type = FT_UNBOUND_INSERT;
	msg.xids = NULL;
	msg.key = NULL;
	msg.max_key = NULL;
	msg.val = NULL;
	msg.is_right_excl = false;

	for (uint64_t i = 1; i <= a_bunch; i++) {
		msg.msn = (MSN){i};
		toku_logger_bind_insert_location(logger, &msg, i, i);
	}

	return 0;
}

static int write_log(TOKULOGGER logger)
{
    LSN fsynced_lsn;
    ml_lock(&logger->input_lock);
    unsigned int n = logger->n_unbound_inserts;
    swap_inbuf_outbuf(logger);

    ml_unlock(&logger->input_lock); // release the input lock now, so
                                    // other threads can fill the
                                    // inbuf.  (Thus enabling group
                                    // commit.)

   write_outbuf_to_logfile(logger, &fsynced_lsn, n);

   release_output(logger, fsynced_lsn);

   return 0;
}


extern "C" int logger_test_tables(void);

extern "C" int logger_test_tables(void)
//Effect: test that an entry that is appended to the list/hash-table can be looked up, that lookups actually find them, and that 
{
    int ret;
    TOKULOGGER logger = NULL;
    initialize_dummymsn();
    if (setup_logger(&logger)) {
        printf("failed to setup logger\n");
        return -1;
    }
    if (add_a_bunch_of_entries(logger, 3*UNBOUND_HASH_BUCKETS)) {
        printf("failed to add a bunch of entries\n");
        return -1;
    }

    if (bind_a_bunch_of_entries(logger, 3*UNBOUND_HASH_BUCKETS)) {
        printf("failed to add a bunch of entries\n");
        return -1;
    }
    ret = verify_a_bunch_of_entries(logger, 3*UNBOUND_HASH_BUCKETS);
    assert(ret == (3*UNBOUND_HASH_BUCKETS)); // 0 is success

    return write_log(logger);
}
