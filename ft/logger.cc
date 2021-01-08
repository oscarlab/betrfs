/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*
COPYING CONDITIONS NOTICE:

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation, and provided that the
  following conditions are met:

      * Redistributions of source code must retain this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below).

      * Redistributions in binary form must reproduce this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below) in the documentation and/or other materials
        provided with the distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301, USA.

COPYRIGHT NOTICE:

  TokuDB, Tokutek Fractal Tree Indexing Library.
  Copyright (C) 2007-2013 Tokutek, Inc.

DISCLAIMER:

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

UNIVERSITY PATENT NOTICE:

  The technology is licensed by the Massachusetts Institute of
  Technology, Rutgers State University of New Jersey, and the Research
  Foundation of State University of New York at Stony Brook under
  United States of America Serial No. 11/760379 and to the patents
  and/or patent applications resulting from it.

PATENT MARKING NOTICE:

  This software is covered by US Patent No. 8,185,551.
  This software is covered by US Patent No. 8,489,638.

PATENT RIGHTS GRANT:

  "THIS IMPLEMENTATION" means the copyrightable works distributed by
  Tokutek as part of the Fractal Tree project.

  "PATENT CLAIMS" means the claims of patents that are owned or
  licensable by Tokutek, both currently or in the future; and that in
  the absence of this license would be infringed by THIS
  IMPLEMENTATION or by using or running THIS IMPLEMENTATION.

  "PATENT CHALLENGE" shall mean a challenge to the validity,
  patentability, enforceability and/or non-infringement of any of the
  PATENT CLAIMS or otherwise opposing any of the PATENT CLAIMS.

  Tokutek hereby grants to you, for the term and geographical scope of
  the PATENT CLAIMS, a non-exclusive, no-charge, royalty-free,
  irrevocable (except as stated in this section) patent license to
  make, have made, use, offer to sell, sell, import, transfer, and
  otherwise run, modify, and propagate the contents of THIS
  IMPLEMENTATION, where such license applies only to the PATENT
  CLAIMS.  This grant does not include claims that would be infringed
  only as a consequence of further modifications of THIS
  IMPLEMENTATION.  If you or your agent or licensee institute or order
  or agree to the institution of patent litigation against any entity
  (including a cross-claim or counterclaim in a lawsuit) alleging that
  THIS IMPLEMENTATION constitutes direct or contributory patent
  infringement, or inducement of patent infringement, then any rights
  granted to you under this License shall terminate as of the date
  such litigation is filed.  If you or your agent or exclusive
  licensee institute or order or agree to the institution of a PATENT
  CHALLENGE, then Tokutek may terminate any rights granted to you
  under this License.
*/

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."

#include <memory.h>
#include <ctype.h>
#include <limits.h>
#include <unistd.h>

#include "ft.h"
#include "log-internal.h"
#include "txn_manager.h"
#include "rollback_log_node_cache.h"
#include "huge_page_detection.h"
#include <util/status.h>
#include "checkpoint.h"

#include "logsuperblock.h"

#define NO_INLINE 0
static const int log_format_version=TOKU_LOG_VERSION;

static int open_logfile (TOKULOGGER logger);
static void logger_write_buffer (TOKULOGGER logger, LSN *fsynced_lsn);
static void delete_logfile(TOKULOGGER logger, long long index, uint32_t version);
static void grab_output(TOKULOGGER logger, LSN *fsynced_lsn);
//TODO:
//static void release_output(TOKULOGGER logger, LSN fsynced_lsn);
extern "C" void toku_dump_stack(void);
static void toku_print_bytes (FILE *outf, uint32_t len, char *data) {
    fprintf(outf, "\"");
    uint32_t i;
    for (i=0; i<len; i++) {
        switch (data[i]) {
        case '"':  fprintf(outf, " \\\""); break;
        case '\\': fprintf(outf, " \\\\"); break;
        case '\n': fprintf(outf, " \\n");  break;
        default:
            if (isprint(data[i])) fprintf(outf, " %c", data[i]);
            else fprintf(outf, " \\%03o", (unsigned char)(data[i]));
        }
    }
    fprintf(outf, " \"");
}

static bool is_a_logfile_any_version (const char *name, uint64_t *number_result, uint32_t *version_of_log) {
    bool rval = true;
    uint64_t result;
    int n;
    int r;
    uint32_t version;
    r = sscanf(name, "log%" SCNu64 ".tokulog%" SCNu32 "%n", &result, &version, &n);
    if (r!=2 || name[n]!='\0' || version <= TOKU_LOG_VERSION_1) {
        //Version 1 does NOT append 'version' to end of '.tokulog'
        version = TOKU_LOG_VERSION_1;
        r = sscanf(name, "log%" SCNu64 ".tokulog%n", &result, &n);
        if (r!=1 || name[n]!='\0') {
            rval = false;
        }
    }
    if (rval) {
        *number_result  = result;
        printf("Log version is %d, log file %s\n", version, name);
        *version_of_log = version;
    }

    return rval;
}


extern TOKULOGGER global_logger;
// TODO: can't fail
int toku_logger_create (TOKULOGGER *resultp) {
    if (complain_and_return_true_if_huge_pages_are_enabled()) {
        *resultp = NULL;
        set_errno(TOKUDB_HUGE_PAGES_ENABLED);
        return TOKUDB_HUGE_PAGES_ENABLED;
    }
    TOKULOGGER CALLOC(result);
    if (result==0) {
        return ENOMEM;
    }
    else
    {
        global_logger = result;
    }
    result->is_open=false;
    result->new_env=false;
    result->write_log_files = true;
    result->trim_log_files = false;
    result->remove_finalize_callback = NULL;
    // fd is uninitialized on purpose
    // ct is uninitialized on purpose
    toku_struct_stat sbuf;
    memset(&sbuf, 0, sizeof(toku_struct_stat));
    toku_stat("/db/log000000000000.tokulog25", &sbuf);
    result->lg_max = sbuf.st_size;
    // lsn is uninitialized
    result->inbuf  = (struct logbuf) {0, LOGGER_MIN_BUF_SIZE, (char *) toku_xmalloc(LOGGER_MIN_BUF_SIZE), ZERO_LSN};
    result->outbuf = (struct logbuf) {0, LOGGER_MIN_BUF_SIZE, (char *) toku_xmalloc(LOGGER_MIN_BUF_SIZE), ZERO_LSN};
    // written_lsn is uninitialized
    // fsynced_lsn is uninitialized
    result->last_completed_checkpoint_lsn = ZERO_LSN;
    // next_log_file_number is uninitialized
    // n_in_file is uninitialized
    toku_mutex_init(&result->ubi_lock, NULL);
    // unbound_insert_lsn is uniitialized
    result->n_unbound_inserts = 0;
    result->ubi_in = &result->ubi_lists[0];
    result->ubi_out = &result->ubi_lists[1];
    toku_list_init(result->ubi_in);
    toku_list_init(result->ubi_out);

    bjm_init(&result->bjm);
    int num_processors = toku_os_get_number_active_processors();
    result->writeback_kibbutz = toku_kibbutz_create_debug(num_processors, "toku_logger_wb");

    result->write_block_size = FT_DEFAULT_NODE_SIZE; // default logging size is the same as the default brt block size
    toku_logfilemgr_create(&result->logfilemgr);
    *resultp=result;
    ml_init(&result->input_lock);
    toku_mutex_init(&result->output_condition_lock, NULL);
    toku_cond_init(&result->output_condition,       NULL);
    toku_cond_init(&result->input_swapped, 	    NULL);
    result->rollback_cachefile = NULL;
    result->output_is_available = true;
    toku_txn_manager_init(&result->txn_manager);
    return 0;
}

static int close_logdir(TOKULOGGER logger) {
    close(logger->dfd);
    logger->dfd = -1;
    return 0;
}

int
toku_logger_open_with_last_xid(const char *directory, TOKULOGGER logger, TXNID last_xid) {
    if (logger->is_open) return EINVAL;

    int r;
    TXNID last_xid_if_clean_shutdown;
    r = toku_logfilemgr_init(logger->logfilemgr, directory, &last_xid_if_clean_shutdown);
    fprintf(stderr, "toku_logger_open_with_last_xid 1: %d\n", r);
    if ( r!=0 )
        return r;

    logger->lsn = toku_logfilemgr_get_last_lsn(logger->logfilemgr);
    logger->written_lsn = logger->lsn;
    logger->fsynced_lsn = logger->lsn;
    logger->inbuf.max_lsn_in_buf  = logger->lsn;
    logger->outbuf.max_lsn_in_buf = logger->lsn;

    // open directory, save pointer for fsyncing t:2445
    logger->dfd = open(directory, O_RDONLY|O_NDELAY|O_DIRECTORY|O_LARGEFILE, S_IRWXU);
    fprintf(stderr, "toku_logger_open_with_last_xid 2: %d\n", r);
    if (r!=0) return r;

    // YZJ: always use the same log file
    // logger->next_log_file_number = nexti;
    logger->next_log_file_number = 0;

    r = open_logfile(logger);
    fprintf(stderr, "toku_logger_open_with_last_xid 4: %d\n", r);
    if (r!=0) return r;
    if (last_xid == TXNID_NONE) {
        last_xid = last_xid_if_clean_shutdown;
    	printf("__func__,last xid is %" PRIu64 "\n", last_xid);
    }
    toku_txn_manager_set_last_xid_from_logger(logger->txn_manager, last_xid);

    logger->is_open = true;
    return 0;
}

int toku_verify_logmagic_read_log_end (int fd, uint32_t *log_end) {
    {
        char magic[8];
        int r=toku_os_read(fd, magic, 8);
        if (r!=8) {
            return DB_BADFORMAT;
        }
        if (memcmp(magic, "tokulogg", 8)!=0) {
            return DB_BADFORMAT;
        }
    }
    {
        int version;
        int r=toku_os_read(fd, &version, 4);
        if (r!=4) {
            return DB_BADFORMAT;
        }
        int version_tmp=toku_ntohl(version);
        if (version_tmp < TOKU_LOG_MIN_SUPPORTED_VERSION || version_tmp > TOKU_LOG_VERSION)
            return DB_BADFORMAT;
    }
    {
        off_t pos=lseek(fd, TOKU_LOG_END_OFFSET, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
    if (pos != TOKU_LOG_END_OFFSET) return get_error_errno(pos);
#else
    if (pos != TOKU_LOG_END_OFFSET) return get_error_errno();
#endif
    }
    {
        int r=toku_os_read(fd, log_end, 4);
        if (r!=4) {
            return DB_BADFORMAT;
        }
    }

    return 0;
}

int toku_update_logfile_end(int fd, uint32_t log_end)
{
    off_t pos = lseek(fd, TOKU_LOG_END_OFFSET, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
    if (pos != TOKU_LOG_END_OFFSET) return get_error_errno(pos);
#else
    if (pos != TOKU_LOG_END_OFFSET) return get_error_errno();
#endif
    int r = toku_os_write(fd, &log_end, 4);
    if (r != 0) return r;
    printf("%s: r=%d\n", __func__, r);
    fsync(fd);
    return 0;
}

int toku_update_logfile_start(int fd, uint32_t log_start)
{
    off_t pos = lseek(fd, TOKU_LOG_START_OFFSET, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
    if (pos != TOKU_LOG_START_OFFSET) return get_error_errno(pos);
#else
    if (pos != TOKU_LOG_START_OFFSET) return get_error_errno();
#endif
    int r = toku_os_write(fd, &log_start, 4);
    if (r != 0) return r;
    printf("%s: r=%d\n", __func__, r);
    fsync(fd);
    return 0;
}

/* use logger->n_in_file or size to update the log super block */
static void toku_logger_header_write(TOKULOGGER logger)
{
   if (logger->fd != -1) {
        struct log_super_block *log_sb = (struct log_super_block *) toku_xmalloc(sizeof(struct log_super_block));
        memset(log_sb, 0, sizeof(struct log_super_block));
        assert(log_sb != NULL);
        memcpy(log_sb->magic, "tokulogg", 8);
        log_sb->version = toku_htonl(log_format_version);
        log_sb->log_end = logger->n_in_file;
        // SCB (9/8/19): Currently, this function is only called on brand new logs.
        log_sb->log_start = sizeof(struct log_super_block);
        off_t pos = lseek(logger->fd, 0, SEEK_SET);
        assert(pos == 0);
        toku_os_full_write(logger->fd, log_sb, sizeof(struct log_super_block));
        toku_free(log_sb);
    } else {
        assert(0);
    }
}

static int toku_logger_update_both_and_verify(TOKULOGGER logger)
{
   if (logger->fd != -1) {
        // Get a backup of current pos */
        off_t old_pos = lseek(logger->fd, 0, SEEK_CUR);
#ifdef TOKU_LINUX_MODULE
        if (old_pos != logger->n_in_file) return get_error_errno(old_pos);
#else
        if (old_pos != logger->n_in_file) return get_error_errno();
#endif
        // Seek to the super block */
        off_t pos = lseek(logger->fd, TOKU_LOG_START_OFFSET, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
        if (pos != TOKU_LOG_START_OFFSET) return get_error_errno(pos);
#else
        if (pos != TOKU_LOG_START_OFFSET) return get_error_errno();
#endif
        uint32_t vals[2] = {logger->n_in_file, logger->n_in_file};
        toku_os_full_write(logger->fd, vals, 8);
        // Go back to the previous position */
        pos = lseek(logger->fd, old_pos, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
        if (pos != old_pos) return get_error_errno(pos);
#else
        if (pos != old_pos) return get_error_errno();
#endif
        return 0;
    } else {
        return -EBADF;
    }
}

static int toku_logger_file_size_update_and_verify(TOKULOGGER logger)
{
   if (logger->fd != -1) {
        // Get a backup of current pos */
        off_t old_pos = lseek(logger->fd, 0, SEEK_CUR);
#ifdef TOKU_LINUX_MODULE
        if (old_pos != logger->n_in_file) return get_error_errno(old_pos);
#else
        if (old_pos != logger->n_in_file) return get_error_errno();
#endif

        // Seek to the super block */
        off_t pos = lseek(logger->fd, TOKU_LOG_END_OFFSET, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
        if (pos != TOKU_LOG_END_OFFSET) return get_error_errno(pos);
#else
        if (pos != TOKU_LOG_END_OFFSET) return get_error_errno();
#endif
        uint32_t log_end = logger->n_in_file;
        toku_os_full_write(logger->fd, &log_end, 4);
        // Go back to the previous position */
        pos = lseek(logger->fd, old_pos, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
        if (pos != old_pos) return get_error_errno(pos);
#else
        if (pos != old_pos) return get_error_errno();
#endif
        return 0;
    } else {
        return -EBADF;
    }
}

int toku_logger_open (const char *directory, TOKULOGGER logger) {
    return toku_logger_open_with_last_xid(directory, logger, TXNID_NONE);
}

bool toku_logger_rollback_is_open (TOKULOGGER logger) {
    return logger->rollback_cachefile != NULL;
}

// SOSP TODO: add the size of a sync_unbound_insert log message to
// logger->inbuf.n_in_buf to avoid a "deadlock" when doing a
// capacity log flush
void toku_logger_append_ubi_entry(TOKULOGGER logger, struct ubi_entry *entry)
//Entry and exit: hold input_lock
{
    toku_list_init(&entry->in_or_out);
    toku_mutex_lock(&logger->ubi_lock);
    toku_list_push(logger->ubi_in, &entry->in_or_out);
    logger->n_unbound_inserts++;
    toku_mutex_unlock(&logger->ubi_lock);
}

//Entry and exit: hold input_lock
struct ubi_entry *toku_alloc_ubi_entry(ubi_state_t state, LSN lsn, MSN msn, DBT *k)
{
    struct ubi_entry *XMALLOC(entry);
    //entry->key = keybs;
    entry->lsn = lsn;
    entry->msn = msn;
    entry->state = state;
    //entry->fth = ft_h;
    entry->diskoff = 0;
    entry->size = 0;
    toku_clone_dbt(&entry->key, *k);
    toku_list_init(&entry->in_or_out);
    toku_list_init(&entry->node_list);
    return entry;
}


#define MAX_CACHED_ROLLBACK_NODES 4096

void
toku_logger_initialize_rollback_cache(TOKULOGGER logger, FT ft) {
    toku_free_unused_blocknums(ft->blocktable, ft->h->root_blocknum);
    logger->rollback_cache.init(MAX_CACHED_ROLLBACK_NODES);
}

int
toku_logger_open_rollback(TOKULOGGER logger, CACHETABLE cachetable, bool create) {
    if (!logger->is_open)
        return -1;
    assert(logger->is_open);
    assert(!logger->rollback_cachefile);

    FT_HANDLE t = NULL;   // Note, there is no DB associated with this BRT.
    toku_ft_handle_create(&t);
    int r = toku_ft_handle_open(t, toku_product_name_strings.rollback_cachefile, create, create, cachetable, NULL_TXN);
    if (r == 0) {
        logger->rollback_cachefile = t->ft->cf;
        toku_logger_initialize_rollback_cache(logger, t->ft);
        //Verify it is empty
        //Must have no data blocks (rollback logs or otherwise).
        toku_block_verify_no_data_blocks_except_root(t->ft->blocktable, t->ft->h->root_blocknum);
        bool is_empty;
        is_empty = toku_ft_is_empty_fast(t);
        assert(is_empty);
    } else {
        toku_ft_handle_close(t);
    }
    return r;
}

//  Requires: Rollback cachefile can only be closed immediately after a checkpoint,
//            so it will always be clean (!h->dirty) when about to be closed.
//            Rollback log can only be closed when there are no open transactions,
//            so it will always be empty (no data blocks) when about to be closed.
void toku_logger_close_rollback(TOKULOGGER logger) {
    CACHEFILE cf = logger->rollback_cachefile;  // stored in logger at rollback cachefile open
    if (cf) {
        FT_HANDLE ft_to_close;
        {   //Find "brt"
            logger->rollback_cache.destroy();
            FT CAST_FROM_VOIDP(ft, toku_cachefile_get_userdata(cf));
            //Verify it is safe to close it.
            assert(!ft->h->dirty);  //Must not be dirty.
            toku_free_unused_blocknums(ft->blocktable, ft->h->root_blocknum);
            //Must have no data blocks (rollback logs or otherwise).
            toku_block_verify_no_data_blocks_except_root(ft->blocktable, ft->h->root_blocknum);
            assert(!ft->h->dirty);
            ft_to_close = toku_ft_get_only_existing_ft_handle(ft);
            {
                bool is_empty;
                is_empty = toku_ft_is_empty_fast(ft_to_close);
                assert(is_empty);
            }
            assert(!ft->h->dirty); // it should not have been dirtied by the toku_ft_is_empty test.
        }

        toku_ft_handle_close(ft_to_close);
        //Set as dealt with already.
        logger->rollback_cachefile = NULL;
    }
}

// No locks held on entry
// No locks held on exit.
// No locks are needed, since you cannot legally close the log concurrently with doing anything else.
// TODO: can't fail
int toku_logger_close(TOKULOGGER *loggerp) {
    int r;
    TOKULOGGER logger = *loggerp;
    if (!logger->is_open) {
        goto is_closed;
    }
    ml_lock(&logger->input_lock);
    LSN fsynced_lsn;
    grab_output(logger, &fsynced_lsn);
    logger_write_buffer(logger, &fsynced_lsn);
    if (logger->fd!=-1) {
        /* YZJ: here we need to update the log superblock */
        r = toku_logger_file_size_update_and_verify(logger);
        assert(r == 0);

        if ( logger->write_log_files ) {
            toku_file_fsync_without_accounting(logger->fd);
        }
        r = close(logger->fd);
        assert(r == 0);
    }
    r = close_logdir(logger);
    assert(r == 0);
    logger->fd=-1;
    release_output(logger, fsynced_lsn);

is_closed:
    bjm_destroy(logger->bjm);
    toku_kibbutz_destroy(logger->writeback_kibbutz);
    toku_free(logger->inbuf.buf);
    toku_free(logger->outbuf.buf);
    //toku_free(logger->ubi_hash);
    // before destroying locks they must be left in the unlocked state.
    ml_destroy(&logger->input_lock);
    toku_mutex_destroy(&logger->ubi_lock);
    toku_mutex_destroy(&logger->output_condition_lock);
    toku_cond_destroy(&logger->output_condition);
    toku_cond_destroy(&logger->input_swapped);
    toku_txn_manager_destroy(logger->txn_manager);
    toku_logfilemgr_destroy(&logger->logfilemgr);
    if (global_logger == logger) {
        global_logger = nullptr;
    }
    toku_free(logger);
    *loggerp=0;
    return 0;
}

void toku_logger_shutdown(TOKULOGGER logger) {
    if (logger->is_open) {
        TXN_MANAGER mgr = logger->txn_manager;
        if (toku_txn_manager_num_live_root_txns(mgr) == 0) {
            TXNID last_xid = toku_txn_manager_get_last_xid(mgr);
            toku_log_shutdown(logger, NULL, true, 0, last_xid);
        }
    }
}


// ***********************************************************
// output mutex/condition manipulation routines
// ***********************************************************

static void
wait_till_output_available (TOKULOGGER logger)
// Effect: Wait until output becomes available.
// Implementation hint: Use a pthread_cond_wait.
// Entry: Holds the output_condition_lock (but not the inlock)
// Exit: Holds the output_condition_lock and logger->output_is_available
//
{
    while (!logger->output_is_available) {
        toku_cond_wait(&logger->output_condition, &logger->output_condition_lock);
    }
}
#if NO_INLINE
static void
wait_till_input_has_space (TOKULOGGER logger, int n_bytes_needed)
// Effect: Wait until input has space.
// Implementation hint: Use a pthread_cond_wait.
{
    while (logger->inbuf.n_in_buf + n_bytes_needed > LOGGER_MIN_BUF_SIZE) {
        toku_cond_wait(&logger->input_swapped, &logger->input_lock.lock);
    }
}
#else
static int
max_int (int a, int b)
{
    if (a>b) return a;
    return b;
}
#endif

static void
grab_output(TOKULOGGER logger, LSN *fsynced_lsn)
// Effect: Wait until output becomes available and get permission to modify output.
// Entry: Holds no lock (including not holding the input lock, since we never hold both at once).
// Exit:  Hold permission to modify output (but none of the locks).
{
    toku_mutex_lock(&logger->output_condition_lock);
    wait_till_output_available(logger);
    logger->output_is_available = false;
    if (fsynced_lsn) {
        *fsynced_lsn = logger->fsynced_lsn;
    }
    toku_mutex_unlock(&logger->output_condition_lock);
}

static bool
wait_till_output_already_written_or_output_buffer_available(TOKULOGGER logger,
                                                            LSN lsn,
                                                            LSN *fsynced_lsn
                                                            )
// Effect: Wait until either the output is available or the lsn has been written.
//  Return true iff the lsn has been written.
//  If returning true, then on exit we don't hold output permission.
//  If returning false, then on exit we do hold output permission.
// Entry: Hold no locks.
// Exit: Hold the output permission if returns false.
{
    bool result;
    toku_mutex_lock(&logger->output_condition_lock);
    while (1) {
        if (logger->fsynced_lsn.lsn >= lsn.lsn) { // we can look at the fsynced lsn since we have the lock.
            result = true;
            break;
        }
        if (logger->output_is_available) {
            logger->output_is_available = false;
            result = false;
            break;
        }
        // otherwise wait for a good time to look again.
        toku_cond_wait(&logger->output_condition, &logger->output_condition_lock);
    }
    *fsynced_lsn = logger->fsynced_lsn;
    toku_mutex_unlock(&logger->output_condition_lock);
    return result;
}

//TODO:
//static
 void
release_output (TOKULOGGER logger, LSN fsynced_lsn)
// Effect: Release output permission.
// Entry: Holds output permissions, but no locks.
// Exit: Holds neither locks nor output permission.
{
    toku_mutex_lock(&logger->output_condition_lock);
    logger->output_is_available = true;
    if (logger->fsynced_lsn.lsn < fsynced_lsn.lsn) {
        logger->fsynced_lsn = fsynced_lsn;
    }
    toku_cond_broadcast(&logger->output_condition);
    toku_mutex_unlock(&logger->output_condition_lock);
}

//static void
void
swap_inbuf_outbuf (TOKULOGGER logger, unsigned int *n_ubi)
// Effect: Swap the inbuf and outbuf (including the unbound_insert lists)
//         Reserve LSNs for the existing unbound_insert_sync messages in the outbuf
// Entry and exit: Hold the input lock and permission to modify output.
{
    struct logbuf tmp = logger->inbuf;
    logger->inbuf = logger->outbuf;
    logger->outbuf = tmp;
    assert(logger->inbuf.n_in_buf == 0);

    toku_mutex_lock(&logger->ubi_lock);
    struct toku_list *tmp2 = logger->ubi_in;
    logger->ubi_in = logger->ubi_out;
    logger->ubi_out = tmp2;
    assert(toku_list_empty(logger->ubi_in));
    *n_ubi = logger->n_unbound_inserts;
    logger->n_unbound_inserts = 0;
    toku_mutex_unlock(&logger->ubi_lock);

    logger->unbound_insert_lsn = logger->lsn;
    logger->lsn.lsn += *n_ubi;
}

static int logger_has_unbound_msg_since_last_flush(TOKULOGGER logger) {
	return !toku_list_empty(logger->ubi_out);
}

static int
write_unbound_insert_nodes(TOKULOGGER logger)
{
    if(logger_has_unbound_msg_since_last_flush(logger)) {

    	CHECKPOINTER cp = toku_cachetable_get_checkpointer(logger->ct);
	int r;

        //printf("%s:%d going to start a partial chkpt\n", __func__, __LINE__);
	if(is_checkpoint_goingon()) {
	        //printf("%s:%d Yes, it is going on a chkpt\n", __func__, __LINE__);
    		r = toku_partial_checkpoint_locked(cp, logger, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT);
	} else {
	        //printf("%s:%d No, it is not going on a chkpt\n", __func__, __LINE__);
    		r = toku_checkpoint(cp, logger, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT, true);
	}
	//printf("%s:%d finished a partial chkpt\n", __func__, __LINE__);
	assert_zero(r);
   }
    return 0;
}

//TODO: Bill
static void
toku_logger_assert_space_in_outbuf(TOKULOGGER logger, unsigned int buflen, unsigned int n)
//Effect complain if there is no space in the outbuf
{
  // we are guaranteeing that there is space in the outbuf...
    if ((logger->outbuf.n_in_buf + buflen*n) > logger->outbuf.buf_size) {
        #if 0
	printf("We do not have enough space in our outbuf."
               "Bill said he would fix this, but currently we hack it.\n");
	#endif
 //       assert(0);

	logger->outbuf.buf = (char *)toku_xrealloc(logger->outbuf.buf, logger->outbuf.buf_size,
                                                   logger->outbuf.n_in_buf+buflen*n);
 	logger->outbuf.buf_size = logger->outbuf.n_in_buf + buflen*n;
    }
}

static void
toku_logger_restore_outbuf_size(TOKULOGGER logger, unsigned int old_buf_size) {
	if(logger->outbuf.buf_size > old_buf_size) {
            logger->outbuf.buf = (char*) toku_xrealloc(logger->outbuf.buf, logger->outbuf.buf_size,
                                                       old_buf_size);
            logger->outbuf.buf_size = old_buf_size;
	} else if(logger->outbuf.buf_size < old_buf_size) {
            assert(0);
	}
}
//TODO: this is modified from log_code.cc
//toku_log_sync_unbound_insert. We should probably fix the
//logformat.cc generation code?
// Entry and exit: Holds permission to modify output (and doesn't let it go, so it's ok to also hold the inlock).
static void out_logbuf_bind_one_insert (TOKULOGGER logger, MSN msn_in_tree,
                                        LSN lsn_of_enq, DISKOFF offset,
                                        DISKOFF size)
// wkj: The only caller should be outbuf_append_unbound_inserts
{

  if (logger == NULL) {
     return;
  }

  assert(logger->write_log_files);

  const unsigned int buflen= (+4 // len at the beginning
                              +1 // log command
                              +8 // lsn
                              +toku_logsizeof_MSN(msn_in_tree)
                              +toku_logsizeof_LSN(lsn_of_enq)
                              +toku_logsizeof_DISKOFF(offset)
                              +toku_logsizeof_DISKOFF(size)
                              +8 // crc + len
                     );
  struct wbuf wbuf;
  wbuf_nocrc_init(&wbuf, logger->outbuf.buf+logger->outbuf.n_in_buf, buflen);
  wbuf_nocrc_int(&wbuf, buflen);
  wbuf_nocrc_char(&wbuf, 'S');
  logger->unbound_insert_lsn.lsn++;
  logger->outbuf.max_lsn_in_buf = logger->unbound_insert_lsn;
  wbuf_nocrc_LSN(&wbuf, logger->unbound_insert_lsn);
  wbuf_nocrc_MSN(&wbuf, msn_in_tree);
  wbuf_nocrc_LSN(&wbuf, lsn_of_enq);
  wbuf_nocrc_DISKOFF(&wbuf, offset);
  wbuf_nocrc_DISKOFF(&wbuf, size);
  wbuf_nocrc_int(&wbuf, x1764_memory(wbuf.buf, wbuf.ndone));
  wbuf_nocrc_int(&wbuf, buflen);
  assert(wbuf.ndone==buflen);
  logger->outbuf.n_in_buf += buflen; //??? do we need this or have we
                                     //already done the accounting?
}


//TODO: Bill
static int
outbuf_append_unbound_inserts(TOKULOGGER logger, unsigned int n_unbound_inserts)
//Pre: all of the "unbound_inserts" in @unbound_inserts should be bound at this point.
//Effect: This function appends sync_unbound_insert messages (which contain the physical locations of the insert values) to logger->outbuf.
// Entry and exit: Holds permission to modify output (and doesn't let it go, so it's ok to also hold the inlock).
{
    struct ubi_entry *entry;
    unsigned int buflen = (+4 // len at the beginning
                              +1 // log command
                              +8 // lsn
                              +toku_logsizeof_MSN(ZERO_MSN) //dont care the actual value
                              +toku_logsizeof_LSN(ZERO_LSN)
                              +toku_logsizeof_DISKOFF(uint64_t(0))
                              +toku_logsizeof_DISKOFF(uint64_t(0))
                              +8 // crc + len
                     );
    struct toku_list *head = logger->ubi_out;
    struct toku_list *lst = head->next;
    toku_logger_assert_space_in_outbuf(logger, buflen, n_unbound_inserts);
    while (lst != head) {
        entry = toku_list_struct(lst, struct ubi_entry, in_or_out);
        //debug only
        if (!ubi_entry_is_bound(entry)) {
            printf("entry unbound!\n");
            printf("\tstate:%d, MSN: %" PRIu64 ", (DISKOFF)offset: %" PRIu64 ", "
                   "(DISKOFF)size: %" PRIu64 "\n", entry->state, entry->msn.msn,
                   entry->diskoff, entry->size);
            return -1;
        }
        //assert the size and realloc the logger outbuf

        out_logbuf_bind_one_insert(logger, entry->msn, entry->lsn,
                                   entry->diskoff, entry->size);
        lst = lst->next;
    }

    return 0;
}

static bool (* logger_write_ubi_test_callback) (TOKULOGGER, void *) = NULL;
static void * ubi_callback_extra = NULL;

void set_logger_write_ubi_test_callback(bool (*cb) (TOKULOGGER, void *), void *extra) {
	logger_write_ubi_test_callback = cb;
	ubi_callback_extra = extra;
}

//TODO: make static again
//static
void
write_outbuf_to_logfile (TOKULOGGER logger, LSN *UU(fsynced_lsn), unsigned int n_unbound_inserts, int update_log_start)
// Effect:  If any unbound_inserts exist, write the nodes containing those messages. Then append sync messages for those unbound_inserts to the outbuf. Then write the contents of outbuf to logfile.  Don't necessarily fsync (but it might, in which case fynced_lsn is updated).
//  If the logfile gets too big, open the next one (that's the case where an fsync might happen).
// Entry and exit: Holds permission to modify output (and doesn't let it go, so it's ok to also hold the inlock).
{
    unsigned int old_buf_size = logger->outbuf.buf_size;
    if (!toku_list_empty(logger->ubi_out)) {
        int r = write_unbound_insert_nodes(logger);
        assert_zero(r);

	//just for testing
	if(logger_write_ubi_test_callback)
	logger_write_ubi_test_callback(logger, ubi_callback_extra);

        // need to figure out what to do if this pushes us over outbuf capacity...
	r = outbuf_append_unbound_inserts(logger, n_unbound_inserts);
	assert_zero(r);

        // cleanup outlist -- Done.
	toku_list * head = logger->ubi_out;
	toku_mutex_lock(&logger->ubi_lock);
	while(!toku_list_empty(head)){
		toku_list *item = toku_list_pop(head);
		struct ubi_entry *entry = toku_list_struct(item, struct ubi_entry, in_or_out);
		destroy_ubi_entry(entry);
	}

	toku_mutex_unlock(&logger->ubi_lock);
    }

    if (logger->outbuf.n_in_buf>0) {
        // Write the outbuf to disk, take accounting measurements
        tokutime_t io_t0 = toku_time_now();
        if (logger->write_log_files) {
            if (logger->n_in_file + logger->outbuf.n_in_buf > logger->lg_max) {
                toku_os_full_write(logger->fd, logger->outbuf.buf, logger->lg_max - logger->n_in_file);
                off_t fp = lseek(logger->fd, sizeof(struct log_super_block), SEEK_SET);
                assert(fp == sizeof(struct log_super_block));
                toku_os_full_write(logger->fd, logger->outbuf.buf + (logger->lg_max - logger->n_in_file), logger->outbuf.n_in_buf + (logger->n_in_file - logger->lg_max));
            } else {
                toku_os_full_write(logger->fd, logger->outbuf.buf, logger->outbuf.n_in_buf);
            }
        }
        tokutime_t io_t1 = toku_time_now();
        logger->num_writes_to_disk++;
        logger->bytes_written_to_disk += logger->outbuf.n_in_buf;
        logger->time_spent_writing_to_disk += (io_t1 - io_t0);

        assert(logger->outbuf.max_lsn_in_buf.lsn > logger->written_lsn.lsn); // since there is something in the buffer, its LSN must be bigger than what's previously written.
        logger->written_lsn = logger->outbuf.max_lsn_in_buf;
        logger->n_in_file += logger->outbuf.n_in_buf;
        logger->n_in_file = logger->n_in_file > logger->lg_max ? (logger->n_in_file - logger->lg_max) + sizeof(struct log_super_block) : logger->n_in_file;
        logger->outbuf.n_in_buf = 0;

        int r;
        if (update_log_start) {
            r = toku_logger_update_both_and_verify(logger);
        } else {
            r = toku_logger_file_size_update_and_verify(logger);
        }

        assert(r == 0);
	toku_logger_restore_outbuf_size(logger, old_buf_size);

    }
}

static bool (* logger_unbound_test_callback) (void *) = NULL;
static void * callback_extra = NULL;

void set_logger_unbound_test_callback(bool (*cb) (void *), void *extra) {
	logger_unbound_test_callback = cb;
	callback_extra = extra;
}

#if NO_INLINE
void
toku_logger_make_space_in_inbuf (TOKULOGGER logger, int n_bytes_needed)
// Entry: Holds the inlock
// Exit:  Holds the inlock
// Effect: Upon exit, the inlock is held and there are at least n_bytes_needed in the buffer.
//  May release the inlock (and then reacquire it), so this is not atomic.
//  May obtain the output lock and output permission (but if it does so, it will have released the inlock, since we don't hold both locks at once).
//   (But may hold output permission and inlock at the same time.)
// Implementation hint: Makes space in the inbuf, possibly by writing the inbuf to disk or increasing the size of the inbuf.  There might not be an fsync.
// Arguments:  logger:         the logger (side effects)
//             n_bytes_needed: how many bytes to make space for.
{

    if (logger->inbuf.n_in_buf + n_bytes_needed <= LOGGER_MIN_BUF_SIZE) {
        return;
    }
    // we wait until the in buf has space
    wait_till_input_has_space(logger, n_bytes_needed);
}

#else
void
toku_logger_make_space_in_inbuf (TOKULOGGER logger, int n_bytes_needed)
// Entry: Holds the inlock
// Exit:  Holds the inlock
// Effect: Upon exit, the inlock is held and there are at least n_bytes_needed in the buffer.
//  May release the inlock (and then reacquire it), so this is not atomic.
//  May obtain the output lock and output permission (but if it does so, it will have released the inlock, since we don't hold both locks at once).
//   (But may hold output permission and inlock at the same time.)
// Implementation hint: Makes space in the inbuf, possibly by writing the inbuf to disk or increasing the size of the inbuf.  There might not be an fsync.
// Arguments:  logger:         the logger (side effects)
//             n_bytes_needed: how many bytes to make space for.
{

if(!logger_unbound_test_callback||!logger_unbound_test_callback(callback_extra))  {
    if (logger->inbuf.n_in_buf + n_bytes_needed <= LOGGER_MIN_BUF_SIZE) {
        return;
    }
    ml_unlock(&logger->input_lock);
    LSN fsynced_lsn;
    grab_output(logger, &fsynced_lsn);

    ml_lock(&logger->input_lock);
    // Some other thread may have written the log out while we didn't have the lock.  If we have space now, then be happy.
    if (logger->inbuf.n_in_buf + n_bytes_needed <= LOGGER_MIN_BUF_SIZE) {
        release_output(logger, fsynced_lsn);
        return;
    }
    if (logger->inbuf.n_in_buf > 0) {
        // There isn't enough space, and there is something in the buffer, so write the inbuf.
        //unsigned int n = logger->n_unbound_inserts;
	//swap_inbuf_outbuf(logger);
        unsigned int n;
        swap_inbuf_outbuf(logger, &n);

	//printf("%s:%d going to call write_outbuf_to_logfile\n", __func__, __LINE__);
        // Don't release the inlock in this case, because we don't want to get starved.
	write_outbuf_to_logfile(logger, &fsynced_lsn, n, 0);
    }
    // the inbuf is empty.  Make it big enough (just in case it is somehow smaller than a single log entry).
    if (n_bytes_needed > logger->inbuf.buf_size) {
        assert(n_bytes_needed < (1<<30)); // it seems unlikely to work if a logentry gets that big.
        int new_size = max_int(logger->inbuf.buf_size * 2, n_bytes_needed); // make it at least twice as big, and big enough for n_bytes
        assert(new_size < (1<<30));
        XREALLOC_N(logger->inbuf.buf_size, new_size, logger->inbuf.buf);
        logger->inbuf.buf_size = new_size;
    }
    release_output(logger, fsynced_lsn);
} else {

    ml_unlock(&logger->input_lock);
    LSN fsynced_lsn;
    grab_output(logger, &fsynced_lsn);

    ml_lock(&logger->input_lock);
    //unsigned int n = logger->n_unbound_inserts;
    //swap_inbuf_outbuf(logger);
    unsigned int n;
    swap_inbuf_outbuf(logger, &n);

  //  printf("%s:%d going to call write_outbuf_to_logfile\n", __func__, __LINE__);
    write_outbuf_to_logfile(logger, &fsynced_lsn, n, 0);

     // the inbuf is empty.  Make it big enough (just in case it is somehow smaller than a single log entry).
    if (n_bytes_needed > logger->inbuf.buf_size) {
        assert(n_bytes_needed < (1<<30)); // it seems unlikely to work if a logentry gets that big.
        int new_size = max_int(logger->inbuf.buf_size * 2, n_bytes_needed); // make it at least twice as big, and big enough for n_bytes
        assert(new_size < (1<<30));
        XREALLOC_N(logger->inbuf.buf_size, new_size, logger->inbuf.buf);
        logger->inbuf.buf_size = new_size;
    }
    release_output(logger, fsynced_lsn);
 }
}
#endif

void toku_logger_fsync (TOKULOGGER logger)
// Effect: This is the exported fsync used by ydb.c for env_log_flush.
// Group commit doesn't have to work.
// Entry: Holds no locks
// Exit: Holds no locks
// Implementation note:  Acquire the output condition lock,
// then the output permission,
// then release the output condition lock, then get the input lock.
// Then release everything.
{
    toku_logger_maybe_fsync(logger, logger->inbuf.max_lsn_in_buf, true, false, false);
}

void toku_logger_fsync_if_lsn_not_fsynced (TOKULOGGER logger, LSN lsn) {
    if (logger->write_log_files) {
        toku_logger_maybe_fsync(logger, lsn, true, false, false);
    }
}

int toku_logger_is_open(TOKULOGGER logger) {
    if (logger==0) return 0;
    return logger->is_open;
}

void toku_logger_set_cachetable (TOKULOGGER logger, CACHETABLE ct) {
    logger->ct = ct;
}

int toku_logger_set_lg_max(TOKULOGGER logger, uint32_t lg_max) {
    if (logger==0) return EINVAL; // no logger
    if (logger->is_open) return EINVAL;
    if (lg_max>(1<<31)) return EINVAL; // too big
    logger->lg_max = lg_max;
    return 0;
}
int toku_logger_get_lg_max(TOKULOGGER logger, uint32_t *lg_maxp) {
    if (logger==0) return EINVAL; // no logger
    *lg_maxp = logger->lg_max;
    return 0;
}

int toku_logger_set_lg_bsize(TOKULOGGER logger, uint32_t bsize) {
    if (logger==0) return EINVAL; // no logger
    if (logger->is_open) return EINVAL;
    if (bsize<=0 || bsize>(1<<30)) return EINVAL;
    logger->write_block_size = bsize;
    return 0;
}

// TODO: Put this in portability layer when ready
// in: file pathname that may have a dirname prefix
// return: file leaf name
static char * fileleafname(char *pathname) {
    const char delimiter = '/';
    char *leafname = strrchr(pathname, delimiter);
    if (leafname)
        leafname++;
    else
        leafname = pathname;
    return leafname;
}

static int logfilenamecompare (const void *ap, const void *bp) {
    char *a=*(char**)ap;
    char *a_leafname = fileleafname(a);
    char *b=*(char**)bp;
    char * b_leafname = fileleafname(b);
    int rval;
    bool valid;
    uint64_t num_a = 0;  // placate compiler
    uint64_t num_b = 0;
    uint32_t ver_a = 0;
    uint32_t ver_b = 0;
    valid = is_a_logfile_any_version(a_leafname, &num_a, &ver_a);
    invariant(valid);
    valid = is_a_logfile_any_version(b_leafname, &num_b, &ver_b);
    invariant(valid);
    if (ver_a < ver_b) rval = -1;
    else if (ver_a > ver_b) rval = +1;
    else if (num_a < num_b) rval = -1;
    else if (num_a > num_b) rval = +1;
    else rval = 0;
    return rval;
}

// Return the log files in sorted order
// Return a null_terminated array of strings, and also return the number of strings in the array.
// Requires: Race conditions must be dealt with by caller.  Either call during initialization or grab the output permission.
int toku_logger_find_logfiles (const char *directory, char ***resultp, int *n_logfiles)
{
    int result_limit=2;
    int n_results=0;
    char **MALLOC_N(result_limit, result);
    assert(result!= NULL);

    // DEP 10/15/19: With SFS, we just have one log.  Just hard-code this for now.
    char f_name[] = "log000000000000.tokulog25";
    int fnamelen = strlen(f_name) + strlen(directory) + 2; // One for the slash and one for the trailing NUL.
    char *XMALLOC_N(fnamelen, fname);
    snprintf(fname, fnamelen, "%s/%s", directory, f_name);

    int fd = open(fname, O_CREAT+O_RDWR+O_BINARY, S_IRWXU);
    if (fd < 0) {
        int err;
#ifdef TOKU_LINUX_MODULE
        err = get_error_errno(fd);
#else
        err = get_error_errno();
#endif
        printf("Open log %s failed, case 1 - %d\n", fname, err);
        toku_free(result);
        toku_free(fname);
        return err;
    }
    close(fd);

    uint64_t thisl;
    uint32_t version_ignore;
    if ( (is_a_logfile_any_version(f_name, &thisl, &version_ignore)) ) {
        if (n_results+1>=result_limit) {
            XREALLOC_N(result_limit, result_limit*2, result);
            result_limit*=2;
        }
        result[n_results++] = fname;
    }
    // Return them in increasing order.  Set width to allow for newer log file names ("xxx.tokulog13")
    // which are one character longer than old log file names ("xxx.tokulog2").  The comparison function
    // won't look beyond the terminating NUL, so an extra character in the comparison string doesn't matter.
    // Allow room for terminating NUL after "xxx.tokulog13" even if result[0] is of form "xxx.tokulog2."
    int width = sizeof(result[0]+2);
    qsort(result, n_results, width, logfilenamecompare);
    *resultp    = result;
    *n_logfiles = n_results;
    result[n_results]=0; // make a trailing null
    return 0;
}

static int open_logfile (TOKULOGGER logger)
// Entry and Exit: This thread has permission to modify the output.
{
    char fname[] = "/db/log000000000000.tokulog25";
    long long index = logger->next_log_file_number;
    uint32_t log_end;
    int r;

    if (logger->write_log_files) {
	// YZJ: For SFS, we do not use O_EXCL and O_TRUNC flag
        logger->fd = open(fname, O_CREAT+O_RDWR+O_BINARY, S_IRWXU);
        if (logger->fd < 0) {
            return 1;
        }
        r = toku_verify_logmagic_read_log_end(logger->fd, &log_end);
        if (r)
            goto no_log;
        if (log_end > (1<<31)) {
            printf("The log size is corrupted\n");
            return DB_BADFORMAT;
        }

        logger->fsynced_lsn = logger->written_lsn;
        logger->n_in_file = log_end;

        TOKULOGFILEINFO XMALLOC(lf_info);
        lf_info->index = index;
        lf_info->maxlsn = logger->written_lsn;
        lf_info->version = TOKU_LOG_VERSION;
        r = toku_logfilemgr_add_logfile_info(logger->logfilemgr, lf_info);
        if (r != 0) return r;
        off_t pos = lseek(logger->fd, log_end, SEEK_SET);
#ifdef TOKU_LINUX_MODULE
        if (pos != log_end) return get_error_errno(pos);
#else
        if (pos != log_end) return get_error_errno();
#endif
        return 0;
    } else {
        // DEP 3/9/19: Don't bother opening a file if we don't want to write
        logger->fd = -1;
        return 0;
    }

no_log:
    logger->new_env = true;
    logger->fsynced_lsn = logger->written_lsn;
    logger->n_in_file = sizeof(struct log_super_block);
    // YZJ: Write log super block to make the log brandly new
    toku_logger_header_write(logger);

    if ( logger->write_log_files ) {
        TOKULOGFILEINFO XMALLOC(lf_info);
        lf_info->index = index;
        lf_info->maxlsn = logger->written_lsn;
        lf_info->version = TOKU_LOG_VERSION;
        r = toku_logfilemgr_add_logfile_info(logger->logfilemgr, lf_info);
        if (r!=0) return r;
    } else {
        // DEP 3/9/19: Don't bother opening a file if we don't want to write
        logger->fd = -1;
    }

    return 0;
}

/* SCB (8/26/19): This function is now a no-op because we don't delete log files
   and we would like to transition to SFS, which doesn't support file deletion */
static void delete_logfile(TOKULOGGER logger, long long index, uint32_t version)
// Entry and Exit: This thread has permission to modify the output.
{
    // YZJ: Do nothing here
    (void)logger;
    (void)index;
    (void)version;
}

void toku_logger_maybe_trim_log(TOKULOGGER logger, LSN trim_lsn)
// On entry and exit: No logger locks held.
// Acquires and releases output permission.
{
    LSN fsynced_lsn;
    grab_output(logger, &fsynced_lsn);
    TOKULOGFILEMGR lfm = logger->logfilemgr;
    int n_logfiles = toku_logfilemgr_num_logfiles(lfm);

    TOKULOGFILEINFO lf_info = NULL;

    if ( logger->write_log_files && logger->trim_log_files) {
        while ( n_logfiles > 1 ) { // don't delete current logfile
            uint32_t log_version;
            lf_info = toku_logfilemgr_get_oldest_logfile_info(lfm);
            log_version = lf_info->version;
            if ( lf_info->maxlsn.lsn >= trim_lsn.lsn ) {
                // file contains an open LSN, can't delete this or any newer log files
                break;
            }
            // need to save copy - toku_logfilemgr_delete_oldest_logfile_info free's the lf_info
            long index = lf_info->index;
            toku_logfilemgr_delete_oldest_logfile_info(lfm);
            n_logfiles--;
            delete_logfile(logger, index, log_version);
        }
    }
    release_output(logger, fsynced_lsn);
}

void toku_logger_write_log_files (TOKULOGGER logger, bool write_log_files)
// Called only during initialization (or just after recovery), so no locks are needed.
{
    logger->write_log_files = write_log_files;
}

void toku_logger_trim_log_files (TOKULOGGER logger, bool trim_log_files)
// Called only during initialization, so no locks are needed.
{
    logger->trim_log_files = trim_log_files;
}

bool toku_logger_txns_exist(TOKULOGGER logger)
// Called during close of environment to ensure that transactions don't exist
{
    return toku_txn_manager_txns_exist(logger->txn_manager);
}

void toku_logger_maybe_fsync(TOKULOGGER logger, LSN lsn, int do_fsync, int update_log_start, bool holds_input_lock)
// Effect: If fsync is nonzero, then make sure that the log is flushed and synced at least up to lsn.
// SCB (9/12/19): If update_log_start is nonzero, write the log_super_block to reflect that lsn is the last entry in an old "logical log".
// Entry: Holds input lock iff 'holds_input_lock'.
// The log entry has already been written to the input buffer.
// Exit:  Holds no locks.
// The input lock may be released and then reacquired.
// Thus this function does not run atomically with respect to other threads.
{
    if (holds_input_lock) {
        ml_unlock(&logger->input_lock);
    }
    if (do_fsync) {
        // reacquire the locks (acquire output permission first)
        LSN  fsynced_lsn;
        bool already_done = wait_till_output_already_written_or_output_buffer_available(logger, lsn, &fsynced_lsn);
        if (already_done) {
            return;
        }

        //otherwise we now own the output permission, and our lsn isn't outputed.
        ml_lock(&logger->input_lock);
	//unsigned int n = logger->n_unbound_inserts;
        //swap_inbuf_outbuf(logger);
        unsigned int n;
        swap_inbuf_outbuf(logger, &n);
	toku_cond_broadcast(&logger->input_swapped);
        //release the input lock now, so other threads can fill the inbuf.
        //(Thus enabling group commit.)
        ml_unlock(&logger->input_lock);

	//printf("%s:%d going to call write_outbuf_to_logfile\n", __func__, __LINE__);
        write_outbuf_to_logfile(logger, &fsynced_lsn, n, update_log_start);
        if (fsynced_lsn.lsn < lsn.lsn) {
            // it may have gotten fsynced by the write_outbuf_to_logfile.
            // toku_file_fsync_without_accounting(logger->fd);
            if (logger->write_log_files)
                toku_logger_maybe_sync_internal_no_flags_no_callbacks(logger->fd);
            assert(fsynced_lsn.lsn <= logger->written_lsn.lsn);
            fsynced_lsn = logger->written_lsn;
        }
        // the last lsn is only accessed while holding output permission
        // or else when the log file is old.
        if (logger->write_log_files) {
            toku_logfilemgr_update_last_lsn(logger->logfilemgr, logger->written_lsn);
        }
        release_output(logger, fsynced_lsn);
    }
}

static void
logger_write_buffer(TOKULOGGER logger, LSN *fsynced_lsn)
// Entry:  Holds the input lock and permission to modify output.
// Exit:   Holds only the permission to modify output.
// Effect:  Write the buffers to the output.  If DO_FSYNC is true, then fsync.
// Note: Only called during single-threaded activity from toku_logger_restart, so locks aren't really needed.
{
    //unsigned int n = logger->n_unbound_inserts;
    //swap_inbuf_outbuf(logger);
    unsigned int n;
    swap_inbuf_outbuf(logger, &n);
    toku_cond_broadcast(&logger->input_swapped);
    ml_unlock(&logger->input_lock);
    //printf("%s:%d going to call write_outbuf_to_logfile\n", __func__, __LINE__);
    write_outbuf_to_logfile(logger, fsynced_lsn, n, 0);
    if (logger->write_log_files) {
        toku_file_fsync_without_accounting(logger->fd);
        toku_logfilemgr_update_last_lsn(logger->logfilemgr, logger->written_lsn);  // t:2294
    }
}

int toku_logger_restart(TOKULOGGER logger, LSN lastlsn)
// Entry and exit: Holds no locks (this is called only during single-threaded activity, such as initial start).
{
    int r;

    // flush out the log buffer
    LSN fsynced_lsn;
    grab_output(logger, &fsynced_lsn);
    ml_lock(&logger->input_lock);
    logger_write_buffer(logger, &fsynced_lsn);

    // close the log file
    if ( logger->write_log_files) { // fsyncs don't work to /dev/null
        toku_file_fsync_without_accounting(logger->fd);
        r = close(logger->fd);
        assert(r == 0);
    }
    logger->fd = -1;

    // reset the LSN's to the lastlsn when the logger was opened
    logger->lsn = logger->written_lsn = logger->fsynced_lsn = lastlsn;
    logger->write_log_files = true;
    logger->trim_log_files = false;
    // open a new log file
    r = open_logfile(logger);
    release_output(logger, fsynced_lsn);
    return r;
}

// fname is the iname
void toku_logger_log_fcreate (TOKUTXN txn, const char *fname, FILENUM filenum, uint32_t mode,
        uint32_t treeflags, uint32_t nodesize, uint32_t basementnodesize,
        enum toku_compression_method compression_method) {
    if (txn) {
        BYTESTRING bs_fname = { .len = (uint32_t) strlen(fname), .data = (char *) fname };
        // fsync log on fcreate
        toku_log_fcreate (txn->logger, (LSN*)0, 1, txn, toku_txn_get_txnid(txn), filenum,
                bs_fname, mode, treeflags, nodesize, basementnodesize, compression_method);
    }
}


// We only do fdelete on open ft's, so we pass the filenum here
void toku_logger_log_fdelete (TOKUTXN txn, FILENUM filenum) {
    if (txn) {
        //No fsync.
        toku_log_fdelete (txn->logger, (LSN*)0, 0, txn, toku_txn_get_txnid(txn), filenum);
    }
}



/* fopen isn't really an action.  It's just for bookkeeping.  We need to know the filename that goes with a filenum. */
void toku_logger_log_fopen (TOKUTXN txn, const char * fname, FILENUM filenum, uint32_t treeflags) {
    if (txn) {
        BYTESTRING bs;
        bs.len = strlen(fname);
        bs.data = (char*)fname;
        toku_log_fopen (txn->logger, (LSN*)0, 0, bs, filenum, treeflags);
    }
}

static int toku_fread_uint8_t_nocrclen (FILE *f, uint8_t *v) {
    int vi = fgetc(f);
    if (vi == EOF) {
        int r = fseek(f, sizeof(struct log_super_block), SEEK_SET);
        assert(r == 0);
        vi = fgetc(f);
    }
    uint8_t vc=(uint8_t)vi;
    *v = vc;
    return 0;
}

int toku_fread_uint8_t (FILE *f, uint8_t *v, struct x1764 *mm, uint32_t *len) {
    int vi = fgetc(f);
    if (vi == EOF) {
        int r = fseek(f, sizeof(struct log_super_block), SEEK_SET);
        assert(r == 0);
        vi = fgetc(f);
    }
    uint8_t vc=(uint8_t)vi;
    x1764_add(mm, &vc, 1);
    (*len)++;
    *v = vc;
    return 0;
}

int toku_fread_uint32_t_nocrclen (FILE *f, uint32_t *v) {
    uint32_t result;
    uint8_t *cp = (uint8_t*)&result;
    int r;
    r = toku_fread_uint8_t_nocrclen (f, cp+0); if (r!=0) return r;
    r = toku_fread_uint8_t_nocrclen (f, cp+1); if (r!=0) return r;
    r = toku_fread_uint8_t_nocrclen (f, cp+2); if (r!=0) return r;
    r = toku_fread_uint8_t_nocrclen (f, cp+3); if (r!=0) return r;
    *v = toku_dtoh32(result);

    return 0;
}
int toku_fread_uint32_t (FILE *f, uint32_t *v, struct x1764 *checksum, uint32_t *len) {
    uint32_t result;
    uint8_t *cp = (uint8_t*)&result;
    int r;
    r = toku_fread_uint8_t (f, cp+0, checksum, len); if(r!=0) return r;
    r = toku_fread_uint8_t (f, cp+1, checksum, len); if(r!=0) return r;
    r = toku_fread_uint8_t (f, cp+2, checksum, len); if(r!=0) return r;
    r = toku_fread_uint8_t (f, cp+3, checksum, len); if(r!=0) return r;
    *v = toku_dtoh32(result);
    return 0;
}

int toku_fread_uint64_t (FILE *f, uint64_t *v, struct x1764 *checksum, uint32_t *len) {
    uint32_t v1,v2;
    int r;
    r=toku_fread_uint32_t(f, &v1, checksum, len);    if (r!=0) return r;
    r=toku_fread_uint32_t(f, &v2, checksum, len);    if (r!=0) return r;
    *v = (((uint64_t)v1)<<32 ) | ((uint64_t)v2);
    return 0;
}

int toku_fread_int64_t (FILE *f, int64_t *v, struct x1764 *checksum, uint32_t *len) {
    uint32_t v1,v2;
    uint64_t tmp;
    int r;
    r=toku_fread_uint32_t(f, &v1, checksum, len);    if (r!=0) return r;
    r=toku_fread_uint32_t(f, &v2, checksum, len);    if (r!=0) return r;
    tmp = (((uint64_t)v1)<<32 ) | ((uint64_t)v2);
    if (tmp <= INT64_MAX) {
        *v = static_cast<int64_t>(tmp);
        return 0;
    }
    set_errno(-EINVAL);
    return -1;
}


int toku_fread_bool (FILE *f, bool *v, struct x1764 *mm, uint32_t *len) {
    uint8_t iv;
    int r = toku_fread_uint8_t(f, &iv, mm, len);
    if (r == 0) {
        *v = (iv!=0);
    }
    return r;
}

int toku_fread_LSN     (FILE *f, LSN *lsn, struct x1764 *checksum, uint32_t *len) {
    return toku_fread_uint64_t (f, &lsn->lsn, checksum, len);
}

int toku_fread_MSN     (FILE *f, MSN *msn, struct x1764 *checksum, uint32_t *len) {
    return toku_fread_uint64_t (f, &msn->msn, checksum, len);
}

int toku_fread_DISKOFF (FILE *f, DISKOFF *d, struct x1764 *checksum, uint32_t *len) {
    return toku_fread_int64_t (f, d, checksum, len);
}

int toku_fread_BLOCKNUM (FILE *f, BLOCKNUM *b, struct x1764 *checksum, uint32_t *len) {
    return toku_fread_uint64_t (f, (uint64_t*)&b->b, checksum, len);
}

int toku_fread_FILENUM (FILE *f, FILENUM *filenum, struct x1764 *checksum, uint32_t *len) {
    return toku_fread_uint32_t (f, &filenum->fileid, checksum, len);
}

int toku_fread_TXNID   (FILE *f, TXNID *txnid, struct x1764 *checksum, uint32_t *len) {
    return toku_fread_uint64_t (f, txnid, checksum, len);
}

int toku_fread_TXNID_PAIR   (FILE *f, TXNID_PAIR *txnid, struct x1764 *checksum, uint32_t *len) {
    TXNID parent;
    TXNID child;
    int r;
    r = toku_fread_TXNID(f, &parent, checksum, len); if (r != 0) { return r; }
    r = toku_fread_TXNID(f, &child, checksum, len);  if (r != 0) { return r; }
    txnid->parent_id64 = parent;
    txnid->child_id64 = child;
    return 0;
}


int toku_fread_XIDP    (FILE *f, XIDP *xidp, struct x1764 *checksum, uint32_t *len) {
    // These reads are verbose because XA defined the fields as "long", but we use 4 bytes, 1 byte and 1 byte respectively.
    TOKU_XA_XID *XMALLOC(xid);
    {
        uint32_t formatID;
        int r = toku_fread_uint32_t(f, &formatID,     checksum, len);
        if (r!=0) return r;
        xid->formatID = formatID;
    }
    {
        uint8_t gtrid_length;
        int r = toku_fread_uint8_t (f, &gtrid_length, checksum, len);
        if (r!=0) return r;
        xid->gtrid_length = gtrid_length;
    }
    {
        uint8_t bqual_length;
        int r = toku_fread_uint8_t (f, &bqual_length, checksum, len);
        if (r!=0) return r;
        xid->bqual_length = bqual_length;
    }
    for (int i=0; i< xid->gtrid_length + xid->bqual_length; i++) {
        uint8_t byte;
        int r = toku_fread_uint8_t(f, &byte, checksum, len);
        if (r!=0) return r;
        xid->data[i] = byte;
    }
    *xidp = xid;
    return 0;
}

// fills in the bs with malloced data.
int toku_fread_BYTESTRING (FILE *f, BYTESTRING *bs, struct x1764 *checksum, uint32_t *len) {
    int r=toku_fread_uint32_t(f, (uint32_t*)&bs->len, checksum, len);
    if (r!=0) return r;
    XMALLOC_N(bs->len, bs->data);
    uint32_t i;
    for (i=0; i<bs->len; i++) {
        r=toku_fread_uint8_t(f, (uint8_t*)&bs->data[i], checksum, len);
        if (r!=0) {
            toku_free(bs->data);
            bs->data=0;
            return r;
        }
    }
    return 0;
}

// fills in the fs with malloced data.
int toku_fread_FILENUMS (FILE *f, FILENUMS *fs, struct x1764 *checksum, uint32_t *len) {
    int r=toku_fread_uint32_t(f, (uint32_t*)&fs->num, checksum, len);
    if (r!=0) return r;
    XMALLOC_N(fs->num, fs->filenums);
    uint32_t i;
    for (i=0; i<fs->num; i++) {
        r=toku_fread_FILENUM (f, &fs->filenums[i], checksum, len);
        if (r!=0) {
            toku_free(fs->filenums);
            fs->filenums=0;
            return r;
        }
    }
    return 0;
}

int toku_logprint_LSN (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    LSN v;
    int r = toku_fread_LSN(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=%" PRIu64, fieldname, v.lsn);
    return 0;
}

int toku_logprint_MSN (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    MSN v;
    int r = toku_fread_MSN(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=%" PRIu64, fieldname, v.msn);
    return 0;
}

int toku_logprint_TXNID (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    TXNID v;
    int r = toku_fread_TXNID(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=%" PRIu64, fieldname, v);
    return 0;
}

int toku_logprint_TXNID_PAIR (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    TXNID_PAIR v;
    int r = toku_fread_TXNID_PAIR(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=%" PRIu64 ",%" PRIu64, fieldname, v.parent_id64, v.child_id64);
    return 0;
}

int toku_logprint_XIDP (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    XIDP vp;
    int r = toku_fread_XIDP(inf, &vp, checksum, len);
    if (r!=0) return r;
    fprintf(outf, "%s={formatID=0x%lx gtrid_length=%ld bqual_length=%ld data=", fieldname, vp->formatID, vp->gtrid_length, vp->bqual_length);
    toku_print_bytes(outf, vp->gtrid_length + vp->bqual_length, vp->data);
    fprintf(outf, "}");
    toku_free(vp);
    return 0;
}

int toku_logprint_uint8_t (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    uint8_t v;
    int r = toku_fread_uint8_t(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=%d", fieldname, v);
    if (format) fprintf(outf, format, v);
    else if (v=='\'') fprintf(outf, "('\'')");
    else if (isprint(v)) fprintf(outf, "('%c')", v);
    else {}/*nothing*/
    return 0;
}

int toku_logprint_uint32_t (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    uint32_t v;
    int r = toku_fread_uint32_t(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=", fieldname);
    fprintf(outf, format ? format : "%d", v);
    return 0;
}

int toku_logprint_uint64_t (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    uint64_t v;
    int r = toku_fread_uint64_t(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=", fieldname);
    fprintf(outf, format ? format : "%" PRId64, v);
    return 0;
}

int toku_logprint_int64_t (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    int64_t v;
    int r = toku_fread_int64_t(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=", fieldname);
    fprintf(outf, format ? format : "%" PRId64, v);
    return 0;
}

int toku_logprint_bool (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    bool v;
    int r = toku_fread_bool(inf, &v, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=%s", fieldname, v ? "true" : "false");
    return 0;

}

void toku_print_BYTESTRING (FILE *outf, uint32_t len, char *data) {
    fprintf(outf, "{len=%u data=", len);
    toku_print_bytes(outf, len, data);
    fprintf(outf, "}");

}

int toku_logprint_BYTESTRING (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    BYTESTRING bs;
    int r = toku_fread_BYTESTRING(inf, &bs, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=", fieldname);
    toku_print_BYTESTRING(outf, bs.len, bs.data);
    toku_free(bs.data);
    return 0;
}

int toku_logprint_BLOCKNUM (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    return toku_logprint_uint64_t(outf, inf, fieldname, checksum, len, format);

}

int toku_logprint_DISKOFF (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    return toku_logprint_int64_t(outf, inf, fieldname, checksum, len, format);

}

int toku_logprint_FILENUM (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format) {
    return toku_logprint_uint32_t(outf, inf, fieldname, checksum, len, format);

}

static void
toku_print_FILENUMS (FILE *outf, uint32_t num, FILENUM *filenums) {
    fprintf(outf, "{num=%u filenums=\"", num);
    uint32_t i;
    for (i=0; i<num; i++) {
        if (i>0)
            fprintf(outf, ",");
        fprintf(outf, "0x%" PRIx32, filenums[i].fileid);
    }
    fprintf(outf, "\"}");

}

int toku_logprint_FILENUMS (FILE *outf, FILE *inf, const char *fieldname, struct x1764 *checksum, uint32_t *len, const char *format __attribute__((__unused__))) {
    FILENUMS bs;
    int r = toku_fread_FILENUMS(inf, &bs, checksum, len);
    if (r!=0) return r;
    fprintf(outf, " %s=", fieldname);
    toku_print_FILENUMS(outf, bs.num, bs.filenums);
    toku_free(bs.filenums);
    return 0;
}

int toku_read_and_print_logmagic (FILE *f, uint32_t *versionp) {
    {
        char magic[8];
        int r=fread(magic, 1, 8, f);
        if (r!=8) {
            return DB_BADFORMAT;
        }
        if (memcmp(magic, "tokulogg", 8)!=0) {
            return DB_BADFORMAT;
        }
    }
    {
        int version;
            int r=fread(&version, 1, 4, f);
        if (r!=4) {
            return DB_BADFORMAT;
        }
        printf("tokulog v.%u\n", toku_ntohl(version));
        //version MUST be in network order regardless of disk order
        *versionp=toku_ntohl(version);
    }
    return 0;
}

int toku_read_logmagic (FILE *f, uint32_t *versionp) {
    {
        char magic[8];
        int r=fread(magic, 1, 8, f);
        if (r!=8) {
            return DB_BADFORMAT;
        }
        if (memcmp(magic, "tokulogg", 8)!=0) {
            return DB_BADFORMAT;
        }
    }
    {
        int version;
            int r=fread(&version, 1, 4, f);
        if (r!=4) {
            return DB_BADFORMAT;
        }
        *versionp=toku_ntohl(version);
    }
    return 0;
}

TXNID_PAIR toku_txn_get_txnid (TOKUTXN txn) {
    TXNID_PAIR tp = { .parent_id64 = TXNID_NONE, .child_id64 = TXNID_NONE};
    if (txn==0) return tp;
    else return txn->txnid;
}

LSN toku_logger_last_lsn(TOKULOGGER logger) {
    return logger->lsn;
}

TOKULOGGER toku_txn_logger (TOKUTXN txn) {
    return txn ? txn->logger : 0;
}

void toku_txnid2txn(TOKULOGGER logger, TXNID_PAIR txnid, TOKUTXN *result) {
    TOKUTXN root_txn = NULL;
    toku_txn_manager_suspend(logger->txn_manager);
    toku_txn_manager_id2txn_unlocked(logger->txn_manager, txnid, &root_txn);
    if (root_txn == NULL || root_txn->txnid.child_id64 == txnid.child_id64) {
        *result = root_txn;
    }
    else if (root_txn != NULL) {
        root_txn->child_manager->suspend();
        root_txn->child_manager->find_tokutxn_by_xid_unlocked(txnid, result);
        root_txn->child_manager->resume();
    }
    toku_txn_manager_resume(logger->txn_manager);
}

TOKUTXN toku_logger_txn_parent (TOKUTXN txn) {
    return txn->parent;
}

void toku_logger_note_checkpoint(TOKULOGGER logger, LSN lsn) {
    logger->last_completed_checkpoint_lsn = lsn;
}

///////////////////////////////////////////////////////////////////////////////////
// Engine status
//
// Status is intended for display to humans to help understand system behavior.
// It does not need to be perfectly thread-safe.

static LOGGER_STATUS_S logger_status;

#define STATUS_INIT(k,c,t,l,inc) TOKUDB_STATUS_INIT(logger_status, k, c, t, "logger: " l, inc)

static void
status_init(void) {
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.
    STATUS_INIT(LOGGER_NEXT_LSN,     nullptr, UINT64,  "next LSN", TOKU_ENGINE_STATUS);
    STATUS_INIT(LOGGER_NUM_WRITES,                  LOGGER_WRITES, UINT64,  "writes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(LOGGER_BYTES_WRITTEN,               LOGGER_WRITES_BYTES, UINT64,  "writes (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(LOGGER_UNCOMPRESSED_BYTES_WRITTEN,  LOGGER_WRITES_UNCOMPRESSED_BYTES, UINT64,  "writes (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(LOGGER_TOKUTIME_WRITES,             LOGGER_WRITES_SECONDS, TOKUTIME,  "writes (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    logger_status.initialized = true;
}
#undef STATUS_INIT

#define STATUS_VALUE(x) logger_status.status[x].value.num

void
toku_logger_get_status(TOKULOGGER logger, LOGGER_STATUS statp) {
    if (!logger_status.initialized)
        status_init();
    if (logger) {
        STATUS_VALUE(LOGGER_NEXT_LSN)    = logger->lsn.lsn;
        STATUS_VALUE(LOGGER_NUM_WRITES)  = logger->num_writes_to_disk;
        STATUS_VALUE(LOGGER_BYTES_WRITTEN)  = logger->bytes_written_to_disk;
        // No compression on logfiles so the uncompressed size is just number of bytes written
        STATUS_VALUE(LOGGER_UNCOMPRESSED_BYTES_WRITTEN)  = logger->bytes_written_to_disk;
        STATUS_VALUE(LOGGER_TOKUTIME_WRITES) = logger->time_spent_writing_to_disk;
    }
    *statp = logger_status;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Used for upgrade:
// if any valid log files exist in log_dir, then
//   set *found_any_logs to true and set *version_found to version number of latest log
int
toku_get_version_of_logs_on_disk(const char *log_dir, bool *found_any_logs, uint32_t *version_found) {
    // DEP 10/15/19: With SFS, we only expect one log file to exist.  Just test whether it exists
    // for now.  We will worry about upgrading log formats later.
    char f_name[] = "log000000000000.tokulog25";
    int fnamelen = strlen(f_name) + strlen(log_dir) + 2; // One for the slash and one for the trailing NUL.
    char *fname = (char *)alloca(fnamelen);
    snprintf(fname, fnamelen, "%s/%s", log_dir, f_name);
    int fd = open(fname, O_CREAT+O_RDWR+O_BINARY, S_IRWXU);
    if (fd < 0) {
        int err;
#ifdef TOKU_LINUX_MODULE
        err = get_error_errno(fd);
#else
        err = get_error_errno();
#endif
        printf("Open log %s failed, case 1 - %d\n", fname, err);
        return err;
    }
    close(fd);

    uint32_t this_log_version;
    uint64_t this_log_number;
    bool is_log = is_a_logfile_any_version(f_name, &this_log_number, &this_log_version);
    if (is_log) {
        *version_found = this_log_version;
        *found_any_logs = true;
    }

    return 0;
}

TXN_MANAGER toku_logger_get_txn_manager(TOKULOGGER logger) {
    return logger->txn_manager;
}

#undef STATUS_VALUE
