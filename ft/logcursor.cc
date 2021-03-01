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

#include "log-internal.h"
#include "logcursor.h"
#include <limits.h>
#include <unistd.h>

#include "logsuperblock.h"

enum lc_direction { LC_FORWARD, LC_BACKWARD, LC_FIRST, LC_LAST };

struct toku_logcursor {
    char *logdir;         // absolute directory name
    char **logfiles;
    int n_logfiles;
    int cur_logfiles_index;
    FILE *cur_fp;
    size_t cur_log_end;
    size_t cur_log_start;
    size_t buffer_size;
    void *buffer;
    bool is_open;
    struct log_entry entry;
    bool entry_valid;
    LSN cur_lsn;
    enum lc_direction last_direction;
};

#define LC_LSN_ERROR (DB_RUNRECOVERY)

void toku_logcursor_print(TOKULOGCURSOR lc)  {
    printf("lc = %p\n", lc);
    printf("  logdir = %s\n", lc->logdir);
    printf("  logfiles = %p\n", lc->logfiles);
    for (int lf=0;lf<lc->n_logfiles;lf++) {
        printf("    logfile[%d] = %p (%s)\n", lf, lc->logfiles[lf], lc->logfiles[lf]);
    }
    printf("  n_logfiles = %d\n", lc->n_logfiles);
    printf("  cur_logfiles_index = %d\n", lc->cur_logfiles_index);
    printf("  cur_fp = %p\n", lc->cur_fp);
    printf("  cur_lsn = %" PRIu64 "\n", lc->cur_lsn.lsn);
    printf("  last_direction = %d\n", (int) lc->last_direction);
}

static int lc_close_cur_logfile(TOKULOGCURSOR lc) {
    int r=0;
    if ( lc->is_open ) {
        r = fclose(lc->cur_fp);
        assert(0==r);
        lc->is_open = false;
    }
    return 0;
}

/* SCB: We read the log superblock from the first 20 bytes of the log file
 * and return the end or the start value, before restoring the cursor.
 */
static toku_off_t lc_file_end(FILE *fp) {
    struct log_super_block *log_sb = NULL;
    ssize_t r;
    toku_off_t ret;

    /* seek to the start of log super block */
    r = fseek(fp, 0, SEEK_SET);
    assert(r == 0);
    log_sb = (struct log_super_block *) toku_xmalloc(sizeof(struct log_super_block)); 
    assert(log_sb != NULL);
    r = fread(log_sb, sizeof(struct log_super_block), 1, fp);
    assert(r == 1);
    r = fseek(fp, 0, SEEK_SET);
    ret = log_sb->log_end;
    toku_free(log_sb);
    return ret;
}

static toku_off_t lc_file_start(FILE *fp) {
    struct log_super_block *log_sb = NULL;
    ssize_t r;
    toku_off_t ret;

    /* seek to the start of log super block */
    r = fseek(fp, 0, SEEK_SET);
    assert(r == 0);
    log_sb = (struct log_super_block *) toku_xmalloc(sizeof(struct log_super_block)); 
    assert(log_sb != NULL);
    r = fread(log_sb, sizeof(struct log_super_block), 1, fp);
    assert(r == 1);
    r = fseek(fp, 0, SEEK_SET);
    ret = log_sb->log_start;
    toku_free(log_sb);
    return ret;
}

// Cat the file and throw away the contents.  This brings the file into the file system cache
// and makes subsequent accesses to it fast.  The intention is to speed up backward scans of the
// file.
static void lc_catfile(const char *fname, void *buffer, size_t buffer_size) {
    int fd = open(fname, O_RDONLY);
    uint32_t logend;
    uint32_t read_bytes = 0;
    int ret;

    if (fd >= 0) {
        ret = toku_verify_logmagic_read_log_end(fd, &logend);
        while (ret == 0 && read_bytes < logend) {
            ssize_t r = read(fd, buffer, buffer_size);
            if ((int)r <= 0)
                break;
            read_bytes += r;
        }
        close(fd);
    }
}

static int lc_open_logfile(TOKULOGCURSOR lc, int index) {
    int r=0;
    assert( !lc->is_open );
    if( index == -1 || index >= lc->n_logfiles) return DB_NOTFOUND;
    lc_catfile(lc->logfiles[index], lc->buffer, lc->buffer_size);
    lc->cur_fp = fopen(lc->logfiles[index], "rb");
    lc->cur_log_end = lc_file_end(lc->cur_fp);
    lc->cur_log_start = lc_file_start(lc->cur_fp);

    if ( lc->cur_fp == NULL ) 
        return DB_NOTFOUND;
    // debug printf("%s:%d %s %p %u\n", __FUNCTION__, __LINE__, lc->logfiles[index], lc->buffer, (unsigned) lc->buffer_size);
#if !TOKU_WINDOWS //Windows reads logs fastest if we use default settings (not use setvbuf to change buffering)
    r = setvbuf(lc->cur_fp, (char *) lc->buffer, _IOFBF, lc->buffer_size);
    assert(r == 0);
#endif
    // position fp past header, ignore 0 length file (t:2384)
    unsigned int version=0;
    if ( lc->cur_log_end >= sizeof(struct log_super_block) ) {
        r = toku_read_logmagic(lc->cur_fp, &version);
        if (r!=0) 
            return DB_BADFORMAT;
        if (version < TOKU_LOG_MIN_SUPPORTED_VERSION || version > TOKU_LOG_VERSION)
            return DB_BADFORMAT;
    }
    if ( lc->cur_log_end >= sizeof(struct log_super_block) ) {
        r = fseek(lc->cur_fp, sizeof(struct log_super_block), SEEK_SET);
        if (r!=0) return r;
    }
    // mark as open
    lc->is_open = true;
    return r;
}

static int lc_check_lsn(TOKULOGCURSOR lc, int dir) {
    int r=0;
    LSN lsn = toku_log_entry_get_lsn(&(lc->entry));
    if (((dir == LC_FORWARD)  && ( lsn.lsn != lc->cur_lsn.lsn + 1 )) ||
        ((dir == LC_BACKWARD) && ( lsn.lsn != lc->cur_lsn.lsn - 1 ))) {
//        int index = lc->cur_logfiles_index;
//        fprintf(stderr, "Bad LSN: %d %s direction = %d, lsn.lsn = %" PRIu64 ", cur_lsn.lsn=%" PRIu64 "\n", 
//                index, lc->logfiles[index], dir, lsn.lsn, lc->cur_lsn.lsn);
        if (tokudb_recovery_trace) 
            printf("DB_RUNRECOVERY: %s:%d r=%d\n", __FUNCTION__, __LINE__, 0);
        return LC_LSN_ERROR;
    }
    lc->cur_lsn.lsn = lsn.lsn;
    return r;
}

// toku_logcursor_create()
//   - returns a pointer to a logcursor

static int lc_create(TOKULOGCURSOR *lc, const char *log_dir) {

    // malloc a cursor
    TOKULOGCURSOR cursor = (TOKULOGCURSOR) toku_xmalloc(sizeof(struct toku_logcursor));
    // find logfiles in logdir
    cursor->is_open = false;
    cursor->cur_logfiles_index = 0;
    cursor->entry_valid = false;
    cursor->buffer_size = 1<<20;                       // use a 1MB stream buffer (setvbuf)
    cursor->buffer = toku_malloc(cursor->buffer_size); // it does not matter if it failes
    // cursor->logdir must be an absolute path
    if (toku_os_is_absolute_name(log_dir)) {
        cursor->logdir = (char *) toku_xmalloc(strlen(log_dir)+1);
        sprintf(cursor->logdir, "%s", log_dir);
    } else {
        /* XXX: DEP 12/16/13 - convert to malloc to get the stack size down.  Probalby won't be used as is anyway, 
         * and could stand to be integrated with the xmalloc below.
         */
        /*
         * XXX: WKJ 9/14/14 - southbound cwd probably incorrect.
         * we don't maintain state well, so always '/'
         */
        char cwd[2] = "/";
        cursor->logdir = (char *) toku_xmalloc(strlen(cwd)+strlen(log_dir)+2);
        sprintf(cursor->logdir, "%s/%s", cwd, log_dir);
    }
    cursor->logfiles = NULL;
    cursor->n_logfiles = 0;
    cursor->cur_fp = NULL;
    cursor->cur_lsn.lsn=0;
    cursor->last_direction=LC_FIRST;
    
    *lc = cursor;
    return 0;
}

static int lc_fix_bad_logfile(TOKULOGCURSOR lc);

int toku_logcursor_create(TOKULOGCURSOR *lc, const char *log_dir) {
    TOKULOGCURSOR cursor;
    int r = lc_create(&cursor, log_dir);
    if ( r!=0 ) 
        return r;

    r = toku_logger_find_logfiles(cursor->logdir, &(cursor->logfiles), &(cursor->n_logfiles));
    if (r!=0) {
        toku_logcursor_destroy(&cursor);
    } else {
	*lc = cursor;
    }
    return r;
}

int toku_logcursor_create_for_file(TOKULOGCURSOR *lc, const char *log_dir, const char *log_file) {
    int r = lc_create(lc, log_dir);
    if ( r!=0 ) 
        return r;

    TOKULOGCURSOR cursor = *lc;
    int fullnamelen = strlen(cursor->logdir) + strlen(log_file) + 3;
    char *XMALLOC_N(fullnamelen, log_file_fullname);
    sprintf(log_file_fullname, "%s/%s", cursor->logdir, log_file);

    cursor->n_logfiles=1;

    char **XMALLOC(logfiles);
    cursor->logfiles = logfiles;
    cursor->logfiles[0] = log_file_fullname;
    *lc = cursor;
    return 0;
}

int toku_logcursor_destroy(TOKULOGCURSOR *lc) {
    int r=0;
    if ( *lc ) {
        if ( (*lc)->entry_valid ) {
            toku_log_free_log_entry_resources(&((*lc)->entry));
            (*lc)->entry_valid = false;
        }
        r = lc_close_cur_logfile(*lc);
        int lf;
        for(lf=0;lf<(*lc)->n_logfiles;lf++) {
            if ( (*lc)->logfiles[lf] ) toku_free((*lc)->logfiles[lf]);
        }
        if ( (*lc)->logfiles ) toku_free((*lc)->logfiles);
        if ( (*lc)->logdir )   toku_free((*lc)->logdir);
        if ( (*lc)->buffer )   toku_free((*lc)->buffer);
        toku_free(*lc);
        *lc = NULL;
    }
    return r;
}

static int lc_log_read(TOKULOGCURSOR lc)
{
    int r = toku_log_fread(lc->cur_fp, &(lc->entry));
    while ( r == EOF ) { 
        // move to next file
        r = lc_close_cur_logfile(lc);                    
        if (r!=0) return r;
        if ( lc->cur_logfiles_index == lc->n_logfiles-1) return DB_NOTFOUND;
        lc->cur_logfiles_index++;
        r = lc_open_logfile(lc, lc->cur_logfiles_index); 
        if (r!=0) return r;
        r = toku_log_fread(lc->cur_fp, &(lc->entry));
    }
    if (r!=0) {
        toku_log_free_log_entry_resources(&(lc->entry));
        time_t tnow = time(NULL);
        if (r==DB_BADFORMAT) {
            fprintf(stderr, "%.24s Tokudb bad log format in %s\n", ctime(&tnow), lc->logfiles[lc->cur_logfiles_index]);
        }
        else {
            fprintf(stderr, "%.24s Tokudb unexpected log format error '%s' in %s\n", ctime(&tnow), strerror(r), lc->logfiles[lc->cur_logfiles_index]);
        }
    }
    return r;
}

static int lc_log_read_backward(TOKULOGCURSOR lc) 
{
    int r = toku_log_fread_backward(lc->cur_fp, &(lc->entry));
    while ( -1 == r) { // if within header length of top of file
        // move to previous file
        r = lc_close_cur_logfile(lc);
        if (r!=0) 
            return r;
        if ( lc->cur_logfiles_index == 0 ) 
            return DB_NOTFOUND;
        lc->cur_logfiles_index--;
        r = lc_open_logfile(lc, lc->cur_logfiles_index);
        if (r!=0) 
            return r;
        // seek to end
	assert(lc->cur_log_end >= sizeof(struct log_super_block));
        r = fseek(lc->cur_fp, lc->cur_log_end, SEEK_SET);
        assert(0==r);
        r = toku_log_fread_backward(lc->cur_fp, &(lc->entry));
    }
    if (r!=0) {
        toku_log_free_log_entry_resources(&(lc->entry));
        time_t tnow = time(NULL);
        if (r==DB_BADFORMAT) {
            fprintf(stderr, "%.24s Tokudb bad log format in %s\n", ctime(&tnow), lc->logfiles[lc->cur_logfiles_index]);
        }
        else {
            fprintf(stderr, "%.24s Tokudb uUnexpected log format error '%s' in %s\n", ctime(&tnow), strerror(r), lc->logfiles[lc->cur_logfiles_index]);
        }
    }
    return r;
}

int toku_logcursor_next(TOKULOGCURSOR lc, struct log_entry **le) {
    int r=0;
    /* SCB (8/27/19): This is mostly for recovery, which expects a DB_NOTFOUND
     * when it is supposed to stop */
    if (lc->cur_fp != NULL) {
        toku_off_t cur_pos = ftell(lc->cur_fp);
        if (cur_pos == lc->cur_log_end) return DB_NOTFOUND;
    }

    if ( lc->entry_valid ) {
        toku_log_free_log_entry_resources(&(lc->entry));
        lc->entry_valid = false;
        if (lc->last_direction == LC_BACKWARD) {
            struct log_entry junk;
            r = toku_log_fread(lc->cur_fp, &junk);
            assert(r == 0);
            toku_log_free_log_entry_resources(&junk);
        }
    } else {
        r = toku_logcursor_first(lc, le);
        return r;
    }
    // read the entry
    r = lc_log_read(lc);                   
    if (r!=0) return r;
    r = lc_check_lsn(lc, LC_FORWARD);  
    if (r!=0) return r;
    lc->last_direction = LC_FORWARD;
    lc->entry_valid = true;
    *le = &(lc->entry);
    return r;
}

int toku_logcursor_prev(TOKULOGCURSOR lc, struct log_entry **le) {
    int r=0;
    if ( lc->entry_valid ) {
        toku_log_free_log_entry_resources(&(lc->entry));
        lc->entry_valid = false;
        if (lc->last_direction == LC_FORWARD) {
            struct log_entry junk;
            r = toku_log_fread_backward(lc->cur_fp, &junk);
            assert(r == 0);
            toku_log_free_log_entry_resources(&junk);
        }
    } else {
        r = toku_logcursor_last(lc, le);
        return r;
    }
    // read the entry
    r = lc_log_read_backward(lc);
    if (r!=0) return r;
    r = lc_check_lsn(lc, LC_BACKWARD);
    if (r!=0) return r;
    lc->last_direction = LC_BACKWARD;
    lc->entry_valid = true;
    *le = &(lc->entry);
    return r;
}

int toku_logcursor_first(TOKULOGCURSOR lc, struct log_entry **le) {
    int r=0;
    if ( lc->entry_valid ) {
        toku_log_free_log_entry_resources(&(lc->entry));
        lc->entry_valid = false;
    }
    // close any but the first log file
    if ( lc->cur_logfiles_index != 0 ) {
        lc_close_cur_logfile(lc);
    }
    // open first log file if needed
    if ( !lc->is_open ) {
        r = lc_open_logfile(lc, 0);
        if (r!=0) 
            return r;
        if (lc->cur_log_end == 0) {
            lc_close_cur_logfile(lc);
            return DB_NOTFOUND;
        }
        lc->cur_logfiles_index = 0;
    }
    // move to "first" entry
    r = fseek(lc->cur_fp, lc->cur_log_start, SEEK_SET);
    if (r != 0) return r;
    // read the entry
    r = lc_log_read(lc);
    if (r!=0) return r;

    r = lc_check_lsn(lc, LC_FIRST);
    if (r!=0) return r;
    lc->last_direction = LC_FIRST;
    lc->entry_valid = true;
    *le = &(lc->entry);
    return r;
}

//get last entry in the logfile specified by logcursor
int toku_logcursor_last(TOKULOGCURSOR lc, struct log_entry **le) {
    int r=0;
    if ( lc->entry_valid ) {
        toku_log_free_log_entry_resources(&(lc->entry));
        lc->entry_valid = false;
    }
    // close any but last log file
    if ( lc->cur_logfiles_index != lc->n_logfiles-1 ) {
        lc_close_cur_logfile(lc);
    }
    // open last log file if needed
    if ( !lc->is_open ) {
        r = lc_open_logfile(lc, lc->n_logfiles-1);
        if (r!=0)
            return r;
        if (lc->cur_log_end == 0 || lc->cur_log_end == sizeof(struct log_super_block)) {
            lc_close_cur_logfile(lc);
            return DB_NOTFOUND;
        }
        lc->cur_logfiles_index = lc->n_logfiles-1;
    }
    while (1) {
        // seek to end
        if (lc->cur_log_end < sizeof(struct log_super_block))
            return DB_BADFORMAT;
        r = fseek(lc->cur_fp, lc->cur_log_end, SEEK_SET);    assert(r==0);
        // read backward
        r = toku_log_fread_backward(lc->cur_fp, &(lc->entry));
        if (r==0) // got a good entry
            break;
        if (r>0) { 
            toku_log_free_log_entry_resources(&(lc->entry));
            // got an error, 
            // probably a corrupted last log entry due to a crash
            // try scanning forward from the beginning to find the last good entry
            time_t tnow = time(NULL);
            fprintf(stderr, "%.24s Tokudb recovery repairing log\n", ctime(&tnow));
            r = lc_fix_bad_logfile(lc);
            if ( r != 0 ) {
                fprintf(stderr, "%.24s Tokudb recovery repair unsuccessful\n", ctime(&tnow));
                return DB_BADFORMAT;
            }
            // try reading again
            r = toku_log_fread_backward(lc->cur_fp, &(lc->entry));
            if (r==0) // got a good entry
                break;
        }
        // move to previous file
        r = lc_close_cur_logfile(lc);
        if (r!=0) 
            return r;
        if ( lc->cur_logfiles_index == 0 ) 
            return DB_NOTFOUND;
        lc->cur_logfiles_index--;
        r = lc_open_logfile(lc, lc->cur_logfiles_index);
        if (r!=0) 
            return r;    
    }
    r = lc_check_lsn(lc, LC_LAST);
    if (r!=0)
        return r;
    lc->last_direction = LC_LAST;
    lc->entry_valid = true;
    *le = &(lc->entry);
    return r;
}

// return 0 if log exists, ENOENT if no log
int 
toku_logcursor_log_exists(const TOKULOGCURSOR lc) {
    int r;

    if (lc->n_logfiles) 
	r = 0;
    else
	r = ENOENT;

    return r;
}

// fix a logfile with a bad last entry
//  - return with fp pointing to end-of-file so that toku_logcursor_last can be retried
static int lc_fix_bad_logfile(TOKULOGCURSOR lc) {
    struct log_entry le;
    unsigned int version=0;
    int r = 0;
    r = fseek(lc->cur_fp, 0, SEEK_SET);                
    if ( r!=0 ) 
        return r;
    r = toku_read_logmagic(lc->cur_fp, &version);      
    if ( r!=0 ) 
        return r;
    if (version != TOKU_LOG_VERSION) 
        return -1;
    r = fseek(lc->cur_fp, lc->cur_log_start, SEEK_SET);
    if  (r != 0) return r;
    toku_off_t last_good_pos;
    toku_off_t temp_pos;
    last_good_pos = ftello(lc->cur_fp);
    while (1) {
        // initialize le 
        //  - reading incomplete entries can result in fields that cannot be freed
        memset(&le, 0, sizeof(le));
        r = toku_log_fread(lc->cur_fp, &le);
        toku_log_free_log_entry_resources(&le);
        if ( r!=0 ) 
            break;
        temp_pos = ftello(lc->cur_fp);
        // SCB (9/13/19): The purpose of this test is to make sure we are only scanning the "logical log".
        if (lc->cur_log_start < lc->cur_log_end) {
        /* SCB (9/13/19): Log file looks like this:
         * [sb|.........st#####################ed.................] (where '#' denotes the "logical log")
         * So in this case we want to bail out if we are before the start or after the end.
         */
            if (temp_pos > lc->cur_log_end || temp_pos < lc->cur_log_start)
                break;
        } else if (lc->cur_log_start > lc->cur_log_end) {
        /* SCB (9/13/19): Log file looks like this:
         * [sb|#########ed.....................st#################] (where '#' denotes the "logical log")
         * So in this case we want to bail out if we are after the end and before the start.
         */
            if (temp_pos < lc->cur_log_end && temp_pos > lc->cur_log_start)
                break;
        } else if (lc->cur_log_start == lc->cur_log_end) {
        /* SCB (9/13/19): Log file looks like this:
         * [sb|..............................(st/ed).................] (where '#' denotes the "logical log")
         * So we should bail out immediately and tell the calling function that there is nothing we can do.
         */
            return DB_BADFORMAT;
        }
        last_good_pos = ftello(lc->cur_fp);
    }
    // now have position of last good entry
    // 1) close the file
    // 2) truncate the file to remove the error
    // 3) reopen the file
    // 4) set the pos to last
    r = lc_close_cur_logfile(lc);                                   
    if ( r!=0 ) 
        return r;
    int fd = open(lc->logfiles[lc->n_logfiles - 1], O_RDWR);  
    assert(fd >= 0);
    if (last_good_pos == lc->cur_log_start)
        last_good_pos = 0;
    r = toku_update_logfile_end(fd, last_good_pos);
    assert(r==0);
    close(fd);
    if ( r!=0 ) 
        return r;
    r = lc_open_logfile(lc, lc->n_logfiles-1);
    if ( r!=0 ) 
        return r;
    r = fseek(lc->cur_fp, lc->cur_log_end, SEEK_SET);
    if ( r!=0 ) 
        return r;
    return 0;
}
