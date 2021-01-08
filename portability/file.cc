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

#include <toku_portability.h>
#include <unistd.h>
#include <toku_assert.h>
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "memory.h"
#include "toku_time.h"
#include "toku_path.h"
#include <portability/toku_atomic.h>

static int toku_assert_on_write_enospc = 0;
static const int toku_write_enospc_sleep = 1;
static uint32_t toku_write_enospc_current;          // number of threads currently blocked on ENOSPC
static uint64_t toku_write_enospc_total;            // total number of times ENOSPC was returned from an attempt to write
static time_t   toku_write_enospc_last_time;        // timestamp of most recent ENOSPC

void toku_set_assert_on_write_enospc(int do_assert) {
    toku_assert_on_write_enospc = do_assert;
}

void toku_fs_get_write_info(time_t *enospc_last_time, uint64_t *enospc_current, uint64_t *enospc_total) {
    *enospc_last_time = toku_write_enospc_last_time;
    *enospc_current = toku_write_enospc_current;
    *enospc_total = toku_write_enospc_total;
}

static ssize_t (*t_write)(int, const void *, size_t);
static ssize_t (*t_full_write)(int, const void *, size_t);
static ssize_t (*t_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*t_full_pwrite)(int, const void *, size_t, off_t, bool);
#ifndef TOKU_LINUX_MODULE
static FILE *  (*t_fdopen)(int, const char *);
#endif
static FILE *  (*t_fopen)(const char *, const char *);
static int     (*t_open)(const char *, int, int);
static int     (*t_fclose)(FILE *);
static ssize_t (*t_read)(int, void *, size_t);
static ssize_t (*t_pread)(int, void *, size_t, off_t);

void toku_set_func_write (ssize_t (*write_fun)(int, const void *, size_t)) {
    t_write = write_fun;
}

void toku_set_func_full_write (ssize_t (*write_fun)(int, const void *, size_t)) {
    t_full_write = write_fun;
}

void toku_set_func_pwrite (ssize_t (*pwrite_fun)(int, const void *, size_t, off_t)) {
    t_pwrite = pwrite_fun;
}

void toku_set_func_full_pwrite (ssize_t (*pwrite_fun)(int, const void *, size_t, off_t, bool)) {
    t_full_pwrite = pwrite_fun;
}

#ifndef TOKU_LINUX_MODULE
void toku_set_func_fdopen(FILE * (*fdopen_fun)(int, const char *)) {
    t_fdopen = fdopen_fun;
}
#endif

void toku_set_func_fopen(FILE * (*fopen_fun)(const char *, const char *)) {
    t_fopen = fopen_fun;
}

void toku_set_func_open(int (*open_fun)(const char *, int, int)) {
    t_open = open_fun;
}

void toku_set_func_fclose(int (*fclose_fun)(FILE*)) {
    t_fclose = fclose_fun;
}

void toku_set_func_read (ssize_t (*read_fun)(int, void *, size_t)) {
    t_read = read_fun;
}

void toku_set_func_pread (ssize_t (*pread_fun)(int, void *, size_t, off_t)) {
    t_pread = pread_fun;
}

void
toku_os_full_write (int fd, const void *buf, size_t len) {
    const char *bp = (const char *) buf;
    while (len > 0) {
        ssize_t r;
        if (t_full_write) {
            r = t_full_write(fd, bp, len);
        } else {
            r = write(fd, bp, len);
        }
        if (r > 0) {
            len           -= r;
            bp            += r;
        }
        else {
            printf("%s: r=%ld\n", __func__, r);
            assert(false);
        }
    }
    assert(len == 0);
}

int
toku_os_write (int fd, const void *buf, size_t len) {
    const char *bp = (const char *) buf;
    int result = 0;
    while (len > 0) {
        ssize_t r;
        if (t_write) {
            r = t_write(fd, bp, len);
        } else {
            r = write(fd, bp, len);
        }
        if (r < 0) {
#ifdef TOKU_LINUX_MODULE
            result = get_error_errno(r);
#else
            result = errno;
#endif
            break;
        }
        len           -= r;
        bp            += r;
    }
    return result;
}

void
toku_os_full_pwrite (int fd, const void *buf, size_t len, toku_off_t off, bool is_blocking) {
    assert(0==((long long)buf)%512);
    assert((len%512 == 0) && (off%512)==0); // to make pwrite work.
    ssize_t r;

    if (ftfs_is_hdd()) {
        const char *bp = (const char *) buf;
        assert(is_blocking == true);
        while (len > 0) {
            if (t_full_pwrite) {
                r = t_full_pwrite(fd, bp, len, off, is_blocking);
            } else {
                r = pwrite(fd, bp, len, off);
            }
            if (r > 0) {
                len           -= r;
                bp            += r;
                off           += r;
            }
            else {
                printf("%s: r=%ld\n", __func__, r);
                assert(false);
            }
        }
        assert(len == 0);
    } else {
        void (* free_cb)(void *) = (is_blocking) ? nullptr : toku_free;
        r = sb_sfs_dio_write(fd, buf, len, off, free_cb);
        assert(r == len);
    }
}

ssize_t
toku_os_pwrite (int fd, const void *buf, size_t len, toku_off_t off) {
    assert(0==((long long)buf)%512); // these asserts are to ensure that direct I/O will work.
    assert(0==len             %512);
    assert(0==off             %512);
    const char *bp = (const char *) buf;
    ssize_t result = 0;
    while (len > 0) {
        ssize_t r;
        if (t_pwrite) {
            r = t_pwrite(fd, bp, len, off);
        } else {
            r = pwrite(fd, bp, len, off);
        }
        if (r < 0) {
#ifdef TOKU_LINUX_MODULE
            result = get_error_errno(r);
#else
            result = errno;
#endif
            break;
        }
        len           -= r;
        bp            += r;
        off           += r;
    }
    return result;
}

#ifndef TOKU_LINUX_MODULE
FILE * 
toku_os_fdopen(int fildes, const char *mode) {
    FILE * rval;
    if (t_fdopen)
	rval = t_fdopen(fildes, mode);
    else 
	rval = fdopen(fildes, mode);
    return rval;
}
#endif
    

FILE *
toku_os_fopen(const char *filename, const char *mode){
    FILE * rval;
    if (t_fopen)
	rval = t_fopen(filename, mode);
    else
	rval = fopen(filename, mode);
    return rval;
}

int 
toku_os_open(const char *path, int oflag, int mode) {
    int rval;
    if (t_open)
	rval = t_open(path, oflag, mode);
    else
	rval = open(path, oflag, mode);
    return rval;
}

int
toku_os_open_direct(const char *path, int oflag, int mode) {
    int rval;
#if defined(HAVE_O_DIRECT)
    /* DEP 9/16/14: For now, enable southbound caching so that we get
     * decent read-ahead behavior on streaming reads.  The fix for
     * double-caching is deeper than this flag.
     */
    rval = toku_os_open(path, oflag, mode);
    /* WKJ 10/15: implemented readpages, so northbound file system
     * should be doing readahead now. also implemented sequential
     * reads using the prelocking api and cursor operations return
     * TOKUDB_CURSOR_CONTINUE so we cache basement nodes. turning
     * O_DIRECT back on to test.
     */
//    rval = toku_os_open(path, oflag | O_DIRECT, mode);
#elif defined(HAVE_F_NOCACHE)
    rval = toku_os_open(path, oflag, mode);
    if (rval >= 0) {
        int r = fcntl(rval, F_NOCACHE, 1);
        if (r < 0) {
            perror("setting F_NOCACHE");
        }
    }
#else
# error "No direct I/O implementation found."
#endif
    return rval;
}

int
toku_os_fclose(FILE * stream) {  
    int rval = -1;
    if (t_fclose)
	rval = t_fclose(stream);
    else {                      // if EINTR, retry until success
	while (rval != 0) {
	    rval = fclose(stream);
#ifdef TOKU_LINUX_MODULE
	    if (rval && get_error_errno(rval) != EINTR)
		break;
#else
	    if (rval && (errno != EINTR))
		break;
#endif
	}
    }
    return rval;
}

int 
toku_os_close(int fd) {  // if EINTR, retry until success
    int r = -1;
    while (r != 0) {
	r = close(fd);
	if (r) {
#ifdef TOKU_LINUX_MODULE
	    int rr = get_error_errno(r);
#else
	    int rr = errno;
#endif
	    if (rr!=EINTR) printf("rr=%d (%s)\n", rr, strerror(rr));
	    assert(rr==EINTR);
	}
    }
    return r;
}

ssize_t 
toku_os_read(int fd, void *buf, size_t count) {
    ssize_t r;
    if (t_read)
        r = t_read(fd, buf, count);
    else
        r = read(fd, buf, count);
    return r;
}

ssize_t
toku_os_pread (int fd, void *buf, size_t count, off_t offset) { 
    assert(0==((long long)buf)%512);
    assert(0==count%512);
    assert(0==offset%512);
    ssize_t r;
    if (ftfs_is_hdd()) {
        if (t_pread) {
	    r = t_pread(fd, buf, count, offset);
        } else {
	    r = pread(fd, buf, count, offset);
        }
    } else {
        r = sb_sfs_dio_read(fd, buf, count, offset, nullptr);
        assert(r == count);
    }
    return r;
}

// fsync logic:

// t_fsync exists for testing purposes only
static int (*t_fsync)(int) = 0;
static uint64_t toku_fsync_count;
static uint64_t toku_fsync_time;
static uint64_t toku_long_fsync_threshold = 1000000;
static uint64_t toku_long_fsync_count;
static uint64_t toku_long_fsync_time;
static uint64_t toku_long_fsync_eintr_count;

void toku_set_func_fsync(int (*fsync_function)(int)) {
    t_fsync = fsync_function;
}

void toku_logger_maybe_sync_internal_no_flags_no_callbacks (int fd) {
    uint64_t tstart = toku_current_time_microsec();
    int r = -1;
    uint64_t eintr_count = 0;
    while (r != 0) {
	r = fdatasync(fd);

	if (r) {
#ifndef TOKU_KERNEL_MODULE
            assert(get_error_errno(r) == EINTR);
#else
            assert(get_error_errno() == EINTR);
#endif
            eintr_count++;
	}
    }
    toku_sync_fetch_and_add(&toku_fsync_count, 1);
    uint64_t duration = toku_current_time_microsec() - tstart;
    toku_sync_fetch_and_add(&toku_fsync_time, duration);
    if (duration >= toku_long_fsync_threshold) {
        toku_sync_fetch_and_add(&toku_long_fsync_count, 1);
        toku_sync_fetch_and_add(&toku_long_fsync_time, duration);
        toku_sync_fetch_and_add(&toku_long_fsync_eintr_count, eintr_count);
    }
}

// keep trying if fsync fails because of EINTR
static void file_fsync_internal (int fd) {
    uint64_t tstart = toku_current_time_microsec();
    int r = -1;
    uint64_t eintr_count = 0;
    while (r != 0) {
	//if (t_fsync) {
	    //r = t_fsync(fd);
        //} else {
	    r = fdatasync(fd);
        //}
	if (r) {
#ifndef TOKU_KERNEL_MODULE
            assert(get_error_errno(r) == EINTR);
#else
            assert(get_error_errno() == EINTR);
#endif
            eintr_count++;
	}
    }
    toku_sync_fetch_and_add(&toku_fsync_count, 1);
    uint64_t duration = toku_current_time_microsec() - tstart;
    toku_sync_fetch_and_add(&toku_fsync_time, duration);
    if (duration >= toku_long_fsync_threshold) {
        toku_sync_fetch_and_add(&toku_long_fsync_count, 1);
        toku_sync_fetch_and_add(&toku_long_fsync_time, duration);
        toku_sync_fetch_and_add(&toku_long_fsync_eintr_count, eintr_count);
    }
}

void toku_file_fsync_without_accounting(int fd) {
    file_fsync_internal(fd);
}

int toku_fsync_dir_by_name_without_accounting(const char *dir_name) {
    int fd, r;

    fd = open(dir_name, O_DIRECTORY);
    if (fd < 0) {
#ifdef TOKU_LINUX_MODULE
        r = get_error_errno(fd);
#else
        r = get_error_errno();
#endif
    } else {
        toku_file_fsync_without_accounting(fd);
        r = close(fd);
    }

    return r;
}

// include fsync in scheduling accounting
void toku_file_fsync(int fd) {
    file_fsync_internal (fd);
}

// for real accounting
void toku_get_fsync_times(uint64_t *fsync_count, uint64_t *fsync_time, uint64_t *long_fsync_threshold, uint64_t *long_fsync_count, uint64_t *long_fsync_time) {
    *fsync_count = toku_fsync_count;
    *fsync_time = toku_fsync_time;
    *long_fsync_threshold = toku_long_fsync_threshold;
    *long_fsync_count = toku_long_fsync_count;
    *long_fsync_time = toku_long_fsync_time;
}

int toku_fsync_directory(const char *fname) {
    int result = 0;
    
    // extract dirname from fname
    const char *sp = strrchr(fname, '/');
    size_t len;
    char *dirname = NULL;
    if (sp) {
        resource_assert(sp >= fname);
        len = sp - fname + 1;
        MALLOC_N(len+1, dirname);
        if (dirname == NULL) {
#ifdef TOKU_LINUX_MODULE
            result = ENOMEM;
#else
            result = get_error_errno();
#endif
        } else {
            strncpy(dirname, fname, len);
            dirname[len] = 0;
        }
    } else {
        dirname = toku_strdup(".");
        if (dirname == NULL) {
#ifdef TOKU_LINUX_MODULE
            result = ENOMEM;
#else
            result = get_error_errno();
#endif
        }
    }

    if (result == 0) {
        result = toku_fsync_dir_by_name_without_accounting(dirname);
    }
    toku_free(dirname);
    return result;
}

