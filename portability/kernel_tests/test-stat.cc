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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <toku_assert.h>
#include <toku_portability.h>
#include <portability/toku_path.h>


#define OFFSETOF(type, field) ((unsigned long) &(((type *) 0)->st_##field))


void print_stat(toku_struct_stat *statbuf);

void print_stat(toku_struct_stat *statbuf)
{
    
        printf( "dev: %lu\n", statbuf->st_dev);
        printf( "ino: %lu\n", statbuf->st_ino);
        printf( "nlink: %lu\n", statbuf->st_nlink);
        printf( "mode: %u\n", statbuf->st_mode);
        printf( "uid: %u\n", statbuf->st_uid);
        printf( "gid: %u\n", statbuf->st_gid);
        printf( "rdev: %lu\n", statbuf->st_rdev);
        printf( "size: %ld\n", statbuf->st_size);
        printf( "blksize: %ld\n", statbuf->st_blksize);
        printf( "blocks: %ld\n", statbuf->st_blocks);
        printf( "atime: %ld\n", statbuf->st_atime);
        printf( "ctime: %ld\n", statbuf->st_ctime);
        printf( "mtime: %ld\n", statbuf->st_mtime);


        printf( "dev: %lu\n",  OFFSETOF(toku_struct_stat, dev));
        printf( "ino: %lu\n",  OFFSETOF(toku_struct_stat, ino));
        printf( "nlink: %lu\n",  OFFSETOF(toku_struct_stat, nlink));
        printf( "mode: %lu\n",  OFFSETOF(toku_struct_stat, mode));
        printf( "uid: %lu\n",  OFFSETOF(toku_struct_stat, uid));
        printf( "gid: %lu\n",  OFFSETOF(toku_struct_stat, gid));
        printf( "rdev: %lu\n",  OFFSETOF(toku_struct_stat, rdev));
        printf( "size: %lu\n",  OFFSETOF(toku_struct_stat, size));
        printf( "blksize: %lu\n",  OFFSETOF(toku_struct_stat, blksize));

        printf( "blocks: %lu\n",  OFFSETOF(toku_struct_stat, blocks));
        printf( "atime: %lu\n",  OFFSETOF(toku_struct_stat, atime));
        printf( "ctime: %lu\n",  OFFSETOF(toku_struct_stat, ctime));
        printf( "mtime: %lu\n",  OFFSETOF(toku_struct_stat, mtime));

}

static void test_stat(const char *dirname, int result, int ex_errno) 
{
    int r;
    toku_struct_stat buf;
    r = toku_stat(dirname, &buf);
    assert((result == 0 && r == 0)  || (result == -1 && r == -ex_errno));
    if (r == 0) {
        printf("file=%s\n", dirname);
        print_stat(&buf);
    }

}

static void test_fstat(int fd, int result, int ex_errno) 
{
    int r;
    toku_struct_stat buf;
    r = toku_fstat(fd, &buf);
    assert(r==result);

    if(ex_errno == 1){;}
    if (r == 0) print_stat(&buf);

}

extern "C" int test_stat(void);

int test_stat(void) 
{
    int r = 0;

    test_stat("/", 0, 0);
    test_stat("./", 0, 0);

    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, 0777);
    assert(r == 0);

    test_stat("testdir",  -1, ENOENT);
    test_stat("testdir/", -1, ENOENT);


    int fd = open(TOKU_TEST_FILENAME_DATA, O_CREAT, 0644);
    assert(fd >= 0);
    test_fstat(fd, 0, 0);
    close(fd);    
    return 0;
}

