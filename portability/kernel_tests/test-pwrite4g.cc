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
/* Verify that toku_os_full_pwrite does the right thing when writing beyond 4GB.  */
#include <fcntl.h>
#include <toku_assert.h>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <portability/toku_path.h>

static int iszero(char *cp, size_t n) {
    size_t i;
    for (i=0; i<n; i++)
        if (cp[i] != 0) 
	    return 0;
    return 1;
}

extern "C" int test_pwrite4g(void);
static int ALIGNMENT_SIZE;
int test_pwrite4g(void) {
    int r;
    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, 0777);
    assert(r==0);
    int fd = open(TOKU_TEST_FILENAME_DATA, O_RDWR | O_CREAT | O_BINARY, S_IRWXU|S_IRWXG|S_IRWXO);
    assert(fd>=0);
    if (ftfs_is_hdd()) {
        ALIGNMENT_SIZE = 512;
    } else {
        ALIGNMENT_SIZE = 4096;
    }

    char *XMALLOC_N_ALIGNED(ALIGNMENT_SIZE, ALIGNMENT_SIZE, buf);
    memset(buf, 0, ALIGNMENT_SIZE);
    strcpy(buf, "hello");
    int64_t offset = (1LL<<32) + ALIGNMENT_SIZE;
    toku_os_full_pwrite(fd, buf, ALIGNMENT_SIZE, offset, true);
    char *XMALLOC_N_ALIGNED(ALIGNMENT_SIZE, ALIGNMENT_SIZE, newbuf);

    r = pread(fd, newbuf, 512, 100);
    assert(r==512);
    assert(iszero(newbuf, 512));
    r = pread(fd, newbuf, 512, offset);
    assert(r==512);
    assert(memcmp(newbuf, buf, 512) == 0);
    int64_t fsize;
    r = toku_os_get_file_size(fd, &fsize);
    printf("fsize: %lu\n", fsize);
    assert(r == 0);
    assert(fsize > 100 + ALIGNMENT_SIZE);
    toku_free(buf);
    toku_free(newbuf);

    r = close(fd);
    assert(r==0);
    return 0;
}
