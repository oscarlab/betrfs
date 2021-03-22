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

#ifndef PORTABILITY_TOKU_PATH_H
#define PORTABILITY_TOKU_PATH_H

#include <stdarg.h>
#include <limits.h>
#include <sys/types.h>

extern "C" {

__attribute__((nonnull))
const char *toku_test_filename(const char *default_filename);

#define TOKU_TEST_FILENAME toku_test_filename(__FILE__)

/* SFS Specific File */
#define TOKU_SFS_DATA_FILE "/db/ftfs_data_2_1_19.tokudb"
#define TOKU_SFS_META_FILE "/db/ftfs_meta_2_1_19.tokudb"
#define TOKU_HARD_LINK_FILE "/db/hard_link_2_1_19.tokudb"
#define TOKU_SFS_TEST_FILE_1 "/db/test_one_2_1_19.tokudb"
#define TOKU_SFS_TEST_FILE_2 "/db/test_two_2_1_19.tokudb"
#define TOKU_SFS_TEST_FILE_3 "/db/test_three_2_1_19.tokudb"

#define TOKU_SFS_DIR_FILE "/db/tokudb.directory"
#define TOKU_SFS_ENV_FILE "/db/tokudb.environment"
#define TOKU_SFS_ROLLBACK_FILE "/db/tokudb.rollback"
#define TOKU_SFS_LOG_FILE "/db/log000000000000.tokulog25"

/* Define the full names of pre-created files */
#define TOKU_TEST_FILENAME_DATA TOKU_SFS_DATA_FILE
#define TOKU_TEST_FILENAME_META TOKU_SFS_META_FILE
#define TOKU_TEST_FILENAME_ONE TOKU_SFS_TEST_FILE_1
#define TOKU_TEST_FILENAME_TWO TOKU_SFS_TEST_FILE_2
#define TOKU_TEST_FILENAME_THREE TOKU_SFS_TEST_FILE_3

#define TOKU_TEST_FILENAME_LOG  TOKU_SFS_LOG_FILE

/* Pre-created directory name */
#define TOKU_TEST_ENV_DIR_NAME "db"

/* DB name to use for unit tests */
#define TOKU_TEST_DATA_DB_NAME "ftfs_data"
#define TOKU_TEST_META_DB_NAME "ftfs_meta"
#define TOKU_TEST_ONE_DB_NAME "test_one"
#define TOKU_TEST_TWO_DB_NAME "test_two"
#define TOKU_TEST_THREE_DB_NAME "test_three"

#define TOKU_TEST_SFS_ONE_NAME "test_one"
#define TOKU_TEST_SFS_TWO_NAME "test_two"
#define TOKU_TEST_SFS_THREE_NAME "test_three"

#ifndef TOKU_LINUX_MODULE
#define TOKU_PATH_MAX PATH_MAX
#else
#define TOKU_PATH_MAX 256
#endif

// Guarantees NUL termination (unless siz == 0)
// siz is full size of dst (including NUL terminator)
// Appends src to end of dst, (truncating if necessary) to use no more than siz bytes (including NUL terminator)
// Returns strnlen(dst, siz)
size_t toku_strlcat(char *dst, const char *src, size_t siz);

// Guarantees NUL termination (unless siz == 0)
// siz is full size of dst (including NUL terminator)
// Appends src to end of dst, (truncating if necessary) to use no more than siz bytes (including NUL terminator)
// Returns strnlen(dst, siz)
size_t toku_strlcpy(char *dst, const char *src, size_t siz);

char *toku_path_join(char *dest, int n, const char *base, ...);
// Effect:
//  Concatenate all the parts into a filename, using portable path separators.
//  Store the result in dest.
// Requires:
//  dest is a buffer of size at least TOKU_PATH_MAX + 1.
//  There are n path components, including base.
// Returns:
//  dest (useful for chaining function calls)

} // extern "C"

#endif // PORTABILITY_TOKU_PATH_H
