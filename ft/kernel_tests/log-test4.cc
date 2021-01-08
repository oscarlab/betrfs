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

#ident "Copyright (c) 2007, 2008 Tokutek Inc.  All rights reserved."

#include "log-test3.h"
#include "logsuperblock.h"

// create and close, making sure that everything is deallocated properly.

extern "C" int log_test4(void);
char log_name[TOKU_PATH_MAX+1];
int log_test4 (void) {
    int r;
    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);
    TOKULOGGER logger;
    r = toku_logger_create(&logger);                                 assert(r == 0);
    r = toku_logger_open(TOKU_TEST_ENV_DIR_NAME, logger);                             assert(r == 0);

    {
	ml_lock(&logger->input_lock);
	toku_logger_make_space_in_inbuf(logger, 5);
	snprintf(logger->inbuf.buf+logger->inbuf.n_in_buf, 5, "a1234");
	logger->inbuf.n_in_buf+=5;
	logger->lsn.lsn++;
	logger->inbuf.max_lsn_in_buf = logger->lsn;
	ml_unlock(&logger->input_lock);
    }

    r = toku_logger_close(&logger);                                  assert(r == 0);
    {
        sprintf(log_name, "%s/log000000000000.tokulog%d", TOKU_TEST_ENV_DIR_NAME, TOKU_LOG_VERSION);
        int fd = open(log_name, O_RDONLY);
        assert(fd >= 0);

        uint32_t log_size;
        r = toku_verify_logmagic_read_log_end(fd, &log_size);
        assert(r==0);
        printf("%s: log_size=%u\n", __func__, log_size);
        assert(log_size==sizeof(struct log_super_block) + 5);

    }
    r = toku_fs_reset(TOKU_TEST_ENV_DIR_NAME, S_IRWXU);                               assert(r==0);
    return 0;
}
