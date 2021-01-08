/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef _FTFS_UNIT_TEST_H
#define _FTFS_UNIT_TEST_H

/* This file defines APIs needed for the Toku unit test build.
 * It is not included in the "production" build of the full file
 * system, only for running unit tests against the key-value store.
 */

#define TOKU_PROC_NAME "toku_test"

extern int run_test(char *);
extern int thread_run_test(char *);
extern char *test_filename;

#endif //_FTFS_UNIT_TEST_H
