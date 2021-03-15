/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the "southbound" portion of BetrFS, aka klibc,
 * that supports just enough of POSIX for the ft code to run.
 *
 * Specifically, this file implements assertion support, mapping to
 * the kernel BUG_ON function.
 */

#include <linux/kernel.h>
#include <linux/string.h>
#include <linux/printk.h>
#include <linux/bug.h>

void
sb_do_assert_zero_fail (uintptr_t expr, const char *expr_as_string, const char *function, const char *file, int line, int caller_errno) {
    printk(KERN_ERR "%s:%d %s: Assertion `%s == 0' failed (errno=%d) (%s=%p)\n", file, line, function, expr_as_string, caller_errno, expr_as_string, (void *) expr);
    BUG();
}

void
sb_do_assert_fail (const char *expr_as_string, const char *function, const char *file, int line, int caller_errno) {
    printk(KERN_ERR "%s:%d %s: Assertion `%s' failed (errno=%d)\n", file, line, function, expr_as_string, caller_errno);
    BUG();
}


/* not necessarily failed -- it is just setting up the toku_backtrace_abort callbacks, we are not using it so leave it
 * as an empty place holder then -JYM */
void sb_assert_set_fpointers(int (*toku_maybe_get_engine_status_text_pointer)(char*, int),
			       void (*toku_maybe_set_env_panic_pointer)(int, const char*),
                               uint64_t num_rows) {

//	BUG();
}
