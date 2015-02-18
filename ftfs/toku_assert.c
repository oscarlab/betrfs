/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/kernel.h>
#include <linux/printk.h>
#include <linux/bug.h>

void toku_assert_init(void) {}

void toku_assert_set_fpointers(int (*toku_maybe_get_engine_status_text_pointer)(char*, int), 
			       void (*toku_maybe_set_env_panic_pointer)(int, const char*),
                               uint64_t num_rows) {
}

void 
toku_do_assert_fail (const char *expr_as_string, const char *function, 
		     const char *file, int line, int caller_errno) {
  printk(KERN_ALERT "%s:%d %s: Assertion `%s' failed (errno=%d)\n", 
	 file, line, function, expr_as_string, caller_errno);
  BUG();
}

void 
toku_do_assert_zero_fail (uintptr_t expr, const char *expr_as_string, const char *function, 
			  const char *file, int line, int caller_errno) {
  printk(KERN_INFO "%s:%d %s: Assertion `%s == 0' failed (errno=%d) (%s=%lu)\n", 
	 file, line, function, expr_as_string, caller_errno, expr_as_string, expr);
  BUG();
}

void
toku_do_assert_expected_fail (uintptr_t expr, uintptr_t expected, const char *expr_as_string, 
			      const char *function, const char *file, int line, int caller_errno) {
  printk(KERN_INFO 
	 "%s:%d %s: Assertion `%s == %lu' failed (errno=%d) (%s=%lu)\n", 
	 file, line, function, expr_as_string, expected, caller_errno, expr_as_string, expr);
  BUG();
}

void 
toku_do_assert(int expr, const char *expr_as_string, const char *function, const char* file, int line, int caller_errno) {
    if (expr == 0)
        toku_do_assert_fail(expr_as_string, function, file, line, caller_errno);
}

