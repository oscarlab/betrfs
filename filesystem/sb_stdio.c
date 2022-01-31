/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound code (klibc), implementing
 * printf, and friends, as well as sched_yield().
 */

#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/sched.h>
#include <linux/printk.h>
#include "sb_files.h"
#include "sb_malloc.h"

/* We only expect toku code to use dprintf for error logs, etc.  Not
 * for any correctness or performance-critical writes (i.e., writes to
 * sfs or the journal should use write or pwrite).
 */
int dprintf(int fd, const char *format, ...)
{
  va_list args;
  int r;

  if (fd == 0) {
	  /* Bug if we try to dprintf to std in */
	  BUG();
	  return -EINVAL;
  } else if (fd > 2) {
	  /* "Normal" file; only expected for debugging, like
	   * toku_dump_ftnode.  It is sloooow.
	   */
	  int max_buf = 512;
	  int len;
	  char *buf = (char *)sb_malloc(max_buf);
	  va_start(args, format);
	  len = vsnprintf(buf, max_buf, format, args);
	  va_end(args);
	  // If snprintf truncated the buffer, truncate the write,
	  // on the assumption this is only debug output
	  if (len >= max_buf) len = max_buf;
	  r = write(fd, buf, len);
	  sb_free(buf);
  } else {
	  /* Standard out/error */
	  va_start(args, format);
	  r = vprintk(format, args);
	  va_end(args);
  }

  return r;
}

int printf(const char *format, ...)
{
  va_list args;
  int r;

  va_start(args, format);
  r = vprintk(format, args);
  va_end(args);

  return r;
}

int putchar (int c)
{
	printf("%c", (char) c);
	return c;
}

int puts(const char *s)
{
	printf("%s", s);
	return 0;
}

int atoi(const char * s) {
  int r;
  sscanf(s, "%d", &r);
  return r;
}

int sched_yield(void){
   schedule();
   return 0;
}
