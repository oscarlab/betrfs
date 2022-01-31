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

static FILE fstderr = {
	.fd = 2,
};
FILE *const stderr = &fstderr;
static FILE fstdout = {
	.fd = 1,
};
FILE *const stdout = &fstdout;

int fprintf(FILE *stream, const char *format, ...)
{
  va_list args;
  int r;

  if (stream != stderr)
    return -EINVAL;

  va_start(args, format);
  r = vprintk(format, args);
  va_end(args);

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

int fputc(int c, FILE *stream)
{
  if (stream != stderr)
    return -EINVAL;
  printf("%c", c);
  return c;
}

static int fputs(const char *s, FILE *stream)
{
  if (stream != stderr)
    return -EINVAL;
  printf("%s", s);
  return 0;
}

int putchar(int c)
{
	return fputc(c, stderr);
}

int puts(const char *s)
{
  return fputs(s, stderr);
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

/* DP: I am fairly cerain all uses of fflush are for error logging purposes.
   Bug if not stderr or stdout */
int fflush(FILE *f) {
  if (f == stderr || f == stdout)
    return 0;
  else
    BUG();
  return -ENOSYS;
}

/*
 * YJIAO: Borrow from musl
 * buffer is allocated in fopen no bother to change it.
 * fprintf uses printk, so _IONBF will not used.
 * the only meaningful thing _IONBF which disables
 * buffer for fread and fwrite
*/
int setvbuf(FILE *f, char *buf, int type, size_t size)
{

	f->lbf = EOF;

	if (type == _IONBF)
		f->bufsize = 0;
	else if (type == _IOLBF)
		f->lbf = '\n';

	f->flags |= F_SVB;

	return 0;
}
