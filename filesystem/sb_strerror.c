/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound code (klibc), and
 * implements strerror and strerror_r for the ft code.
 */

#include <linux/errno.h>
#include <linux/string.h>
/* DP: "Borrowed" from musl */

#define E(a,b) a,
static const unsigned char errid[] = {
#include "__strerror.h"
};

#undef E
#define E(a,b) b "\0"
static const char errmsg[] =
#include "__strerror.h"
  ;

char *strerror(int e)
{
  const char *s;
  int i;
  for (i=0; errid[i] && errid[i] != e; i++);
  for (s=errmsg; i; s++, i--) for (; *s; s++);
  return (char *)s;
}

int strerror_r(int err, char *buf, size_t buflen)
{
  char *msg = strerror(err);
  size_t l = strlen(msg);
  if (l >= buflen) {
    if (buflen) {
      memcpy(buf, msg, buflen-1);
      buf[buflen-1] = 0;
    }
    return ERANGE;
  }
  memcpy(buf, msg, l+1);
  return 0;
}
