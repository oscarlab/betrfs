/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include <linux/string.h>
#include <linux/kernel.h>
#include <linux/time.h>
#include <linux/syscalls.h>
//#include <linux/posix-clock.h>
#include <linux/bug.h>
#include <linux/file.h>
#include <linux/dirent.h>
#include <asm/segment.h>
#include <asm/uaccess.h>
#include <linux/mount.h>
#include <linux/delay.h>
#include <linux/sort.h>
#include <linux/statfs.h>
#include <linux/dcache.h>
#include <linux/fs.h>
#include <linux/fs_struct.h>
#include <linux/namei.h>
#include <linux/security.h>
#include <linux/kallsyms.h>
#include <linux/fcntl.h>
#include "sb_random.h"
#include "sb_files.h"
#include "ftfs.h"
#if 1

/* This file is part of the southbound code (klibc).

   sb_linkage() contains references to all the kernel symbols
   that are compatible with a symbol with the same name in libc or
   other user-space libraries.  Examples are

   - memcpy
   - strcpy
   - etc.

   Calls to those functions in the ft-index code will be resolved
   directly to the kernel versions. */

long toku_linkage(void)
{
  /* I hate compiler warnings -- the dummy code below is just to
     prevent the compiler from complaining about statements w/ no
     effect. */
  return
    (long)memcmp   |
    (long)memcpy   |
    (long)memmove  |
    (long)memset   |
    (long)snprintf |
    (long)sprintf  |
    (long)sscanf   |
    (long)strcmp   |
    (long)strcpy   |
    (long)strlen   |
    (long)strncmp  |
    (long)strncpy  |
    (long)strnlen  |
    (long)strrchr  |
    (long)strstr;
}

/* The dummy functions below are place-holders until we provide real
   implementations of these functions or change the ft code to not
   use them. */

/* Some of these have correct type signatures because, otherwise, the
   compiler complains that they differ from its built-in versions.
   Surprising that the kernel doesn't disable all built-ins when
   compiling modules.  */

/* locktree requires partitioned_counter code, which currently can't
   be imported due to thread-local storage.  So here are stubs for
   those functions.*/
/*void create_partitioned_counter(void) { BUG(); }*/
/*void destroy_partitioned_counter(void) { BUG(); }*/
/*void increment_partitioned_counter(void) { BUG(); }*/
/*void partitioned_counters_destroy(void) { BUG(); }*/
/*void partitioned_counters_init(void) { BUG(); }*/
/*void read_partitioned_counter(void) { BUG(); }*/
/* All libc stuff below, I think. */
void abort(void) { BUG(); } /* dp: This is correct for this function. :) */
/* void backtrace(void) { BUG(); } */
/* void backtrace_symbols_fd(void) { BUG(); } */
//void clock_gettime(void) { BUG(); }
/* int clock_gettime(clockid_t clk_id, struct timespec *tp) {  */
/*   struct k_clock *kc = clockid_to_kclock(clk_id); */
/*   if (!kc) */
/*     return -EINVAL; */
/*   return kc->clock_get(clk_id, tp); */
/* } */

#define TIMEVAL_TO_TIMESPEC(tv, ts) {                                   \
        (ts)->tv_sec = (tv)->tv_sec;                                    \
        (ts)->tv_nsec = (tv)->tv_usec * 1000;                           \
}

extern int gettimeofday(struct timeval *tv, struct timezone *tz);

int clock_gettime (clockid_t clock_id, struct timespec *tp)
{
  int retval = -1;

  switch (clock_id)
    {
#ifdef SYSDEP_GETTIME
      SYSDEP_GETTIME;
#endif // SYSDEP_GETTIME

#ifndef HANDLED_REALTIME
    case CLOCK_REALTIME:
      {
        struct timeval tv;
        retval = gettimeofday (&tv, NULL);
        if (retval == 0)
          TIMEVAL_TO_TIMESPEC (&tv, tp);
      }
      break;
#endif // HANDLED_REALTIME

    default:
      break;
    }

  return retval;
}

static char *da_time = "Wed Jun 30 21:49:08 1993\n";
char* ctime(const time_t *timep) { return da_time; }
char* ctime_r(const time_t *timep, char *buf)
{
     struct tm  result;
     int len;
     int res;
     time_to_tm(*timep, 0, &result);
     result.tm_year = result.tm_year - 70 + 1970;
     result.tm_mon = result.tm_mon+1;
     //result.tm_mday = result.tm_mday-1+1;
     len = strlen(buf);
     res = snprintf(buf, len<26?26:len,
                   "%ld-%d-%d--%d:%d:%d\n",
                result.tm_year, result.tm_mon, result.tm_mday,
                result.tm_hour, result.tm_min, result.tm_sec);
    return 0;
}
pid_t getpid(void) { return current->pid; }

void ftfs_test_break(void) {
	ftfs_log(__func__, "reached debugging breakpoint");
}

// XXX: Why is this still being called by the omt code?
int raise(int sig)
{
	ftfs_error(__func__, "This function is only used in tests.\n"
		   "It does not need to be defined, and shouldn't be used.\n"
		   "Please fix your test so that it does not call raise.");
	BUG();
}

int getrusage_ftfs(int who, struct rusage *ru) { return 0; }

void qsort(void *base, size_t nmemb, size_t size,
	   int (*compar)(const void *, const void *))
{
	sort(base, nmemb, size, compar, NULL);
}

/* random returns long int, overflow can cause negatives */
int rand(void)
{
	int ret = random();
	return (ret >= 0) ? ret : -ret;
}

time_t time(time_t *t)
{
	time_t i = get_seconds();
	if (t)
		*t = i;
	return i;
}

int gettimeofday(struct timeval *tv, struct timezone *tz) {
	if (tv != NULL)
		do_gettimeofday(tv);
	if (tz != NULL)
		*tz = sys_tz;
	return 0;
}

uint32_t (htonl)(uint32_t hostlong) { return htonl(hostlong); }
/* DP: "Borrowed" from musl */
int isprint(int c) { return (unsigned)c-0x20 < 0x5f; }

/* I think we can get rid of this call by setting the #define MMAP_THRESHOLD
   to false in toku_config, as we will not mmap memory in kernel. */
/* void mallopt(void) { BUG(); } */
#ifdef FTFS_DEBUG
void toku_dump_hex(const char *str, void *buf, int len) {
	print_hex_dump(KERN_ALERT, str, DUMP_PREFIX_ADDRESS, 16, 1, buf, len, 1);
}
#endif
void toku_dump_stack(void) {dump_stack();}
uint32_t (ntohl)(uint32_t netlong) { return ntohl(netlong); }
unsigned int sleep(unsigned int seconds) { ssleep(seconds); return 0; }

/* DP: The only usage of system is to call gettid.  Just inline the
 * code, and blow an assert if another call is used .*/
int syscall(int number, ...){
  BUG_ON(!__NR_gettid);
  return task_pid_vnr(current);
 }

pid_t toku_get_tid(void){
  return task_pid_vnr(current);
 }

int usleep(unsigned long usecs) {
  if (usecs < 10000)
    usleep_range(usecs, 2*usecs + 10);
  else
    msleep(usecs / 1000);
  return 0;
}

void exit(int status)
{
    ftfs_error(__func__, " not impl yet");
    BUG();
}

pid_t wait(int *status)
{
    ftfs_error(__func__, " not impl yet");
    BUG();
}


pid_t fork(void)
{
    ftfs_error(__func__, " not impl yet");
    BUG();
}


/* DP: "Borrowed" from musl */
char *__xpg_basename(char *s)
{
	size_t i;
	if (!s || !*s) return ".";
	i = strlen(s)-1;
	for (; i&&s[i]=='/'; i--) s[i] = 0;
	for (; i&&s[i-1]!='/'; i--);
	return s+i;
}

char *setlocale(int category, const char *locale){
    ftfs_error(__func__, " not impl yet");
    BUG();
}

unsigned long long strtoull(const char *nptr, char **endptr,
                                                   int base){
    ftfs_error(__func__, " not impl yet");
    BUG();
}

long int strtol(const char *nptr, char **endptr, int base){
    ftfs_error(__func__, " not impl yet");
    BUG();

}

unsigned long int strtoul(const char *nptr, char **endptr, int base){
    ftfs_error(__func__, " not impl yet");
    BUG();

}

long long strtoll(const char *nptr, char **endptr, int base){
    ftfs_error(__func__, " not impl yet");
    BUG();

}

char * strtok_r(char *str, const char *delim, char **saveptr) {
    ftfs_error(__func__, " not impl yet");
    BUG();
}
/* DP: These were turned on with -O2.  May not be
 * necessary.  Just dropping them in for now.
 */

#endif
