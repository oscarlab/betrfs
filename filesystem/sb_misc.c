/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound (klibc) code.
 *
 * It is a grab-bag of other stuff the toku code needs, and
 * most of it is ripe with opportunity for clean-up and simplifying
 * our code base.
 */

#include <linux/string.h>
#include <linux/cpumask.h>
#include <linux/cpufreq.h>
#include <linux/smp.h>
#include <linux/slab.h>
#include <linux/percpu.h>
#include <linux/syscalls.h>
#include <asm/page_types.h>
#include <linux/profile.h>
#include <linux/mm.h>
#include <linux/file.h>
#include <linux/mman.h>
#include <asm/page_types.h>
#include <linux/notifier.h>
#include <linux/ctype.h>
#include <linux/resource.h>
#include <linux/dirent.h>
#include "ftfs.h"
#include "ftfs_southbound.h"
#include "sb_misc.h"


typedef long (* sys_newlstat_t)(const char *, struct stat *);
typedef long (* sys_setrlimit_t) (unsigned int, struct rlimit *);
typedef long (* sys_getrlimit_t) (unsigned int, struct rlimit *);
DECLARE_SYMBOL_FTFS(sys_newlstat);
DECLARE_SYMBOL_FTFS(sys_setrlimit);
DECLARE_SYMBOL_FTFS(sys_getrlimit);

int resolve_toku_misc_symbols(void)
{
	LOOKUP_SYMBOL_FTFS(sys_newlstat);
	LOOKUP_SYMBOL_FTFS(sys_setrlimit);
	LOOKUP_SYMBOL_FTFS(sys_getrlimit);
	return 0;
}

#ifdef SOUTHBOUND_TESTING /* only used during toku unit testing */
extern char* ftfs_test_filename;
const char *toku_test_filename(const char *default_filename)
{
  int i;

  if (ftfs_test_filename)
    return ftfs_test_filename;

  i = strlen(default_filename);
  while (i > 0 && default_filename[i-1] != '/')
    i--;
  return default_filename+i;
}
#endif

int toku_os_get_number_processors(void)
{
  return num_present_cpus();
}

int toku_os_get_number_active_processors(void)
{
  int n = num_online_cpus();
  return n < toku_ncpus ? n : toku_ncpus;
}

//On x86_64:There is one single variable cpu_khz that gets written by all the CPUs. So,
//the frequency set by last CPU will be seen on /proc/cpuinfo of all the CPUs in the system.

extern uint64_t toku_cached_hz;
int toku_os_get_processor_frequency(uint64_t* hzret){
    int r;
    uint64_t khz;
    if (toku_cached_hz) {
        *hzret = toku_cached_hz;
        r = 0;
    } else {
        khz = cpufreq_quick_get(smp_processor_id());
        if(!khz)
          khz = cpu_khz;
        *hzret = khz * 1000U;
        toku_cached_hz = *hzret;
        r = 0;
    }
    return r;
}

extern int toku_cached_pagesize;

int toku_os_get_pagesize(void) {
    int pagesize = toku_cached_pagesize;
    if(pagesize == 0) {
        pagesize = PAGE_SIZE ;
        if(pagesize) {
            toku_cached_pagesize = pagesize;
        }
    }
    return pagesize;
}

uint64_t toku_os_get_phys_memory_size(void) {
	ftfs_log(__func__, "totalram_pages = %8lu", totalram_pages);
	return totalram_pages * PAGE_SIZE;
}

//typical undergrad recursive algo. just watch "." and ".."
//i assume there is no loops by links.


int setrlimit64(int resource, struct rlimit *rlim)
{
	mm_segment_t saved;
	int ret;

	saved = get_fs();
	set_fs(get_ds());
	ret = ftfs_sys_setrlimit(resource, rlim);
	set_fs(saved);

	return ret;

}

int getrlimit64(int resource, struct rlimit *rlim)
{
	mm_segment_t saved;
	int ret;

	saved = get_fs();
	set_fs(get_ds());
	ret = ftfs_sys_getrlimit(resource, rlim);
	set_fs(saved);

	return ret;
}

/* isalnum defined as a macro in linux/ctype.h. use () to prevent expansion */
int (isalnum)(int c)
{
	return isalnum(c);
}
