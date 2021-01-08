/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef _FTFS_H
#define _FTFS_H

/* This file defines several internal functions and parameters
 * for the module, including kernel linking, hooks for initializing
 * the kv store, and internal resource management.
 */

#include <linux/sched.h>
#include <linux/nsproxy.h>
#include <linux/mount.h>
#include <linux/mnt_namespace.h>
#include <linux/mm.h>


extern size_t db_cachesize;
//#define FTFS_SCALE_CACHE(totalram_pages) (totalram_pages / 8) //1/4
#define FTFS_SCALE_CACHE(totalram_pages) (totalram_pages / 4)
extern int toku_ncpus;

void ftfs_error (const char * function, const char * fmt, ...);
void ftfs_log (const char * function, const char * fmt, ...);

#ifdef FTFS_DEBUG
static inline void ftfs_debug_break(void) {
	printk(KERN_ALERT "debug breakpoint");
}

#  define FTFS_DEBUG_ON(err)				\
	if (err) {					\
		ftfs_error(__func__, "err=%d", (err));	\
		ftfs_debug_break();			\
		BUG();					\
	}
#else
#  define FTFS_DEBUG_ON(err)
#endif

#define DECLARE_SYMBOL_FTFS(kern_func)			\
	static kern_func##_t ftfs_##kern_func = NULL

#define LOOKUP_SYMBOL_FTFS(kern_func)					\
	ftfs_##kern_func =						\
		(kern_func##_t) kallsyms_lookup_name(#kern_func);	\
	if (!ftfs_##kern_func) {					\
		ftfs_error(__func__, #kern_func " not found");		\
		return -EPERM;						\
	}

#define LOOKUP_SYMBOL_FTFS_VOID(kern_func)					\
	ftfs_##kern_func =						\
		(kern_func##_t) kallsyms_lookup_name(#kern_func);	\
	if (!ftfs_##kern_func) {					\
		ftfs_error(__func__, #kern_func " not found");		\
		return;						\
	}

extern int toku_ft_layer_init_with_panicenv(void);
static inline int init_ft_index(void)
{
	struct sysinfo info;
	si_meminfo(&info);
	db_cachesize = FTFS_SCALE_CACHE(info.totalram) << PAGE_SHIFT;

	return toku_ft_layer_init_with_panicenv();
}

extern void toku_ydb_destroy(void);
static inline void destroy_ft_index(void)
{
    toku_ydb_destroy();
}

void init_mem_trace(void);

#define FTFS_SUPER_MAGIC 0XF7F5

#endif /* _FTFS_H */
