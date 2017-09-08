/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:
#include <linux/kernel.h>
#include <linux/types.h>
#include <linux/timer.h>
#include <linux/rtc.h>
#include "ftfs_pthread.h"

#include "ftfs.h"

extern char *strerror(int e);
void set_errno(int ret)
{
	return;
}

int get_errno(void)
{
	return ENOSYS;
}

void ftfs_set_errno(int ret)
{
	set_errno(ret);
}

int ftfs_get_errno(void)
{
	return get_errno();
}


void perror(const char *s)
{
	int e = get_errno();

	printk(KERN_ALERT "%s\n", s);
	printk(KERN_ALERT ": %s\n", strerror(e));
}

void print_day_time(void)
{
        struct timeval time;
        unsigned long local_time;
        struct rtc_time tm;

        do_gettimeofday(&time);
        local_time = (u32)(time.tv_sec - (sys_tz.tz_minuteswest * 60));
        rtc_time_to_tm(local_time, &tm);
        trace_printk(" @ (%04d-%02d-%02d %02d:%02d:%02d)\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}
