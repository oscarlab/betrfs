/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the "southbound" code in BetrFS (aka klibc).
 *
 * In particular, it provides "stubs" for implementing errno.
 *
 * DEP 10/28/19: I am alarmed to discover that these are just stubs.
 *               I recommend we weed out the use of errno in the ft code,
 *               or fix this.
 */
#include <linux/kernel.h>
#include <linux/types.h>
#include <linux/timer.h>
#include <linux/rtc.h>

#include "ftfs.h"

extern char *strerror(int e);

void sb_set_errno(int ret)
{
	// XXX: Should this do something?
	return;
}

int sb_get_errno(void)
{
	// XXX: Should this do something?
	return ENOSYS;
}
