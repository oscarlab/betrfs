/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound interface (really klibc),
 * which implements partitioned counters, using Linux per-CPU
 * variables.
 */

#include <linux/percpu_counter.h>
#include <linux/slab.h>
#include "ftfs.h"

typedef struct partitioned_counter {
	struct percpu_counter pcpu_counter;
} *PARTITIONED_COUNTER;


/*
 * The kernel version implementation of util/partitioned_counter.cc|h
 * based on linux per-cpu variables
 */

PARTITIONED_COUNTER create_partitioned_counter(void)
{
	int err;
	PARTITIONED_COUNTER pc;

	pc = (PARTITIONED_COUNTER) kmalloc(sizeof(*pc), GFP_KERNEL);
	if (!pc) {
		ftfs_error(__func__, "err allocating a partitioned counter: %d",
			-ENOMEM);
		return NULL;
	}

	err = percpu_counter_init(&pc->pcpu_counter, 0);
	if (err) {
		ftfs_error(__func__, "err creating a partitioned counter: %d",
			err);
		return NULL;
	}

	return pc;
}

void destroy_partitioned_counter(PARTITIONED_COUNTER pc)
{
	percpu_counter_destroy(&pc->pcpu_counter);
	kfree(pc);
}

void increment_partitioned_counter(PARTITIONED_COUNTER pc, uint64_t amount)
{
	percpu_counter_add(&pc->pcpu_counter, amount);
}

uint64_t read_partitioned_counter(PARTITIONED_COUNTER pc) {
	return percpu_counter_sum(&pc->pcpu_counter);
}

void partitioned_counters_destroy(void) {}

void partitioned_counters_init(void) {}
