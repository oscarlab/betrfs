/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef _FTFS_PARTITIONED_COUNTER_H
#define _FTFS_PARTITIONED_COUNTER_H

#include <linux/percpu_counter.h>
#include <linux/slab.h>

struct partitioned_counter {
	struct percpu_counter pcpu_counter;
};

typedef struct partitioned_counter *PARTITIONED_COUNTER;

PARTITIONED_COUNTER create_partitioned_counter(void);

void destroy_partitioned_counter(PARTITIONED_COUNTER pc);

void increment_partitioned_counter(PARTITIONED_COUNTER pc, uint64_t amount);

void partitioned_counters_destroy(void);

void partitioned_counters_init(void) ;

uint64_t read_partitioned_counter(PARTITIONED_COUNTER pc);

#endif //_FTFS_PARTITIONED_COUNTER_H
