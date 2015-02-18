/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include "ftfs_partitioned_counter.h"
#include "ftfs.h"

/* This is the kernel version implementation of util/partitioned_counter based on per-cpu variables
*/
void test_percpu_counter_add(struct percpu_counter *fbc, s64 amount, s32 batch);
s64 test_percpu_counter_sum(struct percpu_counter *fbc);


PARTITIONED_COUNTER create_partitioned_counter(void) {
    PARTITIONED_COUNTER pc = (PARTITIONED_COUNTER) kmalloc(sizeof(struct partitioned_counter), GFP_KERNEL);
    int err = percpu_counter_init(& pc->pcpu_counter, 0);
    if (err) {
	    ftfs_error(__func__, "err creating a partitioned counter");
	    return NULL;
    }
    percpu_counter_set(&pc->pcpu_counter, 0);
    return pc;
}

void destroy_partitioned_counter(PARTITIONED_COUNTER pc) {
    percpu_counter_destroy(&pc->pcpu_counter);
    kfree(pc);
}

void increment_partitioned_counter(PARTITIONED_COUNTER pc, uint64_t amount) {
    test_percpu_counter_add(&pc->pcpu_counter, amount, 32);
}

void partitioned_counters_destroy(void) {

}

void partitioned_counters_init(void) {

}

uint64_t read_partitioned_counter(PARTITIONED_COUNTER pc) {
    return test_percpu_counter_sum(&pc->pcpu_counter);
}

 void test_percpu_counter_add(struct percpu_counter *fbc, s64 amount, s32 batch)
 {
        s64 count;
        preempt_disable();
        count = __this_cpu_read(*fbc->counters) + amount;
        if (count >= batch || count <= -batch) {
            unsigned long flags;
            raw_spin_lock_irqsave(&fbc->lock, flags);
            fbc->count += count;
            __this_cpu_sub(*fbc->counters, count - amount);
            raw_spin_unlock_irqrestore(&fbc->lock, flags);
        } else {
            this_cpu_add(*fbc->counters, amount);
        }
        preempt_enable();
 }

s64 test_percpu_counter_sum(struct percpu_counter *fbc)
{
    s64 ret;
    int cpu;
    unsigned long flags;
    raw_spin_lock_irqsave(&fbc->lock, flags);
    ret = fbc->count;
    for_each_online_cpu(cpu) {
        s32 *pcount = per_cpu_ptr(fbc->counters, cpu);
        ret += *pcount;
    }
    raw_spin_unlock_irqrestore(&fbc->lock, flags);
    return ret;
}

