/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include "toku_mempool.h"
#include <linux/types.h>
#include <linux/kernel.h>
#include <linux/bug.h>

void dump_mempool_space_info(struct mempool * mp){
	size_t used_space;
	size_t size;
	size_t frag_size;
	size_t free_space;
	size_t alloc_space;

	size = toku_mempool_get_size(mp);
	frag_size = toku_mempool_get_frag_size(mp);
	used_space = toku_mempool_get_used_space(mp);
	free_space = toku_mempool_get_free_space(mp);
	alloc_space = toku_mempool_get_allocated_space(mp);

	if(alloc_space > size) BUG();	
	printk(KERN_INFO "used space: %zu, size: %zu, frag size: %zu, free space: %zu, allocated space: %zu\n",  used_space,size,frag_size, free_space, alloc_space);


}
int test_mempool(void){
	struct mempool dummy_mempool;
	struct mempool another_dummy;
	size_t poolsize;
	void * v_1;
	void * v_2;

	toku_mempool_construct(&dummy_mempool,1024);	
	poolsize = toku_mempool_footprint(&dummy_mempool);

	printk(KERN_INFO "mempool:%p, memory size:%zu\n", &dummy_mempool, poolsize);

	dump_mempool_space_info(&dummy_mempool);
	
	v_1 = toku_mempool_malloc(&dummy_mempool, 512, 1);
	if(!v_1) BUG();
	printk(KERN_INFO "after allocated a 512 byte chunk\n");
	dump_mempool_space_info(&dummy_mempool);

	v_2 = toku_mempool_malloc(&dummy_mempool, 216, 1);
	if(!v_2) BUG();
	printk(KERN_INFO "after allocated another 216 byte chunk\n");
	dump_mempool_space_info(&dummy_mempool);

	toku_mempool_mfree(&dummy_mempool, v_2, 216);
	printk(KERN_INFO "after freed 216 byte chunk\n");
	dump_mempool_space_info(&dummy_mempool);

	toku_mempool_copy_construct(&another_dummy, dummy_mempool.base, 1024);
	printk(KERN_INFO "copy constructed another dummy mp\n");
	dump_mempool_space_info(&another_dummy);
	toku_mempool_destroy(&another_dummy);

	toku_mempool_clone(&dummy_mempool, &another_dummy);
	printk(KERN_INFO "cloned to another dummy mp\n");
	dump_mempool_space_info(&another_dummy);
	toku_mempool_destroy(&dummy_mempool);
	toku_mempool_destroy(&another_dummy);
	return 0;

	
}


