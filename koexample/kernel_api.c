// kernel_api.c
#include <linux/module.h>
#include <linux/slab.h>

void *my_alloc(size_t s) {
	return kmalloc(s, GFP_KERNEL);
}

void my_free(void *p) {
	kfree(p);
}
