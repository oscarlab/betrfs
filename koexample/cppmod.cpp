//  cppmod.cpp, finally, my C++ code
#include "kernel_api.h"
#include "circular_buffer.h"

#include "ftfs_pthread.h"

pthread_t p;
int global = 0;

void *pthread_func(void *arg) {
    global = *((int *)arg);

    return (void *)0xdeadbeef;
}

extern "C" void start_driver(void) {
    int i = 12345;
    void *result;

	printk(KERN_INFO "C++ Part starts...\n");

    pthread_create(&p, NULL, pthread_func, &i);

    pthread_join(p, &result);

    printk(KERN_INFO "ret %p global %d\n", result, global);
} 

extern "C" void stop_driver(void) {
	printk(KERN_INFO "C++ Part ends...\n");
}
