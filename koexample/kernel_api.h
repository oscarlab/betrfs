// kernel_api.h.
#ifndef KERNEL_API_H
#define KERNEL_API_H
#define __KERNEL__
#ifdef __cplusplus
extern "C" {
#endif

typedef long unsigned int size_t;
#define KERN_SOH        "\001"          /* ASCII Start Of Header */
#define KERN_INFO       KERN_SOH "6"    /* informational */
#define NULL    0UL

extern void printk(const char *fmt, ...);
extern void *my_alloc(size_t size);
extern void my_free(void *p);

#ifdef __cplusplus
}
#endif

#endif
