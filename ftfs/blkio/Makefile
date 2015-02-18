DEBUG = 0

EXTRA_CFLAGS += -I$(PWD)/include

#ifeq ($(DEBUG),1)
#	EXTRA_CFLAGS += -DCONFIG_QUOTIENT_FILTER_DEBUG
#endif

# If KERNELRELEASE is defined, we've been invoked from the
# kernel build system and can use its language.
ifneq ($(KERNELRELEASE),)
	obj-m := block_dev.o block_dev_test.o
# Otherwise we were called directly from the command
# line; invoke the kernel build system.
else
    KERNELDIR ?= /lib/modules/$(shell uname -r)/build
	PWD := $(shell pwd)

default:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) clean

endif
