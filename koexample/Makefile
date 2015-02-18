# If KERNELRELEASE is defined, we've been invoked from the
# kernel build system and can use its language.
ifneq ($(KERNELRELEASE),)
	obj-m += mydrv.o
	mydrv-objs := interface.o cppmod.o kernel_api.o ftfs_pthread.o ftfs_malloc.o
	list-multi := mydrv.o
# Otherwise we were called directly from the command
# line; invoke the kernel build system.
else
	KERNELDIR ?= /lib/modules/$(shell uname -r)/build
	PWD := $(shell pwd)
endif


all: cppmod.o module
module:
	make -C $(KERNELDIR) M=$(PWD) modules

clean:
	make -C $(KERNELDIR) M=$(PWD) clean

cppmod.o : cppmod.cpp
	g++ -C $(KERNELDIR) -I/usr/include -c -o $@ -fno-builtin -fno-exceptions -fno-rtti -nostdinc -mcmodel=kernel $<


