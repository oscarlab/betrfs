ifdef M
	include $(M)/.mkinclude
else
	include $(PWD)/.mkinclude
endif

obj-m := simplefs.o
simplefs-objs := sfs.o sfs_in_kernel_write.o
ccflag-y := -DSFS_DEBUG

all: clean ko mkfs-sfs

ko:
	make -C $(MOD_KERN_SOURCE) M=`pwd` modules

mkfs-sfs_SOURCES:
	mkfs-sfs.c sfs.h
clean:
	make -C $(MOD_KERN_SOURCE) M=`pwd` clean
	rm -rf mkfs-sfs *.ko
