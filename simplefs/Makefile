ifdef M
	include $(M)/.mkinclude
else
	include $(PWD)/.mkinclude
endif

obj-m := simplefs.o
ccflags-y += -Wall -Werror

simplefs-objs := sfs.o sfs_in_kernel_write.o

.PHONY: all ko clean

all: ko mkfs-sfs

ko:
	$(MAKE) -C $(MOD_KERN_SOURCE) M=$(PWD) modules

# https://stackoverflow.com/a/52036564/13040392
-include mkfs-sfs.d
mkfs-sfs: mkfs-sfs.c
	$(CC) $(ccflags-y) -MMD -MP $< -o $@

clean:
	$(MAKE) -C $(MOD_KERN_SOURCE) M=$(PWD) clean
	$(RM) mkfs-sfs mkfs-sfs.d
