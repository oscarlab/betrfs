ifdef M
	include $(M)/.mkinclude
else
	include $(PWD)/.mkinclude
endif

obj-m += ftfs.o

ftfs-y := nb_super.o nb_bstore.o ftfs_metadata.o rbtreekv.o ftfs_module.o

all:
	make -C /lib/modules/$(KDIR)/build M=$(PWD) modules
clean:
	make -C /lib/modules/$(KDIR)/build M=$(PWD) clean
