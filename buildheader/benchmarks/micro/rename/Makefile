ARCH ?= x86_64

ifeq "$(ARCH)" "i386"
ARCH_OPT := -m32
else ifeq "$(ARCH)" "x86_64"
ARCH_OPT := -m64
else
ARCH_OPT = $(error "not supported architecture")
endif

ALL = rename_test

all: ${ALL}

.c: $@.c
	gcc $(ARCH_OPT) -Wall -o $@ $@.c

bench-rename:
	touch /mnt/benchmarks/file1
	touch /mnt/benchmarks/file2
	/bin/bash -c 'time ./rename_test /mnt/benchmarks/file1 /mnt/benchmarks/file2 500000'

clean:
	rm -f *~
	for i in $(ALL); do \
		rm -f $$i; \
	done;

