# MetaTM Project
# File Name: Makefile
#
# Description: Build these files.
#
# Operating Systems & Architecture Group
# University of Texas at Austin - Department of Computer Sciences
# Copyright 2006, 2007. All Rights Reserved.
# See LICENSE file for license terms.

ARCH ?= x86_64

ifeq "$(ARCH)" "i386"
ARCH_OPT := -m32
else ifeq "$(ARCH)" "x86_64"
ARCH_OPT := -m64
else
ARCH_OPT = $(error "not supported architecture")
endif

ALL = syscall-overhead misc-syscall-times

all: ${ALL}

.c: $@.c syscall.h
	gcc $(ARCH_OPT) -Wall -o $@ $@.c -lm 

bench-access:
	./misc-syscall-times -a
bench-stat:
	./misc-syscall-times -s
bench-open:
	./misc-syscall-times -o
bench-unlink:
	./misc-syscall-times -u
bench-mkdir:
	./misc-syscall-times -m
bench-read:
	./misc-syscall-times -r
bench-write:
	./misc-syscall-times -w
bench-chmod:
	./misc-syscall-times -c

clean:
	rm -f *~
	for i in $(ALL); do \
		rm -f $$i; \
	done;

