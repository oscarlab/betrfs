CC = gcc
CXX = g++

ARCH ?= x86_64

ifeq "$(ARCH)" "i386"
ARCH_OPT := -m32
else ifeq "$(ARCH)" "x86_64"
ARCH_OPT := -m64
else
ARCH_OPT = $(error "not supported architecture")
endif

#all: largefb smallfb largefb-tx smallfb-tx largefb-sync smallfb-sync \
#	postmark postmark-tx postmark-sync

all: largefb smallfb largefb-sync smallfb-sync \
	postmark postmark-sync

largefb: large.c
	$(CC) -g $(ARCH_OPT) -o largefb large.c

smallfb: small.c
	$(CC) -g $(ARCH_OPT) -o smallfb small.c

postmark: postmark.c
	$(CC) -g $(ARCH_OPT) -o postmark postmark.c

#largefb-tx: large.c
#	$(CC) -g $(ARCH_OPT) -DTX -o largefb-tx large.c

#smallfb-tx: small.c
#	$(CC) -g $(ARCH_OPT) -DTX -o smallfb-tx small.c

#postmark-tx: postmark.c
#	$(CC) -g $(ARCH_OPT) -DTX -o postmark-tx postmark.c

largefb-sync: large.c
	$(CC) -g $(ARCH_OPT) -DSYNC -o largefb-sync large.c

smallfb-sync: small.c
	$(CC) -g $(ARCH_OPT) -DSYNC -o smallfb-sync small.c

postmark-sync: postmark.c
	$(CC) -g $(ARCH_OPT) -DSYNC -o postmark-sync postmark.c


clean: 
	rm -f bw largefb smallfb largefb-tx smallfb-tx largefb-sync smallfb-sync postmark postmark-tx postmark-sync  *.o

clean-all: clean
	rm -rf d* FLUSH_TMP*
