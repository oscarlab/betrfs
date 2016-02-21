CC = gcc
LD = gcc

CFLAGS += -Wall -Wextra -Wshadow #-Werror
CFLAGS += -Wmissing-prototypes -Wmissing-declarations  
CFLAGS += -std=c99

ifeq ($(DEBUG), 1)
	CFLAGS += -g3 -O0 -DDEBUG
else
	CFLAGS += -g3 -O3
ifeq ($(FLTO), 1)
	CFLAGS += -flto
	LDFLAGS += -flto
endif
endif

INCDIR=.

CPPFLAGS += -I$(INCDIR) -D_GNU_SOURCE
LDLIBS += -pthread

OBJECTS := $(patsubst %.c, %, $(wildcard *.c))
# threadpool is not going to be target
TARGETS := $(patsubst threadpool, , $(OBJECTS))

.PHONY: all clean

all: $(TARGETS)

# override the default so we can depend on threadpool.o
benchmark-fs-threaded: benchmark-fs-threaded.c threadpool.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $^ $(LDLIBS)  -o $@


tidy:
	rm -rf *.mount

clean: tidy
	rm -rf $(TARGETS) benchmark-bucket
