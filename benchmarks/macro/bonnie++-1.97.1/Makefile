EXES=bonnie++ zcav getc_putc getc_putc_helper
EXE=bon_csv2html generate_randfile

all: $(EXE) $(EXES)

SCRIPTS=bon_csv2txt

prefix=/usr/local
eprefix=${prefix}
#MORE_WARNINGS=-Weffc++
WFLAGS=-Wall -W -Wshadow -Wpointer-arith -Wwrite-strings -pedantic -ffor-scope -Wcast-align -Wsign-compare -Wpointer-arith -Wwrite-strings -Wformat-security -Wswitch-enum -Winit-self $(MORE_WARNINGS)
CFLAGS=-O2  -DNDEBUG $(WFLAGS) $(MORECFLAGS)
CXX=g++ $(CFLAGS)
LINK=g++
THREAD_LFLAGS=-pthread

INSTALL=/usr/bin/install -c
INSTALL_PROGRAM=${INSTALL}

BONSRC=bonnie++.cpp bon_io.cpp bon_file.cpp bon_time.cpp semaphore.cpp \
 sync.cpp thread.cpp bon_suid.cpp duration.cpp rand.o util.o
BONOBJS=$(BONSRC:.cpp=.o)

MAN1=bon_csv2html.1 bon_csv2txt.1 generate_randfile.1
MAN8=bonnie++.8 zcav.8 getc_putc.8

ZCAVSRC=zcav.cpp thread.cpp zcav_io.cpp bon_suid.cpp duration.cpp
ZCAVOBJS=$(ZCAVSRC:.cpp=.o)

GETCSRC=getc_putc.cpp bon_suid.cpp duration.cpp util.o
GETCOBJS=$(GETCSRC:.cpp=.o)

GETCHSRC=getc_putc_helper.cpp duration.cpp
GETCHOBJS=$(GETCHSRC:.cpp=.o)

bonnie++: $(BONOBJS)
	$(LINK) -o bonnie++ $(BONOBJS) $(THREAD_LFLAGS)

zcav: $(ZCAVOBJS)
	$(LINK) -o zcav $(ZCAVOBJS) $(THREAD_LFLAGS)

getc_putc: $(GETCOBJS) getc_putc_helper
	$(LINK) -o getc_putc $(GETCOBJS) $(THREAD_LFLAGS)

getc_putc_helper: $(GETCHOBJS)
	$(CXX) -o getc_putc_helper $(GETCHOBJS)

bon_csv2html: bon_csv2html.o
	$(LINK) bon_csv2html.o -o bon_csv2html

generate_randfile: generate_randfile.o
	$(LINK) generate_randfile.o -o generate_randfile

install-bin: $(EXE) $(EXES)
	mkdir -p $(eprefix)/bin $(eprefix)/sbin
	${INSTALL} -s $(EXES) $(eprefix)/sbin
	${INSTALL} -s $(EXE) $(eprefix)/bin
	${INSTALL} $(SCRIPTS) $(eprefix)/bin

install: install-bin
	mkdir -p ${prefix}/share/man/man1 ${prefix}/share/man/man8
	${INSTALL} -m 644 $(MAN1) ${prefix}/share/man/man1
	${INSTALL} -m 644 $(MAN8) ${prefix}/share/man/man8

%.o: %.cpp
	$(CXX) -c $<

clean:
	rm -f $(EXE) $(EXES) *.o build-stamp install-stamp
	rm -rf debian/tmp core debian/*.debhelper
	rm -f debian/{substvars,files} config.log depends.bak

realclean: clean
	rm -f config.* Makefile bonnie++.spec port.h conf.h configure.lineno
	rm -f bon_csv2txt bon_csv2html.1 sun/pkginfo bonnie.h

dep:
	makedepend -Y -f depends *.cpp 2> /dev/null

include depends

