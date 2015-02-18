
/*
 * COPYRIGHT NOTICE:
 * Copyright (c) Tim Bray, 1990.
 * Copyright (c) Russell Coker, 1999.  I have updated the program, added
 * support for >2G on 32bit machines, and tests for file creation.
 * Licensed under the GPL version 2.0.
 * DISCLAIMER:
 * This program is provided AS IS with no warranty of any kind, and
 * The author makes no representation with respect to the adequacy of this
 *  program for any particular purpose or with respect to its adequacy to
 *  produce any particular result, and
 * The author shall not be liable for loss or damage arising out of
 *  the use of this program regardless of how sustained, and
 * In no event shall the author be liable for special, direct, indirect
 *  or consequential damage, loss, costs or fees or expenses of any
 *  nature or kind.
 */

#include "bonnie.h"

#include <stdlib.h>

#include "conf.h"
#ifdef HAVE_ALGORITHM
#include <algorithm>
#else
#ifdef HAVE_ALGO
#include <algo>
#else
#include <algo.h>
#endif
#endif

#include <sys/wait.h>
#include <unistd.h>
#include <sys/time.h>
#include <pwd.h>
#include <grp.h>
#include <sys/utsname.h>
#include "sync.h"

#include <time.h>
#include "bon_io.h"
#include "bon_file.h"
#include "bon_time.h"
#include "rand.h"
#include <ctype.h>
#include <string.h>
#include <signal.h>

void usage();

class CGlobalItems
{
public:
  bool quiet;
  int byte_io_size;
  bool sync_bonnie;
#ifdef O_DIRECT
  bool use_direct_io;
#endif
  BonTimer timer;
  int ram;
  Sync *syn;
  char *name;
  bool bufSync;
  int  io_chunk_bits;
  int  file_chunk_bits;
  int io_chunk_size() const { return m_io_chunk_size; }
  int file_chunk_size() const { return m_file_chunk_size; }
  bool *doExit;
  void set_io_chunk_size(int size)
    { delete m_buf; pa_new(size, m_buf, m_buf_pa); m_io_chunk_size = size; }
  void set_file_chunk_size(int size)
    { delete m_buf; m_buf = new char[__max(size, m_io_chunk_size)]; m_file_chunk_size = size; }

  // Return the page-aligned version of the local buffer
  char *buf() { return m_buf_pa; }

  CGlobalItems(bool *exitFlag);
  ~CGlobalItems() { delete name; delete m_buf; delete syn; }

  void decrement_and_wait(int nr_sem);

  void SetName(CPCCHAR path)
  {
    delete name;
    name = new char[strlen(path) + 15];
    pid_t myPid = getpid();
    sprintf(name, "%s/Bonnie.%d", path, int(myPid));
  }

  void setSync(SYNC_TYPE type, int semKey = 0, int num_tests = 0)
  {
    syn = new Sync(type, semKey, num_tests);
  }

private:
  int m_io_chunk_size;
  int m_file_chunk_size;

  char *m_buf;     // Pointer to the entire buffer
  char *m_buf_pa;  // Pointer to the page-aligned version of the same buffer

  // Implement a page-aligned version of new.
  // 'p' is the pointer created
  // 'page_aligned_p' is the page-aligned pointer created
  void pa_new(unsigned int num_bytes, char *&p, char *&page_aligned_p)
  {
    int page_size = getpagesize();
    p = ::new char [num_bytes + page_size];

    page_aligned_p = (char *)((((unsigned long)p + page_size - 1) / page_size) * page_size);
  }

  CGlobalItems(const CGlobalItems &f);
  CGlobalItems & operator =(const CGlobalItems &f);
};

CGlobalItems::CGlobalItems(bool *exitFlag)
 : quiet(false)
 , byte_io_size(DefaultByteIO)
 , sync_bonnie(false)
#ifdef O_DIRECT
 , use_direct_io(false)
#endif
 , timer()
 , ram(0)
 , syn(NULL)
 , name(NULL)
 , bufSync(false)
 , io_chunk_bits(DefaultChunkBits)
 , file_chunk_bits(DefaultChunkBits)
 , doExit(exitFlag)
 , m_io_chunk_size(DefaultChunkSize)
 , m_file_chunk_size(DefaultChunkSize)
 , m_buf(NULL)
 , m_buf_pa(NULL)
{
  pa_new(__max(m_io_chunk_size, m_file_chunk_size), m_buf, m_buf_pa);
  SetName(".");
}

void CGlobalItems::decrement_and_wait(int nr_sem)
{
  if(syn->decrement_and_wait(nr_sem))
    exit(1);
}

int TestDirOps(int directory_size, int max_size, int min_size
             , int num_directories, CGlobalItems &globals);
int TestFileOps(int file_size, CGlobalItems &globals);

static bool exitNow;
static bool already_printed_error;

extern "C"
{
  void ctrl_c_handler(int sig, siginfo_t *siginf, void *unused)
  {
    if(siginf->si_signo == SIGXCPU)
      fprintf(stderr, "Exceeded CPU usage.\n");
    else if(siginf->si_signo == SIGXFSZ)
      fprintf(stderr, "exceeded file storage limits.\n");
    exitNow = true;
  }
}

int main(int argc, char *argv[])
{
  int    file_size = DefaultFileSize;
  int    directory_size = DefaultDirectorySize;
  int    directory_max_size = DefaultDirectoryMaxSize;
  int    directory_min_size = DefaultDirectoryMinSize;
  int    num_bonnie_procs = 0;
  int    num_directories = 1;
  int    test_count = -1;
  const char * machine = NULL;
  char *userName = NULL, *groupName = NULL;
  CGlobalItems globals(&exitNow);
  bool setSize = false;

  exitNow = false;
  already_printed_error = false;

  struct sigaction sa;
  sa.sa_sigaction = &ctrl_c_handler;
  sa.sa_flags = SA_RESETHAND | SA_SIGINFO;
  if(sigaction(SIGINT, &sa, NULL)
   || sigaction(SIGXCPU, &sa, NULL)
   || sigaction(SIGXFSZ, &sa, NULL))
  {
    printf("Can't handle SIGINT.\n");
    return 1;
  }
  sa.sa_handler = SIG_IGN;
  if(sigaction(SIGHUP, &sa, NULL))
  {
    printf("Can't handle SIGHUP.\n");
    return 1;
  }

#ifdef _SC_PHYS_PAGES
  int page_size = sysconf(_SC_PAGESIZE);
  int num_pages = sysconf(_SC_PHYS_PAGES);
  if(page_size != -1 && num_pages != -1)
  {
    globals.ram = page_size/1024 * (num_pages/1024);
  }
#endif

  pid_t myPid = 0;
  myPid = getpid();
  globals.timer.random_source.seedNum(myPid ^ time(NULL));
  int concurrency = 1;

  int int_c;
  while(-1 != (int_c = getopt(argc, argv, "bc:d:f::g:l:m:n:p:qr:s:u:x:y:z:Z:"
#ifdef O_DIRECT
                             "D"
#endif
                            )) )
  {
    switch(char(int_c))
    {
      case '?':
      case ':':
        usage();
      break;
      case 'b':
        globals.bufSync = true;
      break;
      case 'c':
        concurrency = atoi(optarg);
      break;
      case 'd':
        if(chdir(optarg))
        {
          fprintf(stderr, "Can't change to directory \"%s\".\n", optarg);
          usage();
        }
      break;
      case 'f':
        if(optarg)
          globals.byte_io_size = atoi(optarg);
        else
          globals.byte_io_size = 0;
      break;
      case 'm':
        machine = optarg;
      break;
      case 'n':
      {
        char *sbuf = _strdup(optarg);
        char *size = strtok(sbuf, ":");
        directory_size = size_from_str(size, "m");
        size = strtok(NULL, ":");
        if(size)
        {
          directory_max_size = size_from_str(size, "kmg");
          size = strtok(NULL, ":");
          if(size)
          {
            directory_min_size = size_from_str(size, "kmg");
            size = strtok(NULL, ":");
            if(size)
            {
              num_directories = size_from_str(size, "k");
              size = strtok(NULL, "");
              if(size)
              {
                int tmp = size_from_str(size, "kmg");
                globals.set_file_chunk_size(tmp);
              }
            }
          }
        }
        free(sbuf);
      }
      break;
      case 'p':
        num_bonnie_procs = atoi(optarg);
                        /* Set semaphore to # of bonnie++ procs
                           to synchronize */
      break;
      case 'q':
        globals.quiet = true;
      break;
      case 'r':
        globals.ram = atoi(optarg);
      break;
      case 's':
      {
        char *sbuf = _strdup(optarg);
        char *size = strtok(sbuf, ":");
#ifdef _LARGEFILE64_SOURCE
        file_size = size_from_str(size, "gt");
#else
        file_size = size_from_str(size, "g");
#endif
        size = strtok(NULL, "");
        if(size)
        {
          int tmp = size_from_str(size, "k");
          globals.set_io_chunk_size(tmp);
        }
        setSize = true;
        free(sbuf);
      }
      break;
      case 'g':
        if(groupName)
          usage();
        groupName = optarg;
      break;
      case 'u':
      {
        if(userName)
          usage();
        userName = _strdup(optarg);
        int i;
        for(i = 0; userName[i] && userName[i] != ':'; i++) {}

        if(userName[i] == ':')
        {
          if(groupName)
            usage();
          userName[i] = '\0';
          groupName = &userName[i + 1];
        }
      }
      break;
      case 'x':
        test_count = atoi(optarg);
      break;
      case 'y':
                        /* tell procs to synchronize via previous
                           defined semaphore */
        switch(tolower(optarg[0]))
        {
        case 's':
          globals.setSync(eSem, SemKey, TestCount);
        break;
        case 'p':
          globals.setSync(ePrompt);
        break;
        }
        globals.sync_bonnie = true;
      break;
      case 'z':
      {
        UINT tmp;
        if(sscanf(optarg, "%u", &tmp) == 1)
          globals.timer.random_source.seedNum(tmp);
      }
      break;
#ifdef O_DIRECT
      case 'D':
        /* open file descriptor with direct I/O */
        globals.use_direct_io = true;
      break;
#endif
      case 'Z':
      {
        if(globals.timer.random_source.seedFile(optarg))
          return eParam;
      }
      break;
    }
  }
  if(concurrency < 1 || concurrency > 200)
    usage();
  if(!globals.syn)
    globals.setSync(eNone);
  if(optind < argc)
    usage();

  if(globals.ram && !setSize)
  {
    if(file_size < (globals.ram * 2))
      file_size = globals.ram * 2;
    // round up to the nearest gig
    if(file_size % 1024 > 512)
      file_size = file_size + 1024 - (file_size % 1024);
  }
#ifndef _LARGEFILE64_SOURCE
  if(file_size == 2048)
    file_size = 2047;
  if(file_size > 2048)
  {
    fprintf(stderr, "Large File Support not present, can't do %dM.\n", file_size);
    usage();
  }
#endif
  globals.byte_io_size = __min(file_size, globals.byte_io_size);
  globals.byte_io_size = __max(0, globals.byte_io_size);

  if(machine == NULL)
  {
    struct utsname utsBuf;
    if(uname(&utsBuf) != -1)
      machine = utsBuf.nodename;
  }

  globals.timer.setMachineName(machine);
  globals.timer.setConcurrency(concurrency);

  if(userName || groupName)
  {
    if(bon_setugid(userName, groupName, globals.quiet))
      return 1;
    if(userName)
      free(userName);
  }
  else if(geteuid() == 0)
  {
    fprintf(stderr, "You must use the \"-u\" switch when running as root.\n");
    usage();
  }

  if(num_bonnie_procs && globals.sync_bonnie)
    usage();

  if(num_bonnie_procs)
  {
    globals.setSync(eSem, SemKey, TestCount);
    if(num_bonnie_procs == -1)
    {
      return globals.syn->clear_sem();
    }
    else
    {
      return globals.syn->create(num_bonnie_procs);
    }
  }

  if(globals.sync_bonnie)
  {
    if(globals.syn->get_semid())
      return 1;
  }

  if(file_size < 0 || directory_size < 0 || (!file_size && !directory_size) )
    usage();
  if(globals.io_chunk_size() < 256 || globals.io_chunk_size() > Unit)
    usage();
  if(globals.file_chunk_size() < 256 || globals.file_chunk_size() > Unit)
    usage();
  int i;
  globals.io_chunk_bits = 0;
  globals.file_chunk_bits = 0;
  for(i = globals.io_chunk_size(); i > 1; i = i >> 1, globals.io_chunk_bits++)
  {}

  if(1 << globals.io_chunk_bits != globals.io_chunk_size())
    usage();
  for(i = globals.file_chunk_size(); i > 1; i = i >> 1, globals.file_chunk_bits++)
  {}

  if(1 << globals.file_chunk_bits != globals.file_chunk_size())
    usage();

  if( (directory_max_size != -1 && directory_max_size != -2)
     && (directory_max_size < directory_min_size || directory_max_size < 0
     || directory_min_size < 0) )
    usage();
#ifndef _LARGEFILE64_SOURCE
  if(file_size > (1 << (31 - 20 + globals.io_chunk_bits)) )
  {
    fprintf(stderr
   , "The small chunk size and large IO size make this test impossible in 32bit.\n");
    usage();
  }
#endif
  if(file_size && globals.ram && (file_size * concurrency) < (globals.ram * 2) )
  {
    fprintf(stderr
          , "File size should be double RAM for good results, RAM is %dM.\n"
          , globals.ram);
    usage();
  }

  // if doing more than one test run then we print a header before the
  // csv format output.
  if(test_count > 1)
  {
    globals.timer.SetType(BonTimer::csv);
    globals.timer.PrintHeader(stdout);
  }
  for(; test_count > 0 || test_count == -1; test_count--)
  {
    globals.timer.Initialize();
    int rc;
    rc = TestFileOps(file_size, globals);
    if(rc) return rc;
    rc = TestDirOps(directory_size, directory_max_size, directory_min_size
                  , num_directories, globals);
    if(rc) return rc;
    // if we are only doing one test run then print a plain-text version of
    // the results before printing a csv version.
    if(test_count == -1)
    {
      globals.timer.SetType(BonTimer::txt);
      rc = globals.timer.DoReportIO(file_size, globals.byte_io_size
                    , globals.io_chunk_size(), globals.quiet ? stderr : stdout);
      rc |= globals.timer.DoReportFile(directory_size
                    , directory_max_size, directory_min_size, num_directories
                    , globals.file_chunk_size()
                    , globals.quiet ? stderr : stdout);
    }
    // print a csv version in every case
    globals.timer.SetType(BonTimer::csv);
    rc = globals.timer.DoReportIO(file_size, globals.byte_io_size
                   , globals.io_chunk_size(), stdout);
    rc |= globals.timer.DoReportFile(directory_size
                    , directory_max_size, directory_min_size, num_directories
                    , globals.file_chunk_size(), stdout);
    if(rc) return rc;
  }
  return eNoErr;
}

int
TestFileOps(int file_size, CGlobalItems &globals)
{
  if(file_size)
  {
    CFileOp file(globals.timer, file_size, globals.io_chunk_bits, globals.bufSync
#ifdef O_DIRECT
               , globals.use_direct_io
#endif
               );
    int    num_chunks;
    int    words;
    char  *buf = globals.buf();
    int    bufindex;
    int    i;

    // default is we have 1M / 8K * 300 chunks = 38400
    num_chunks = Unit / globals.io_chunk_size() * file_size;
    int char_io_chunks = Unit / globals.io_chunk_size() * globals.byte_io_size;

    int rc;
    rc = file.Open(globals.name, true);
    if(rc)
      return rc;
    if(exitNow)
      return eCtrl_C;
    Duration dur;

    globals.timer.start();
    if(char_io_chunks)
    {
      dur.reset();
      globals.decrement_and_wait(ByteWrite);
      // Fill up a file, writing it a char at a time
      if(!globals.quiet) fprintf(stderr, "Writing a byte at a time...");
      for(words = 0; words < char_io_chunks; words++)
      {
        dur.start();
        if(file.write_block_byte() == -1)
          return 1;
        dur.stop();
        if(exitNow)
          return eCtrl_C;
      }
      fflush(NULL);
      /*
       * note that we always close the file before measuring time, in an
       *  effort to force as much of the I/O out as we can
       */
      file.Close();
      globals.timer.stop_and_record(ByteWrite);
      globals.timer.add_latency(ByteWrite, dur.getMax());
      if(!globals.quiet) fprintf(stderr, "done\n");
    }
    /* Write the whole file from scratch, again, with block I/O */
    if(file.reopen(true))
      return 1;
    dur.reset();
    globals.decrement_and_wait(FastWrite);
    if(!globals.quiet) fprintf(stderr, "Writing intelligently...");
    memset(buf, 0, globals.io_chunk_size());
    globals.timer.start();
    bufindex = 0;
    // for the number of chunks of file data
    for(i = 0; i < num_chunks; i++)
    {
      if(exitNow)
        return eCtrl_C;
      // for each chunk in the Unit
      buf[bufindex]++;
      bufindex = (bufindex + 1) % globals.io_chunk_size();
      dur.start();
      if(file.write_block(PVOID(buf)) == -1)
      {
        fprintf(stderr, "Can't write block %d.\n", i);
        return 1;
      }
      dur.stop();
    }
    file.Close();
    globals.timer.stop_and_record(FastWrite);
    globals.timer.add_latency(FastWrite, dur.getMax());
    if(!globals.quiet) fprintf(stderr, "done\n");


    /* Now read & rewrite it using block I/O.  Dirty one word in each block */
    if(file.reopen(false))
      return 1;
    if (file.seek(0, SEEK_SET) == -1)
    {
      if(!globals.quiet) fprintf(stderr, "error in lseek(2) before rewrite\n");
      return 1;
    }
    dur.reset();
    globals.decrement_and_wait(ReWrite);
    if(!globals.quiet) fprintf(stderr, "Rewriting...");
    globals.timer.start();
    bufindex = 0;
    for(words = 0; words < num_chunks; words++)
    { // for each chunk in the file
      dur.start();
      if (file.read_block(PVOID(buf)) == -1)
        return 1;
      bufindex = bufindex % globals.io_chunk_size();
      buf[bufindex]++;
      bufindex++;
      if (file.seek(-1, SEEK_CUR) == -1)
        return 1;
      if (file.write_block(PVOID(buf)) == -1)
        return io_error("re write(2)");
      dur.stop();
      if(exitNow)
        return eCtrl_C;
    }
    file.Close();
    globals.timer.stop_and_record(ReWrite);
    globals.timer.add_latency(ReWrite, dur.getMax());
    if(!globals.quiet) fprintf(stderr, "done\n");

    if(char_io_chunks)
    {
      // read them all back a byte at a time
      if(file.reopen(false))
        return 1;
      dur.reset();
      globals.decrement_and_wait(ByteRead);
      if(!globals.quiet) fprintf(stderr, "Reading a byte at a time...");
      globals.timer.start();

      for(words = 0; words < char_io_chunks; words++)
      {
        dur.start();
        if(file.read_block_byte(buf) == -1)
          return 1;
        dur.stop();
        if(exitNow)
          return eCtrl_C;
      }

      file.Close();
      globals.timer.stop_and_record(ByteRead);
      globals.timer.add_latency(ByteRead, dur.getMax());
      if(!globals.quiet) fprintf(stderr, "done\n");
    }

    /* Now suck it in, Chunk at a time, as fast as we can */
    if(file.reopen(false))
      return 1;
    if (file.seek(0, SEEK_SET) == -1)
      return io_error("lseek before read");
    dur.reset();
    globals.decrement_and_wait(FastRead);
    if(!globals.quiet) fprintf(stderr, "Reading intelligently...");
    globals.timer.start();
    for(i = 0; i < num_chunks; i++)
    { /* per block */
      dur.start();
      if ((words = file.read_block(PVOID(buf))) == -1)
        return io_error("read(2)");
      dur.stop();
      if(exitNow)
        return eCtrl_C;
    } /* per block */
    file.Close();
    globals.timer.stop_and_record(FastRead);
    globals.timer.add_latency(FastRead, dur.getMax());
    if(!globals.quiet) fprintf(stderr, "done\n");

    globals.timer.start();
    if(file.seek_test(globals.timer.random_source, globals.quiet, *globals.syn))
      return 1;

    /*
     * Now test random seeks; first, set up for communicating with children.
     * The object of the game is to do "Seeks" lseek() calls as quickly
     *  as possible.  So we'll farm them out among SeekProcCount processes.
     *  We'll control them by writing 1-byte tickets down a pipe which
     *  the children all read.  We write "Seeks" bytes with val 1, whichever
     *  child happens to get them does it and the right number of seeks get
     *  done.
     * The idea is that since the write() of the tickets is probably
     *  atomic, the parent process likely won't get scheduled while the
     *  children are seeking away.  If you draw a picture of the likely
     *  timelines for three children, it seems likely that the seeks will
     *  overlap very nicely with the process scheduling with the effect
     *  that there will *always* be a seek() outstanding on the file.
     * Question: should the file be opened *before* the fork, so that
     *  all the children are lseeking on the same underlying file object?
     */
  }
  return eNoErr;
}

int
TestDirOps(int directory_size, int max_size, int min_size
         , int num_directories, CGlobalItems &globals)
{
  COpenTest open_test(globals.file_chunk_size(), globals.bufSync, globals.doExit);
  if(!directory_size)
  {
    return 0;
  }
  // if directory_size (in K) * data per file*2 > (ram << 10) (IE memory /1024)
  // then the storage of file names will take more than half RAM and there
  // won't be enough RAM to have Bonnie++ paged in and to have a reasonable
  // meta-data cache.
  if(globals.ram && directory_size * MaxDataPerFile * 2 > (globals.ram << 10))
  {
    fprintf(stderr
        , "When testing %dK of files in %d MiB of RAM the system is likely to\n"
           "start paging Bonnie++ data and the test will give suspect\n"
           "results, use less files or install more RAM for this test.\n"
          , directory_size, globals.ram);
    return eParam;
  }
  // Can't use more than 1G of RAM
  if(directory_size * MaxDataPerFile > (1 << 20))
  {
    fprintf(stderr, "Not enough ram to test with %dK files.\n"
                  , directory_size);
    return eParam;
  }
  globals.decrement_and_wait(CreateSeq);
  if(!globals.quiet) fprintf(stderr, "Create files in sequential order...");
  if(open_test.create(globals.name, globals.timer, directory_size
                    , max_size, min_size, num_directories, false))
    return 1;
  globals.decrement_and_wait(StatSeq);
  if(!globals.quiet) fprintf(stderr, "done.\nStat files in sequential order...");
  if(open_test.stat_sequential(globals.timer))
    return 1;
  globals.decrement_and_wait(DelSeq);
  if(!globals.quiet) fprintf(stderr, "done.\nDelete files in sequential order...");
  if(open_test.delete_sequential(globals.timer))
    return 1;
  if(!globals.quiet) fprintf(stderr, "done.\n");

  globals.decrement_and_wait(CreateRand);
  if(!globals.quiet) fprintf(stderr, "Create files in random order...");
  if(open_test.create(globals.name, globals.timer, directory_size
                    , max_size, min_size, num_directories, true))
    return 1;
  globals.decrement_and_wait(StatRand);
  if(!globals.quiet) fprintf(stderr, "done.\nStat files in random order...");
  if(open_test.stat_random(globals.timer))
    return 1;
  globals.decrement_and_wait(DelRand);
  if(!globals.quiet) fprintf(stderr, "done.\nDelete files in random order...");
  if(open_test.delete_random(globals.timer))
    return 1;
  if(!globals.quiet) fprintf(stderr, "done.\n");
  return eNoErr;
}

void
usage()
{
  fprintf(stderr, "usage:\n"
    "bonnie++ [-d scratch-dir] [-c concurrency] [-s size(MiB)[:chunk-size(b)]]\n"
    "      [-n number-to-stat[:max-size[:min-size][:num-directories[:chunk-size]]]]\n"
    "      [-m machine-name] [-r ram-size-in-MiB]\n"
    "      [-x number-of-tests] [-u uid-to-use:gid-to-use] [-g gid-to-use]\n"
    "      [-q] [-f] [-b] [-p processes | -y] [-z seed | -Z random-file]\n"
#ifdef O_DIRECT
    "      [-D]\n"
#endif
    "\nVersion: " BON_VERSION "\n");
  exit(eParam);
}

int
io_error(CPCCHAR message, bool do_exit)
{
  char buf[1024];

  if(!already_printed_error && !do_exit)
  {
    sprintf(buf, "Bonnie: drastic I/O error (%s)", message);
    perror(buf);
    already_printed_error = 1;
  }
  if(do_exit)
    exit(1);
  return(1);
}

