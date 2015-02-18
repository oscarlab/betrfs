#include "bonnie.h"
#include <stdlib.h>
#include <fcntl.h>

#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>
#include "sync.h"

#include <string.h>
#include <limits.h>

#include "bon_io.h"
#include "bon_time.h"


#define END_SEEK_PROCESS INT_MIN

CFileOp::~CFileOp()
{
  Close();
  if(m_name)
  {
    unlink(m_name);
    free(m_name);
  }
  delete m_buf;
}

Thread *CFileOp::newThread(int threadNum)
{
  return new CFileOp(threadNum, this);
}

CFileOp::CFileOp(int threadNum, CFileOp *parent)
 : Thread(threadNum, parent)
 , m_timer(parent->m_timer)
 , m_file_size(parent->m_file_size)
 , m_fd(-1)
 , m_isopen(false)
 , m_name(PCHAR(malloc(strlen(parent->m_name) + 5)))
 , m_sync(parent->m_sync)
#ifdef O_DIRECT
 , m_use_direct_io(parent->m_use_direct_io)
#endif
 , m_chunk_bits(parent->m_chunk_bits)
 , m_chunk_size(parent->m_chunk_size)
 , m_total_chunks(parent->m_total_chunks)
 , m_buf(new char[m_chunk_size])
{
  strcpy(m_name, parent->m_name);
}

int CFileOp::action(PVOID)
{
  struct report_s seeker_report;
  if(reopen(false))
    return 1;
  int ticket;
  int rc;
  Duration dur, test_time;
  rc = Read(&ticket, sizeof(ticket), 0);
  CPU_Duration test_cpu;
  test_time.getTime(&seeker_report.StartTime);
  test_cpu.start();
  if(rc == sizeof(ticket) && ticket != END_SEEK_PROCESS) do
  {
    bool update = false;
    if(ticket < 0)
    {
      ticket = abs(ticket);
      update = true;
    }
    dur.start();
    if(doseek(ticket % m_total_chunks, update) )
      return 1;
    dur.stop();
  } while((rc = Read(&ticket, sizeof(ticket), 0)) == sizeof(ticket)
         && ticket != END_SEEK_PROCESS);

  if(rc != sizeof(ticket))
  {
    fprintf(stderr, "Can't read ticket.\n");
    return 1;
  }
  Close();
  // seeker report is start and end times, CPU used, and latency
  test_time.getTime(&seeker_report.EndTime);
  seeker_report.CPU = test_cpu.stop();
  seeker_report.Latency = dur.getMax();
  if(Write(&seeker_report, sizeof(seeker_report), 0) != sizeof(seeker_report))
  {
    fprintf(stderr, "Can't write report.\n");
    return 1;
  }
  return 0;
}

int CFileOp::seek_test(Rand &r, bool quiet, Sync &s)
{
  int seek_tickets[SeekProcCount + Seeks];
  int next;
  for(next = 0; next < Seeks; next++)
  {
    seek_tickets[next] = r.getNum();
    if(seek_tickets[next] < 0)
      seek_tickets[next] = abs(seek_tickets[next]);
    if(seek_tickets[next] % UpdateSeek == 0)
      seek_tickets[next] = -seek_tickets[next];
  }
  for( ; next < (Seeks + SeekProcCount); next++)
    seek_tickets[next] = END_SEEK_PROCESS;
  if(reopen(false))
    return 1;
  go(NULL, SeekProcCount);

  sleep(3);
  if(s.decrement_and_wait(Lseek))
    return 1;
  if(!quiet) fprintf(stderr, "start 'em...");
  if(Write(seek_tickets, sizeof(seek_tickets), 0) != int(sizeof(seek_tickets)) )
  {
    fprintf(stderr, "Can't write tickets.\n");
    return 1;
  }
  Close();
  for (next = 0; next < SeekProcCount; next++)
  { /* for each child */
    struct report_s seeker_report;

    int rc;
    if((rc = Read(&seeker_report, sizeof(seeker_report), 0))
        != sizeof(seeker_report))
    {
      fprintf(stderr, "Can't read from pipe, expected %d, got %d.\n"
                    , int(sizeof(seeker_report)), rc);
      return 1;
    }

    /*
     * each child writes back its CPU, start & end times.  The elapsed time
     *  to do all the seeks is the time the first child started until the
     *  time the last child stopped
     */
    m_timer.add_delta_report(seeker_report, Lseek);
    if(!quiet) fprintf(stderr, "done...");
  } /* for each child */
  if(!quiet) fprintf(stderr, "\n");
  return 0;
}

int CFileOp::seek(int offset, int whence)
{
  OFF_TYPE rc;
  OFF_TYPE real_offset = offset;
  real_offset *= m_chunk_size;
  rc = file_lseek(m_fd, real_offset, whence);

  if(rc == OFF_TYPE(-1))
  {
    sprintf(m_buf, "Error in lseek to chunk %d(" OFF_T_PRINTF ")", offset, real_offset);
    perror(m_buf);
    return rc;
  }
  return 0;
}

int CFileOp::read_block(PVOID buf)
{
  int total = 0;
  bool printed_error = false;
  while(total != m_chunk_size)
  {
    int rc = read(m_fd, buf, m_chunk_size - total);
    if(rc == -1)
    {
      io_error("re-write read"); // exits program
    }
    else if(rc != m_chunk_size)
    {
      if(!printed_error)
      {
        fprintf(stderr, "Can't read a full block, only got %d bytes.\n", rc);
        printed_error = true;
        if(rc == 0)
          return -1;
      }
    }
    total += rc;
  }
  return total;
}

int CFileOp::read_block_byte(char *buf)
{
  char next;
  for(int i = 0; i < m_chunk_size; i++)
  {
    if(read(m_fd, &next, 1) != 1)
    {
      fprintf(stderr, "Can't read a byte\n");
      return -1;
    }
    /* just to fool optimizers */
    buf[int(next)]++;
  }

  return 0;
}

int CFileOp::write_block(PVOID buf)
{
  int rc = ::write(m_fd, buf, m_chunk_size);
  if(rc != m_chunk_size)
  {
    perror("Can't write block.");
    return -1;
  }
  return rc;
}

int CFileOp::write_block_byte()
{
  for(int i = 0; i < m_chunk_size; i++)
  {
    char c = i & 0x7f;
    if(write(m_fd, &c, 1) != 1)
    {
      fprintf(stderr, "Can't write() - disk full?\n");
      return -1;
    }
  }
  return 0;
}

int CFileOp::Open(CPCCHAR base_name, bool create)
{
  m_name = PCHAR(malloc(strlen(base_name) + 5));
  strcpy(m_name, base_name);
  return reopen(create);
}

CFileOp::CFileOp(BonTimer &timer, int file_size, int chunk_bits, bool use_sync
#ifdef O_DIRECT
               , bool use_direct_io
#endif
                )
 : m_timer(timer)
 , m_file_size(file_size)
 , m_fd(-1)
 , m_isopen(false)
 , m_name(NULL)
 , m_sync(use_sync)
#ifdef O_DIRECT
 , m_use_direct_io(use_direct_io)
#endif
 , m_chunk_bits(chunk_bits)
 , m_chunk_size(1 << m_chunk_bits)
 , m_total_chunks(Unit / m_chunk_size * file_size)
 , m_buf(new char[m_chunk_size])
{
  if(m_total_chunks / file_size * m_chunk_size != Unit)
  {
    fprintf(stderr, "File size %d too big for chunk size %d\n", file_size, m_chunk_size);
    exit(1);
  }
}

int CFileOp::reopen(bool create)
{
  if(m_isopen) Close();

  m_isopen = true;
  if(m_open(m_name, create))
    return 1;
  return 0;
}

int CFileOp::m_open(CPCCHAR base_name, bool create)
{
  int flags;
  if(create)
  { /* create from scratch */
    unlink(base_name);
    flags = O_RDWR | O_CREAT | O_EXCL;
#ifdef O_DIRECT
    if(m_use_direct_io)
      flags |= O_DIRECT;
#endif
  }
  else
  {
    flags = O_RDWR;
#ifdef _LARGEFILE64_SOURCE
    flags |= O_LARGEFILE;
#endif
  }
  m_fd = file_open(base_name, flags, S_IRUSR | S_IWUSR);

  if(m_fd == -1)
  {
    fprintf(stderr, "Can't open file %s\n", base_name);
    return -1;
  }
  return 0;
}

void CFileOp::Close()
{
  if(!m_isopen)
    return;
  if(m_fd != -1)
  {
    if(fsync(m_fd))
      fprintf(stderr, "Can't sync file.\n");
    close(m_fd);
  }
  m_isopen = false;
  m_fd = -1;
}


/*
 * Do a typical-of-something random I/O.  Any serious application that
 *  has a random I/O bottleneck is going to be smart enough to operate
 *  in a page mode, and not stupidly pull individual words out at
 *  odd offsets.  To keep the cache from getting too clever, some
 *  pages must be updated.  However an application that updated each of
 *  many random pages that it looked at is hard to imagine.
 * However, it would be wrong to put the update percentage in as a
 *  parameter - the effect is too nonlinear.  Need a profile
 *  of what Oracle or Ingres or some such actually does.
 * Be warned - there is a *sharp* elbow in this curve - on a 1-MiB file,
 *  most substantial unix systems show >2000 random I/Os per second -
 *  obviously they've cached the whole thing and are just doing buffer
 *  copies.
 */
int
CFileOp::doseek(unsigned int where, bool update)
{
  if (seek(where, SEEK_SET) == -1)
    return -1;
  if (read_block(PVOID(m_buf)) == -1)
    return -1;

  /* every so often, update a block */
  if (update)
  { /* update this block */

    /* touch a byte */
    m_buf[where % m_chunk_size]--;
    if(seek(where, SEEK_SET) == -1)
      return io_error("lseek in doseek update");
    if (write_block(PVOID(m_buf)) == -1)
      return -1;
    if(m_sync)
    {
      if(fsync(m_fd))
      {
        fprintf(stderr, "Can't sync file.\n");
        return -1;
      }
    }
  } /* update this block */
  return 0;
}

