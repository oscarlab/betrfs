#include "zcav_io.h"

#include <unistd.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

ZcavRead::~ZcavRead()
{
  delete m_name;
}

int ZcavRead::Open(bool *finished, int block_size, const char *file
                 , const char *log, int chunk_size, int do_write)
{
  m_name = strdup(file);
  m_finished = finished;
  m_block_size = block_size;
  m_chunk_size = chunk_size;
  m_do_write = do_write;
  m_buf = calloc(chunk_size * MEG, 1);

  if(strcmp(file, "-"))
  {
    if(m_do_write)
      m_fd = file_open(file, O_WRONLY);
    else
      m_fd = file_open(file, O_RDONLY);
    if(m_fd == -1)
    {
      fprintf(stderr, "Can't open %s\n", file);
      return 1;
    }
  }
  else
  {
    m_fd = 0;
  }
  if(strcmp(log, "-"))
  {
    m_logFile = true;
    m_log = fopen(log, "w");
    if(m_log == NULL)
    {
      fprintf(stderr, "Can't open %s\n", log);
      close(m_fd);
      return 1;
    }
  }
  else
  {
    m_logFile = false;
    m_log = stdout;
  }
  return 0;
}

void ZcavRead::Close()
{
  if(m_logFile)
    fclose(m_log);
  if(m_fd != 0)
    ::close(m_fd);
}

int ZcavRead::writeStatus(int fd, char c)
{
  if(write(fd, &c, 1) != 1)
  {
    fprintf(stderr, "Write channel broken\n");
    return 1;
  }
  return 0;
}

int ZcavRead::Read(int max_loops, int max_size, int writeCom, int skip_rate, int start_offset)
{
  bool exiting = false;
  if(max_loops == 1)
    fprintf(m_log, "#block offset (GiB), MiB/s, time\n");
  for(int loops = 0; !exiting && loops < max_loops; loops++)
  {
    int i = 0;
#ifdef _LARGEFILE64_SOURCE
    if(start_offset)
    {
      OFF_TYPE real_offset = OFF_TYPE(start_offset) * OFF_TYPE(m_block_size) * OFF_TYPE(1<<20);
      if(file_lseek(m_fd, real_offset, SEEK_CUR) == OFF_TYPE(-1))
      {
        fprintf(stderr, "Can't lseek().\n");
        writeStatus(writeCom, eSEEK);
        return 1;
      }
      i = start_offset;
    }
    else
#endif
    if(lseek(m_fd, 0, SEEK_SET))
    {
      fprintf(stderr, "Can't lseek().\n");
      writeStatus(writeCom, eSEEK);
      return 1;
    }

    // i is block index
    double total_read_time = 0.0;
    bool nextLoop = false;
    for( ; !nextLoop && (!max_size || i < max_size)
              && (loops == 0 || (m_times[i] && m_times[i][0] != -1.0))
              && (!max_size || i < max_size); i++)
    {
      double read_time = access_data(i ? skip_rate - 1 : 0);
      if(read_time < 0.0)
      {
        if(i == 0)
        {
          fprintf(stderr, "Data file/device \"%s\" too small.\n", m_name);
          writeStatus(writeCom, eSIZE);
          return 1;
        }
        nextLoop = true;
        break;
      }
      total_read_time += read_time;
      if(max_loops == 1)
      {
        printavg(i * skip_rate, read_time, m_block_size);
      }
      else
      {
        if(loops == 0)
        {
          m_times.push_back(new double[max_loops]);
          m_count.push_back(0);
        }
        m_times[i][loops] = read_time;
        m_count[i]++;
      }
    } // end loop for reading blocks

    time_t now = time(NULL);
    struct tm *cur_time = localtime(&now);
    fprintf(stderr, "# Finished loop %d, on device %s at %d:%02d:%02d\n"
          , loops + 1, m_name, cur_time->tm_hour, cur_time->tm_min
          , cur_time->tm_sec);
    fprintf(m_log, "# Read %d megs in %d seconds, %d megabytes per second.\n"
         , i * m_block_size, int(total_read_time)
         , int(double(i * m_block_size) / total_read_time));

    if(exiting)
      return 1;
  } // end loop for multiple disk reads
  if(max_loops > 1)
  {
    fprintf(m_log, "#loops: %d\n", max_loops);
    fprintf(m_log, "#block offset (GiB), MiB/s, time\n");
    for(int i = 0; m_times[i]; i++)
      printavg(i * skip_rate, average(m_times[i], m_count[i]), m_block_size);
  }
  writeStatus(writeCom, eEND);
  return 0;
}

void ZcavRead::printavg(int position, double avg, int block_size)
{
  if(avg < 1.0)
    fprintf(m_log, "#%.2f ++++ %.3f\n", float(position) * float(block_size) / 1024.0, avg);
  else
    fprintf(m_log, "%.2f %.2f %.3f\n", float(position) * float(block_size) / 1024.0, double(block_size) / avg, avg);
}

int compar(const void *a, const void *b)
{
  double *c = (double *)(a);
  double *d = (double *)(b);
  if(*c < *d) return -1;
  if(*c > *d) return 1;
  return 0;
}

// Returns the mean of the values in the array.  If the array contains
// more than 2 items then discard the highest and lowest thirds of the
// results before calculating the mean.
double average(double *array, int count)
{
  qsort(array, count, sizeof(double), compar);
  int skip = count / 3;
  int arr_items = count - (skip * 2);
  double total = 0.0;
  for(int i = skip; i < (count - skip); i++)
  {
    total += double(array[i]);
  }
  return total / double(arr_items);
}

// just like read() or write() but will not return a partial result and the
// size is expressed in MEG.
ssize_t ZcavRead::access_all(int count)
{
  ssize_t total = 0;
  count *= MEG;
  while(total != static_cast<ssize_t>(count) )
  {
    ssize_t rc;
    // for both read and write just pass the base address of the buffer
    // as we don't care for the data, if we ever do checksums we have to
    // change this
    if(m_do_write)
      rc = write(m_fd, m_buf, count - total);
    else
      rc = read(m_fd, m_buf, count - total);
    if(rc == -1 || rc == 0)
      return -1;
    total += rc;
  }
  if(m_do_write && fsync(m_fd))
    return -1;
  return total / MEG;
}

// Read/write a block of data
double ZcavRead::access_data(int skip)
{
#ifdef _LARGEFILE64_SOURCE
  if(skip)
  {
    OFF_TYPE real_offset = OFF_TYPE(skip) * OFF_TYPE(m_block_size) * OFF_TYPE(1<<20);
    if(file_lseek(m_fd, real_offset, SEEK_CUR) == OFF_TYPE(-1))
      return -1.0;
  }
#endif

  m_dur.start();
  for(int i = 0; i < m_block_size; i+= m_chunk_size)
  {
    int access_size = m_chunk_size;
    if(i + m_chunk_size > m_block_size)
      access_size = m_block_size - i;
    int rc = access_all(access_size);
    if(rc != access_size)
      return -1.0;
  }
  return m_dur.stop();
}

