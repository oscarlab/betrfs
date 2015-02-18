#ifndef ZCAV_IO_H
#define ZCAV_IO_H

#include "bonnie.h"
#include <vector>

#include "duration.h"
using namespace std;

enum results
{
  eEND = 0,
  eSEEK = 1,
  eSIZE = 2
};

// Returns the mean of the values in the array.  If the array contains
// more than 2 items then discard the highest and lowest thirds of the
// results before calculating the mean.
double average(double *array, int count);

const int MEG = 1024*1024;
const int DEFAULT_BLOCK_SIZE = 512;
const int DEFAULT_CHUNK_SIZE = 1;

class ZcavRead
{
public:
  ZcavRead(){ m_name = NULL; }
  ~ZcavRead();

  int Open(bool *finished, int block_size, const char *file, const char *log
         , int chunk_size, int do_write);
  void Close();
  int Read(int max_loops, int max_size, int writeCom, int skip_rate, int start_offset);

private:
  ssize_t access_all(int count);

  // write the status to the parent thread
  int writeStatus(int fd, char c);

  // Read the m_block_count megabytes of data from the fd and return the
  // amount of time elapsed in seconds.
  double access_data(int skip);
  void printavg(int position, double avg, int block_size);

  bool *m_finished;
  vector <double *> m_times;
  vector<int> m_count; // number of times each block has been read
  void *m_buf;
  int m_fd;
  FILE *m_log;
  bool m_logFile;
  int m_block_size;
  char *m_name;
  int m_chunk_size;
  int m_do_write;
  Duration m_dur;

  ZcavRead(const ZcavRead &t);
  ZcavRead & operator =(const ZcavRead &t);
};

#endif

