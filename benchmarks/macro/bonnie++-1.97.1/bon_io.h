#ifndef BON_FILE
#define BON_FILE

#include "bonnie.h"
#include "thread.h"
class Sync;
class BonTimer;
class Rand;

class CFileOp : public Thread
{
public:
  CFileOp(BonTimer &timer, int file_size, int chunk_bits, bool use_sync
#ifdef O_DIRECT
        , bool use_direct_io = false
#endif
         );
  int Open(CPCCHAR base_name, bool create);
  ~CFileOp();
  int write_block_byte();
  int write_block(PVOID buf);
  int read_block_byte(char *buf);
  int read_block(PVOID buf);
  int seek(int offset, int whence);
  int doseek(unsigned int where, bool update);
  int seek_test(Rand &r, bool quiet, Sync &s);
  void Close();
  // reopen a file, bool for whether the file should be unlink()'d and creat()'d
  int reopen(bool create);
  BonTimer &getTimer() { return m_timer; }
  int chunks() const { return m_total_chunks; }
private:
  virtual int action(PVOID param); // called for seek test
  virtual Thread *newThread(int threadNum);
  CFileOp(int threadNum, CFileOp *parent);
  int m_open(CPCCHAR base_name, bool create);

  BonTimer &m_timer;
  int m_file_size;
  FILE_TYPE m_fd;
  bool m_isopen;
  char *m_name;
  bool m_sync;
#ifdef O_DIRECT
  bool m_use_direct_io;
#endif
  const int m_chunk_bits, m_chunk_size;
  int m_total_chunks;
  char *m_buf;

  CFileOp(const CFileOp &f);
  CFileOp & operator =(const CFileOp &f);
};


#endif
