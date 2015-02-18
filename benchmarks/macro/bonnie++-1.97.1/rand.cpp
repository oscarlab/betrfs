#include "rand.h"
#include <unistd.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

bool Rand::seedFile(CPCCHAR name)
{
  int fd = file_open(name, O_RDONLY);
  struct stat buf;
  if(fd == -1 || fstat(fd, &buf) == -1)
  {
    fprintf(stderr, "Can't open random file \"%s\".\n", name);
    if(fd != -1)
      close(fd);
    return true;
  }
  int size = buf.st_size / sizeof(int);
  delete(m_arr);
  m_arr = new int[size];
  m_size = size;
  if(size_t(read(fd, m_arr, size * sizeof(int))) != size * sizeof(int))
  {
    fprintf(stderr, "Can't read random data from \"%s\".\n", name);
    return true;
  }
  for(int i = 0; i < size; i++)
  {
    m_arr[i] = abs(int(ntohl(m_arr[i])));
  }
  close(fd);
  m_ind = -1;
  m_name = string(name);
  return false;
}
 
void Rand::seedNum(UINT num)
{
  delete(m_arr);
  m_arr = NULL;
  m_size = 0;
  srand(num);
  m_init = num;
  char buf[12];
  sprintf(buf, "%u", num);
  m_name = string(buf);
}

void Rand::reset()
{
  if(m_arr)
    m_ind = -1;
  else
    srand(m_init);
}
