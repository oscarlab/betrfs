#include <stdlib.h>
#include "thread.h"
#include <stdio.h>

#include <unistd.h>
#include <time.h>
#include <sys/wait.h>
#include <pthread.h>

Thread::Thread()
 : m_read(-1)
 , m_write(-1)
 , m_threadNum(-1)
 , m_thread_info(NULL)
 , m_parentRead(-1)
 , m_parentWrite(-1)
 , m_childRead(-1)
 , m_childWrite(-1)
 , m_numThreads(0)
 , m_retVal(NULL)
{
}

Thread::Thread(int threadNum, const Thread *parent)
 : m_read(parent->m_childRead)
 , m_write(parent->m_childWrite)
 , m_threadNum(threadNum)
 , m_thread_info(NULL)
 , m_parentRead(-1)
 , m_parentWrite(-1)
 , m_childRead(-1)
 , m_childWrite(-1)
 , m_numThreads(parent->m_numThreads)
 , m_retVal(&parent->m_retVal[threadNum])
{
}

Thread::~Thread()
{
  if(m_threadNum == -1)
  {
    for(int i = 0; i < m_numThreads; i++)
    {
      pthread_join(m_thread_info[i], NULL);
    }
    delete m_thread_info;
    close(m_parentRead);
    close(m_parentWrite);
    close(m_childRead);
    close(m_childWrite);
    delete m_retVal;
  }
}

// for the benefit of this function and the new Thread class it may create
// the Thread class must do nothing of note in it's constructor or it's
// go() member function.
PVOID thread_func(PVOID param)
{
  THREAD_DATA *td = (THREAD_DATA *)param;
  Thread *thread = td->f->newThread(td->threadNum);
  thread->setRetVal(thread->action(td->param));
  delete thread;
  delete td;
  return NULL;
}

void Thread::go(PVOID param, int num)
{
  m_numThreads += num;
  FILE_TYPE control[2];
  FILE_TYPE feedback[2];
  if (pipe(feedback) || pipe(control))
  {
    fprintf(stderr, "Can't open pipes.\n");
    exit(1);
  }
  m_parentRead = feedback[0];
  m_parentWrite = control[1];
  m_childRead = control[0];
  m_childWrite = feedback[1];
  m_read = m_parentRead;
  m_write = m_parentWrite;
  m_readPoll.events = POLLIN | POLLERR | POLLHUP | POLLNVAL;
  m_writePoll.events = POLLOUT | POLLERR | POLLHUP | POLLNVAL;
  m_readPoll.fd = m_parentRead;
  m_writePoll.fd = m_parentWrite;
  pthread_attr_t attr;
  if(pthread_attr_init(&attr))
    fprintf(stderr, "Can't init thread attributes.\n");
  if(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE))
    fprintf(stderr, "Can't set thread attributes.\n");
  m_thread_info = new pthread_t[num];

  m_retVal = new int[num + 1];
  for(int i = 1; i <= num; i++)
  {
    m_retVal[i] = -1;
    THREAD_DATA *td = new THREAD_DATA;
    td->f = this;
    td->param = param;
    td->threadNum = i;
    int p = pthread_create(&m_thread_info[i - 1], &attr, thread_func, PVOID(td));
    if(p)
    {
      fprintf(stderr, "Can't create a thread.\n");
      exit(1);
    }
  }
  if(pthread_attr_destroy(&attr))
    fprintf(stderr, "Can't destroy thread attributes.\n");
  m_readPoll.fd = m_read;
  m_writePoll.fd = m_write;
}

void Thread::setRetVal(int rc)
{
  *m_retVal = rc;
}

int Thread::Read(PVOID buf, int size, int timeout)
{
  if(timeout)
  {
    int rc = poll(&m_readPoll, 1, timeout * 1000);
    if(rc < 0)
    {
      fprintf(stderr, "Can't poll read ITC.\n");
      return -1;
    }
    if(!rc)
      return 0;
  }
  if(size != read(m_read, buf, size) )
  {
    fprintf(stderr, "Can't read data from ITC pipe.\n");
    return -1;
  }
  return size;
}

int Thread::Write(PVOID buf, int size, int timeout)
{
  if(timeout)
  {
    int rc = poll(&m_writePoll, 1, timeout * 1000);
    if(rc < 0)
    {
      fprintf(stderr, "Can't poll write ITC.\n");
      return -1;
    }
    if(!rc)
      return 0;
  }
  if(size != write(m_write, buf, size))
  {
    fprintf(stderr, "Can't write data to ITC pipe.\n");
    return -1;
  }
  return size;
}

