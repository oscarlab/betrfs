#include "port.h"
#include "semaphore.h"
#include "sync.h"
#include <stdio.h>

Sync::Sync(SYNC_TYPE type, int semKey, int num_tests)
 : Semaphore(semKey, num_tests)
 , m_type(type)
{
}

int Sync::decrement_and_wait(int nr_sem)
{
  switch(m_type)
  {
  case eSem:
    return Semaphore::decrement_and_wait(nr_sem);
  case ePrompt:
    printf("\n%d:\n", nr_sem);
    fflush(NULL);
    char buf[16];
    fgets(buf, sizeof(buf) - 1, stdin);
  case eNone:
  break;
  }
  return 0;
}

int Sync::get_semid()
{
  if(m_type == eSem)
    return Semaphore::get_semid();
  return 0;
}
