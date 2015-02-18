#include "semaphore.h"

typedef enum
{
  eSem, ePrompt, eNone
} SYNC_TYPE;

class Sync : public Semaphore
{
public:
  Sync(SYNC_TYPE type, int semKey = 0, int num_tests = 0);

  int decrement_and_wait(int nr_sem);

  // get the handle to a semaphore set previously created
  int get_semid();

private:
  SYNC_TYPE m_type;

};

