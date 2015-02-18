using namespace std;

#include <stdlib.h>

#include "duration.h"
#define TIMEVAL_TO_DOUBLE(XX) (double((XX).tv_sec) + double((XX).tv_usec) / 1000000.0)

#include "conf.h"
#include <unistd.h>
#include <sys/resource.h>
#include <sys/time.h>

#ifdef HAVE_ALGORITHM
#include <algorithm>
#else
#ifdef HAVE_ALGO
#include <algo>
#else
#include <algo.h>
#endif
#endif

Duration_Base::Duration_Base()
 : m_start(0.0)
 , m_max(0.0)
{
}

double Duration_Base::start()
{
  getTime(&m_start);
  return m_start;
}

double Duration_Base::stop()
{
  double tv;
  getTime(&tv);
  double ret;
  ret = tv - m_start;
  m_max = __max(m_max, ret);
  return ret;
}

bool Duration::getTime(double *tv)
{
  TIMEVAL_TYPE t;
  if (gettimeofday(&t, static_cast<struct timezone *>(NULL)) == -1)
    return true;
  *tv = TIMEVAL_TO_DOUBLE(t);
  return false;
}

bool CPU_Duration::getTime(double *tv)
{
  struct rusage res_usage;
 
  getrusage(RUSAGE_SELF, &res_usage);
  *tv = TIMEVAL_TO_DOUBLE(res_usage.ru_utime) + TIMEVAL_TO_DOUBLE(res_usage.ru_stime);
  return false;
}
