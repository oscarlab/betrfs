#ifndef DURATION_H
#define DURATION_H

#include "port.h"

#include <sys/time.h>
#include <unistd.h>

class Duration_Base
{
public:
  Duration_Base();
  virtual ~Duration_Base() {};
  void reset(){ m_max = 0.0; }
  double start();
  double stop();
  double getMax() { return m_max; }

  virtual bool getTime(double *tv) = 0;

private:
  double m_start;
  double m_max;
};

class Duration : public Duration_Base
{
public:
  virtual bool getTime(double *time);
};

class CPU_Duration : public Duration_Base
{
public:
  virtual bool getTime(double *time);
};

#endif
