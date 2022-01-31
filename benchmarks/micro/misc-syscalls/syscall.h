#ifndef SYSCALL_H
#define SYSCALL_H

#include <errno.h>
#include <unistd.h>
#include <inttypes.h>

#define ITERATIONS 1000000
#define SKIP 100000

#define int_80(no, rv) asm volatile("mov %1, %%eax\n"		\
			   	    "int $0x80\n"		\
				    "mov %%eax, %0"		\
				    : "=g"(rv) : "g"(no) : "eax", "memory")

#define syscall_instr(no, rv) asm volatile("mov %1, %%eax\n"		\
					   "syscall\n"	                \
					   "mov %%eax, %0"		\
					   : "=g"(rv) : "g"(no) : "eax", "memory")

 

#define sysenter_lite(no, rv, x) asm volatile("mov %1, %%eax\n"		\
					      "call *%2    \n"		\
					      "mov %%eax, %0"		\
					      : "=g"(rv) : "g"(no), "g"(x) : "eax", "memory")



__inline__ uint64_t rdtsc() {
  uint32_t lo, hi;
  /* We cannot use "=A", since this would use %rax on x86_64 */
  __asm__ __volatile__ ("mfence\n\trdtsc" : "=a" (lo), "=d" (hi));
  return (uint64_t)hi << 32 | lo;
}


uint64_t mean(uint64_t *samples, uint64_t size){

  uint64_t mean = 0, i;
  for(i = 0; i < size; i++){
    if(i >= SKIP && i < (ITERATIONS - SKIP))
      mean += samples[i];
  }
  size -= SKIP * 2;
  mean /= size;
  return mean;
}

double stddev(uint64_t *samples, uint64_t size, uint64_t mean){

  double stdev = 0.0;

  uint64_t i;
  for(i = 0; i < size; i++){
    if(i >= SKIP && i < (ITERATIONS - SKIP))
      stdev += pow(abs(samples[i] - mean), 2.0);
  }
  if(size == 0)
    return 0.0;

  size -= SKIP * 2;

  stdev /= (size - 1);
  stdev = sqrt(stdev);
  return stdev;
}

static int saucer(const void* p1, const void* p2){
  if(*((uint64_t *) p1) < *((uint64_t *) p2)) return -1;
  if(*((uint64_t *) p1) == *((uint64_t *) p2)) return 0;
  return 1;
}

#define exp(setup, syscall, cleanup, msg) do{           \
  int retval = 0;                                       \
  for(count = 0, total=0; count < ITERATIONS; count++){ \
    setup;                                              \
    start = rdtsc();                                    \
    syscall;                                            \
    end = rdtsc();                                      \
    samples[count] = end - start;                       \
    if (retval < 0) {                                   \
      printf("Oh no! Error in syscall:%d\n", errno);	\
      exit(100);					\
    }                                                   \
    cleanup;                                            \
  }                                                     \
  qsort(samples, ITERATIONS, sizeof(uint64_t), saucer); \
  avg = mean(samples, count);                           \
  stdev = stddev(samples, count, avg);                  \
  printf(msg);                                          \
  printf(" cycles = %" PRIu64 ", stdev=%.2f\n", avg, stdev);   \
  sync();						\
}while(0);

#endif
