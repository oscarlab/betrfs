/*
 * DEP 2/29/08: This measures the time required for various methods of
 * making a system call on a system
 */

#include <stdio.h>
#include <elf.h>
#include <asm/unistd.h>
#include <stdint.h>
#include <math.h>
#include <stdlib.h>
#include "syscall.h"
#include <unistd.h>

int main(int argc, char* argv[], char* envp[]){
  int a = 0;
  uint64_t start = 0, end = 0;
  uint64_t total = 0, count = 0;
  uint64_t samples[ITERATIONS];
  uint64_t avg = 0;
  double stdev = 0.0;

  exp(1, syscall_instr(__NR_getppid, a), 1, "syscall instr.");

  exp(1, int_80(__NR_getppid, a), 1, "int 80        ");

  exp(1, getppid(), 1, "libc cycles   ");

  return 0;
}

/* linux-c-mode */
