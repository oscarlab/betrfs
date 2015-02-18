#!/bin/bash

## This is not part of the bonnie++ suite, but added for convenience.

TESTDIR=/mnt/benchmark
FILE_SIZE=8G # heuristic: twice RAM

mkdir -p $TESTDIR

./bonnie++ -d  $TESTDIR -s $FILE_SIZE -f -b


#bonnie++ [-d scratch-dir] [-c concurrency] [-s size(MiB)[:chunk-size(b)]]
#      [-n number-to-stat[:max-size[:min-size][:num-directories[:chunk-size]]]]
#      [-m machine-name] [-r ram-size-in-MiB]
#      [-x number-of-tests] [-u uid-to-use:gid-to-use] [-g gid-to-use]
#      [-q] [-f] [-b] [-p processes | -y] [-z seed | -Z random-file]
#      [-D]

### details of options
#  -d = directory for testing
#  -s = size of files for IO test. To skip this test use zero. To have realistic test, use size that is double of RAM. 
#  -n = Number of files for file creation test (measured in multiples of 1024 files)
#  -m = hostname for display purpose
#  -r = your RAM, you can skip this since you don't need Bonnie to determine RAM, you pay attention on -s to be 2 x RAM
#  -x = number of tests
#  -u/g = run Bonnie as user/group. Specify only user and his primary group will be chosen. Recommended not to run as root. 
#  -q = quiet mode so you may miss some messages
#  -f = fast mode (skip per-character IO test = write/read single character, use only block IO test)
#  -b = no write buffering
#  -p = number of processes used by semaphores