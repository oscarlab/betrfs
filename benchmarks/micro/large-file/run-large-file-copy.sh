#!/bin/bash

exe="../lmbench-3/bin/x86_64-linux-gnu/lmdd"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' in ../lmbench-3 first!"
    exit 1
fi


# benchmark parameters
io_size=4096
copy_size=$((4*1024*1024*1024))
mnt="/mnt/benchmark"
output="large22222.file"

$exe if=internal of=$mnt/$output bs=$io_size count=$(($copy_size / $io_size)) opat=1 fsync=1
