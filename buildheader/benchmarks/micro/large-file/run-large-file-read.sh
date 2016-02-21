#!/bin/bash

. params.sh

read_exe="dd"
write_exe="./sequential_write"

if [ ! -e $write_exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ ! -e $mnt/$input ]; then
    echo "no input file... generating"
    $write_exe -o$mnt/$input -b$io_size -n$random_buffers -s$f_size
    echo "done generating input file."
fi

sudo ../../clear-fs-caches.sh

echo "beginning test..."
$read_exe if=$mnt/$input of=/dev/null bs=$io_size count=$(($f_size / $io_size)) 2>&1 | grep "10737" 
echo "done!"
