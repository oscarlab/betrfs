#!/bin/bash

. params.sh

exe="./random_write"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ ! -e $mnt/$input ] ; then
    echo "Stop! run run-large-file-write.sh first!"
    echo "This will generate the input file"
#    exit 1
fi

sudo ../../clear-fs-caches.sh

echo "beginning random write test..."
#sudo blktrace -d /dev/sda4 -b 4096 &
$exe  -f $mnt/$input -s$f_size
#sleep 10 
#sudo pkill -15 blktrace
echo "done!"
