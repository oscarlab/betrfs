#!/bin/bash

exe="./smallfb"


if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi


# benchmark parameters
io_size=4096
f_size=$((1024*1024*1024))
mnt="/mnt/benchmark"

#Cleanup and setup ftfs
sudo ../../cleanup-fs.sh
sudo ../../setup-${1}.sh

#Copy the binary to the mountpoint for testing
cp smallfb $mnt/

#Clear the Caches
sudo ../../clear-fs-caches.sh

echo "Executing LFS small benchmark"
cd $mnt/
./smallfb 1000 4000 4 
echo "done."

# Post test-run Cleanup
cd ~/ft-index/benchmarks/
sudo ./cleanup-fs.sh
