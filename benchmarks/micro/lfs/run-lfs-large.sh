#!/bin/bash

set -x

exe="./largefb"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi


# benchmark parameters
io_size=4096
f_size=$((1024*1024*1024))
mnt="/mnt/benchmark"


sudo ../../cleanup-fs.sh
sudo ../../setup-${1}.sh

cp largefb $mnt/
#sudo chmod a+rwx $mnt/largefb
sudo ../../clear-fs-caches.sh

echo "Executing LFS large benchmark"
cd $mnt/

sudo ./largefb 256
echo "done."
cd ~/ft-index/benchmarks/
sudo ./cleanup-fs.sh
