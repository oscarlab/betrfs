#!/bin/bash

sudo /home/ftfs/ft-index/benchmarks/setup-ext4.sh

. /home/ftfs/ft-index/benchmarks/fs-info.sh

cd /mnt/benchmark
head -c 1000000000 /dev/urandom > some_file
#sudo umount -l /mnt/benchmark

sudo ../../clear-fs-caches.sh

#sudo mount -t ext4 $sb_dev $mntpnt

time mv /mnt/benchmark/some_file /mnt/benchmark/some_other_file
