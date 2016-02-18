#!/bin/bash

#gcc -g ../large-file/sequential_write.c -o ../large-file/sequential_write -lrt


exe="./sequential_write"

if [  -z "$1" ] ; then
    echo "Usage: $0 <fs_type>"
    exit 1
fi
if [ ! -e $exe ] ; then
    #echo "Stop! You must run 'make' in ../lmbench-3 first!"
    echo "Stop! You must run 'make' first!"
    exit 1
fi
#sudo ../../cleanup-fs.sh
#sudo rm -rf $mnt
sudo ../../setup-$1.sh


f_size=$((1024*1024*1024))
mnt="/mnt/benchmark"
write_size=40960
random_buffers=5 
i=0
while [  $i -lt 64 ]; do
	sudo $exe -o$mnt/$i -b$write_size -n$random_buffers -s$f_size
	let i=i+1
	done

sudo ../../cleanup-fs.sh
sudo mount -t ext4 /dev/sda6 /mnt
sudo df  /mnt/
sudo ../../setup-ftfs.sh

let i=0
while [ $i -lt 32 ]; do
	sudo rm $mnt/$i
	let i=i+1
	done

sudo ../../cleanup-fs.sh
sudo mount -t ext4 /dev/sda6 /mnt
sudo df  /mnt/
