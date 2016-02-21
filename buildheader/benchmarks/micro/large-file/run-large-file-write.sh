#!/bin/bash

. params.sh

exe="./sequential_write"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ -e $mnt/$input ] ; then
	echo "File Exists. This is an overwrite."
	rm $mnt/$input
fi

echo "a" > $mnt/zzz
sudo ../../clear-fs-caches.sh
#iostat -p
echo "beginning sequential write test..." 
$exe -o$mnt/$input -b$io_size -n$random_buffers -s$f_size
echo "done!"
#iostat -p
