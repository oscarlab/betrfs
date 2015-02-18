#!/bin/bash

. params.sh

exe="./sequential_write"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ ! -e $mnt/$input ] ; then
    echo "Warning. The first run will use the right-edge optimization."
    echo "********Please discard the first run********"
fi

if [ -e $mnt/$input ] ; then
    rm $mnt/$input
fi

sudo ../../clear-fs-caches.sh

echo "beginning sequential write test..."
$exe -o$mnt/$input -b$io_size -n$random_buffers -s$f_size
echo "done!"
