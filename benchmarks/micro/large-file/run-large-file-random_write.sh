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
    exit 1
fi

sudo ../../clear-fs-caches.sh

echo "beginning random write test..."
$exe  -f $mnt/$input -s $f_size
echo "done!"
