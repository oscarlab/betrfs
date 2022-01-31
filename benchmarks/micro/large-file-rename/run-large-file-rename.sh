#!/bin/bash

# benchmark parameters
file_size=$((1024*1024*1024))
mnt="/mnt/benchmark"
input="some.large.file"
output="some.other.large.file"

if [ ! -e $mnt/$input ]; then
    echo "no large file... generating"
    head -c $file_size /dev/urandom > $mnt/$input
    echo "done generating large file."
fi

sudo ../../clear-fs-caches.sh

echo "beginning test..."
time mv $mnt/$input $mnt/$output
echo "done"
