#!/bin/bash

if [ ! -e benchmark-fs-threaded ] ; then
    echo "run \'make benchmark-fs-threaded\' first."
    exit 1
fi


# benchmark parameters
#thread_counts="1"
#thread_counts="2"
thread_counts="4"
iosize="200"
operations="1"
#max_fanout="1000000"
max_fanout="128"
#num_files="500000"
num_files="3000000"
#num_files="1000000"

mnt="/mnt/benchmark"

outfile="fs-threaded"

echo "Running benchmarks on $mnt"
for num_threads in $thread_counts ; do
    sudo ../../clear-fs-caches.sh
    dir="microfile-bucket.$(date +%s)"
    cmd="./benchmark-fs-threaded --files $num_files -d $max_fanout --iosize $iosize --operations $operations --pwrite --serial --threads $num_threads --dir $mnt/$dir"
    echo $cmd
    (
        $cmd 
    ) | tee -a $outfile-$num_threads.results
    if [ $? != 0 ] ; then
        echo "got error $?"
    fi
done


