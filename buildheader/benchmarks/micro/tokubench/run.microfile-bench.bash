#!/bin/bash

if [ ! -e benchmark-fs-threaded ] ; then
    echo "make benchmark-fs-threaded first. cowardly not doing it here."
    return 1
fi

# benchmark parameters
thread_counts="1 4 8"
iosize="200"
operations="1"
dir_depth="128"
num_files="5000000"

mounts="/mnt/ext4 /mnt/xfs /mnt/btrfs /zfs"

for mnt in $mounts ; do
    echo "Running benchmarks on $mnt"
    for num_threads in $thread_counts ; do
        dir="microfile-bucket.$(date +%s)"
        fs=$(echo $mnt | tr / _)
        cmd="./benchmark-fs-threaded --files $num_files -d $dir_depth --iosize $iosize --operations $operations -u --threads $num_threads --dir $mnt/$dir"
        echo $cmd
        (
            $cmd 
        ) | tee -a $fs-$num_threads.results
        if [ $? != 0 ] ; then
            echo "got error $?"
            return $?
        fi
    done
done

