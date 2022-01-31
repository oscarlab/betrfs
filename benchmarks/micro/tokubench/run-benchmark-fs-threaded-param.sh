#!/bin/bash

. ../../fs-info.sh

#####################################
# Simply parameterize number_files  # 
# and num_threads                   #
#####################################


if [ ! -e benchmark-fs-threaded ] ; then
    echo "run \'make benchmark-fs-threaded\' first."
    exit 1
fi

#################################### 
# Required input parameters        # 
####################################

if [ "$#" -ne 3 ] ; then
    echo "Need and only need thread_counts and num_files and testtype"
    exit 1
fi

thread_counts=$1
num_files=$2
testtype=$3

#################################### 
# Hard codeded parameters          # 
####################################

iosize="200"
operations="1"
max_fanout="128"
mnt="/mnt/benchmark"
outfile="${testtype}-threaded"

#################################### 
# Run the benchmark                # 
####################################
TIME=`date +"%F-%H-%M-%S"`

echo "Running benchmarks on $mnt"
for num_threads in $thread_counts ; do
    sudo ../../clear-fs-caches.sh
    dir="microfile-bucket.$(date +%s)"
    cmd="./benchmark-fs-threaded --files $num_files -d $max_fanout --iosize $iosize --operations $operations --pwrite --serial --threads $num_threads --dir $mnt/$dir"
    echo $cmd
    (
        $cmd 
    ) | tee -a  $resultdir/$outfile-$num_threads-${num_files}-${TIME}.csv

    sed -i '$ s/^/result.'${testtype}', /' $resultdir/$outfile-$num_threads-${num_files}-${TIME}.csv

    if [ $? != 0 ] ; then
        echo "got error $?"
    fi
done

