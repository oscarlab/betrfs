#!/bin/bash

. params.sh
. ../../fs-info.sh

exe="./sequential_write"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ -e $mnt/$input ] ; then
	echo "File Exists. This is an overwrite."
	rm $mnt/$input
fi

if [ "$#" -ne 1 ] && [ "$#" -ne 2 ] ; then
	echo "Need fstype file as argument!"
	exit 1
fi

FS=$1
TIME=`date +"%d-%m-%Y-%H-%M-%S"`
FILE="$1-seq-write-${TIME}"

if [ "$#" -eq 2 ] ; then
	FILE=$2
fi

echo "clearing cache"

echo "a" > $mnt/zzz
sudo -E ../../clear-fs-caches.sh

echo "beginning sequential write test..."

cmd="$exe -o$mnt/$input -b$io_size -n$random_buffers -s$f_size"

echo $cmd
(
    $cmd
) | tee -a ${resultdir}/${FILE}.csv


if [ $? != 0 ] ; then
    echo "got error $?"
    exit $?
fi

sed -i '$ s/^/result.'${FS}', /'  ${resultdir}/${FILE}.csv

echo "done!"
