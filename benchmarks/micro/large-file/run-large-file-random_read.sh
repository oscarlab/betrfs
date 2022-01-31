#!/bin/bash

. params.sh
. ../../fs-info.sh

exe="./random_read"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ ! -e $mnt/$input ] ; then
    echo "Stop! run run-large-file-write.sh first!"
    echo "This will generate the input file"
    exit 1
fi

if [ "$#" -ne 2 ] ; then
	echo "Need fstype and iosize as argument!"
	exit 1
fi

FS=$1
IOSIZE=$2

TIME=`date +"%d-%m-%Y-%H-%M-%S"`
FILE=$FS-rand-read-${IOSIZE}-${TIME}

sudo ../../clear-fs-caches.sh
echo 3 > /proc/sys/vm/drop_caches

echo "beginning random read test..."
cmd="$exe  -f $mnt/$input -s $f_size -i $IOSIZE"
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
