#!/bin/bash

. params.sh
. ../../fs-info.sh

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

if [ "$#" -ne 1 ] && [ "$#" -ne 2 ] ; then
	echo "Need fstype as argument!"
	exit 1
fi

if [ "$#" -eq 2 ] ; then
    if [ "$2" != "--force-hdd" ] && [ "$2" != "--force-ssd" ]; then
        echo "Invalid argument!"
        exit 1
    fi
fi

TIME=`date +"%d-%m-%Y-%H-%M-%S"`
FILE=$1-rand-write-4-${TIME}
FS=$1

sudo -E ../../clear-fs-caches.sh $2

echo "beginning random write test..."
TIME=`date +"%d-%m-%Y-%H-%M-%S"`
cmd="$exe  -f $mnt/$input -s $f_size"
echo $cmd
(
    $cmd
) | tee -a  ${resultdir}/${FILE}.csv

if [ $? != 0 ] ; then
    echo "got error $?"
    exit $?
fi

sed -i '$ s/^/result.'${FS}', /'  $resultdir/${FILE}.csv

echo "done!"
