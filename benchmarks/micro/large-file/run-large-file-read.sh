#!/bin/bash
set -e
set -x

. params.sh
. ../../fs-info.sh

read_exe="dd"
write_exe="./sequential_write"


if [ ! -e $write_exe ] ; then
    echo "Stop! You must run 'make' first!"
    exit 1
fi

if [ ! -e $mnt/$input ]; then
    echo "no input file... generating"
    $write_exe -o$mnt/$input -b$io_size -n$random_buffers -s$f_size
    echo "done generating input file."
fi

####################################
# Provide the fstype as argument ! #
####################################
if [ "$#" -ne 1 ] && [ "$#" -ne 2 ] ; then
	echo "Need fstype as argument!"
	exit 1
fi

FS=$1
TIME=`date +"%d-%m-%Y-%H-%M-%S"`
FILE="$1-seq-read-${TIME}"

if [ "$#" -eq 2 ] ; then
	FILE=$2
fi

sudo -E ../../clear-fs-caches.sh

RESFILE=${resultdir}/${FILE}

echo "beginning test..."
$read_exe if=$mnt/$input of=/dev/null bs=$io_size count=$(($f_size / $io_size)) > ${RESFILE}.csv 2>&1

SIZE=`tail -n 1 ${RESFILE}.csv  | awk '{print $1}'`
SEC=`tail -n 1 ${RESFILE}.csv  | awk '{print $6}'`
THRU=`tail -n 1 ${RESFILE}.csv  | awk '{print $8}'`
SIZE_MB=`echo "scale=6; $SIZE/1000000" | bc -l`

echo "result.${FS}, read.seq, $SIZE_MB, $SEC, $THRU" >>  ${RESFILE}.csv
echo "done!"
