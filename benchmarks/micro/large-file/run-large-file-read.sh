#!/bin/bash
set -e
set -x

## for introducing resultdir
. ../../fs-info.sh
. params.sh

read_exe="dd"
write_exe="./sequential_write"

env_check() {
	if [ ! -e $exe ] ; then
		echo "Stop! You must run 'make' first!"
		exit 1
	fi
	if [ ! -e $mnt/$input ] ; then
		echo "File does not exist. Please create a large file first!"
		exit 1
	fi
}

env_check
TIME=`date +"%d-%m-%Y-%H-%M-%S"`
OUTPUT_FILE="seq-read10g-${TIME}"

while [[ $# -gt 0 ]]
do
	key="$1"
	case $key in
		-f|--filesystem)
		FS="$2"
		shift # past argument
		shift # past value
		;;
		-s|--filesize)
		f_size="$2"
		NUM_GB=$(($f_size/1024/1024/1024))
		OUTPUT_FILE="seq-read${NUM_GB}g-${TIME}"
		shift # past argument
		shift # past value
		;;
		--force-hdd|--force-ssd)
	        rotational_arg=$key
		shift
		;;
		*)    # unknown option
		echo "Unknown parameter: $1"
		exit 0
		;;
	esac
done

### Update output file name
if [ ! -z "FS" ]
then
	OUTPUT_FILE="${FS}-${OUTPUT_FILE}"
fi

echo "FILE SYSTEM    = ${FS}"
echo "TEST FILE SIZE = ${f_size}"
echo "OUTPUT FILE    = ${OUTPUT_FILE}"

sudo -E ../../clear-fs-caches.sh $rotational_arg

RESFILE=${resultdir}/${OUTPUT_FILE}

echo "beginning test..."
$read_exe if=$mnt/$input of=/dev/null bs=$io_size count=$(($f_size / $io_size)) > ${RESFILE}.csv 2>&1

SIZE=`tail -n 1 ${RESFILE}.csv  | awk '{print $1}'`

#### NOTE: 14.04 SEC is positioned $6 and on 16.04 it is positioned on $8
SEC=`tail -n 1 ${RESFILE}.csv  | awk '{print $8}'`
THRU=`tail -n 1 ${RESFILE}.csv  | awk '{print $10}'`
SIZE_MB=`echo "scale=6; $SIZE/1000000" | bc -l`

echo "result.${FS}, read.seq, $SIZE_MB, $SEC, $THRU" >>  ${RESFILE}.csv
echo "done!"
