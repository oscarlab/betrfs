#!/bin/bash

## for variable resultdir
. ../../fs-info.sh
. params.sh

exe="./sequential_write"

env_check() {
	if [ ! -e $exe ] ; then
		echo "Stop! You must run 'make' first!"
		exit 1
	fi
	if [ -e $mnt/$input ] ; then
		echo "File Exists. This is an overwrite."
		rm $mnt/$input
		exit 1
	fi
}

env_check
TIME=`date +"%d-%m-%Y-%H-%M-%S"`
## Default output file name
OUTPUT_FILE="seq-write10g-${TIME}"
FS=""
NUM_GB=""

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
		OUTPUT_FILE="seq-write${NUM_GB}g-${TIME}"
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

echo "Clearing cache..."

echo "a" > $mnt/zzz
sudo -E ../../clear-fs-caches.sh $rotational_arg

echo "Beginning sequential write test..."

cmd="$exe -o$mnt/$input -b$io_size -n$random_buffers -s$f_size"

echo $cmd
(
    $cmd
) | tee -a ${resultdir}/${OUTPUT_FILE}.csv


if [ $? != 0 ] ; then
    echo "got error $?"
    exit $?
fi

sed -i '$ s/^/result.'${FS}', /'  ${resultdir}/${OUTPUT_FILE}.csv

echo "done!"
