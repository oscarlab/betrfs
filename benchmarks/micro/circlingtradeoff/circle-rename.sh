#!/bin/bash

TIMEFORMAT="%R"

declare -a result
declare -a resultmv
cnt=0

FT_HOMEDIR=/home/ftfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh
sudo -E $FT_HOMEDIR/benchmarks/setup-ftfs.sh  10000

echo "Copying linux src"
cp -r $FT_HOMEDIR/benchmarks/support-files/linux-3.11.10 $mntpnt

dirs=($(find $mntpnt/linux-3.11.10 | shuf | head -100))

#echo "Array: ${dirs[@]}"

sudo -E $FT_HOMEDIR/benchmarks/cleanup-fs.sh

for i in 2048 1024 512 256 128 64 32 16 8 4 2 1
do
	FT_HOMEDIR=/home/ftfs/ft-index
	. $FT_HOMEDIR/benchmarks/fs-info.sh
	sudo -E $FT_HOMEDIR/benchmarks/setup-ftfs.sh  $i

#	if [ -d $mntpnt/linux-3.11.10 ]; then
#	:
#	else
#	. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
#	fi

	echo "Copying linux src"
	cp -r $FT_HOMEDIR/benchmarks/support-files/linux-3.11.10 $mntpnt

	sudo -E $FT_HOMEDIR/benchmarks/clear-fs-caches.sh $i

	echo "Circle Size :$i"

	for j in $(seq 0 99)
	do
		echo "Moving ${dirs[$j]}"
		result[$j]=$( { time mv ${dirs[$j]} $mntpnt/ >/dev/null; } 2>&1 )
		echo "Time: ${result[$j]}"
	done

	total=$( IFS="+"; bc <<< "${result[*]}" )
	echo "Total time for rename: $total"

	resultmv[$cnt]=$total
	cnt=$((cnt+1))

	sudo -E $FT_HOMEDIR/benchmarks/cleanup-fs.sh
done

echo "mv: ${resultmv[@]}"
