#!/bin/bash

TIMEFORMAT="%R"

declare -a result
declare -a avg
cnt=0

for i in 4 8 16 32 64 128 256 512 1024 2048 3072 4096 5120 6144
do
	. /home/ftfs/ft-index/benchmarks/fs-info.sh
	sudo /home/ftfs/ft-index/benchmarks/setup-ftfs.sh  2>&1 tmp

	# benchmark parameters
	file_size=$((1024*$i))
	mnt="/mnt/benchmark"
	input="some.large.file"
	output="some.other.large.file"

	for i in $(seq 0 4)
	do
	#if [ ! -e $mnt/$input ]; then
		echo "no large file... generating"
		head -c $file_size /dev/urandom > $mnt/$input$i
		echo "done generating large file."
	#fi
	done

	for i in $(seq 0 4)
	do
		sudo ../../clear-fs-caches.sh 2>&1 tmp
		echo "beginning test..."
		result[$i]=$( { time mv $mnt/$input$i $mnt/$output$i >/dev/null; } 2>&1 )
		echo "done"
		done

	num=5
	total=$( IFS="+"; bc <<< "${result[*]}" )
	echo $total
	avg[$cnt]=$(echo $total / $num | bc -l)

	echo "file size: $file_size   Avg Time: ${avg[$cnt]}"
	echo "Array: ${result[@]}"
	
	cnt=$((cnt+1))
	sudo /home/ftfs/ft-index/benchmarks/cleanup-fs.sh 2>&1 tmp
done


