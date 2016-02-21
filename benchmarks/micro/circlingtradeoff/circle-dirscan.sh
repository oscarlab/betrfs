#!/bin/bash

TIMEFORMAT="%R"

declare -A resultfind
declare -A resultgrep
cnt=0
for i in 2048 1024 512 256 128 64 32 16 8 4 2 1 0
#for i in 2048 
do
	FT_HOMEDIR=/home/ftfs/ft-index
	. $FT_HOMEDIR/benchmarks/fs-info.sh
	sudo $FT_HOMEDIR/benchmarks/cleanup-fs.sh
	sudo $FT_HOMEDIR/benchmarks/setup-ftfs-parameterized.sh $i 
#	sudo $FT_HOMEDIR/benchmarks/setup-ext4.sh 

#	dropbox=Dropbox
	linux=linux-3.11.10

#	if [ -d $mntpnt/linux-3.11.10 ]; then
#	:
#	else
#	. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
#	fi

	echo "Copying linux src"
	cp -r $FT_HOMEDIR/benchmarks/support-files/linux-3.11.10 $mntpnt
#	cp -r /home/ftfs/$dropbox $mntpnt
	echo "checpoint" > /proc/toku_checkpoint	
	echo "Circle Size: $i"
	
#	for k in $(seq 0 49)
#	do 
#		sudo $FT_HOMEDIR/benchmarks/clear-fs-caches.sh
#		echo "begining find"	
#		resultfind[$cnt, $k]=$( { time find $mntpnt/$linux -type f >/dev/null; } 2>&1 )
#		echo "done"
#		echo "Time(find): ${resultfind[$cnt, $k]}"
#	done
	
#	num=50	
#	total=$( IFS="+"; bc <<< "${resultfind[*]}" )
#	resultavgf[$cnt]=$(echo $total / $num | bc -l)
	
	for k in $(seq 0 49)
	do 
		sudo $FT_HOMEDIR/benchmarks/clear-fs-caches.sh
		echo "begining grep"
		resultgrep[$cnt, $k]=$( { time grep -r "cpu_to_be64" $mntpnt/$linux >/dev/null; } 2>&1 )
		echo "done"
		echo "Time(grep): ${resultgrep[$cnt, $k]}"
	done
	
#	total=$( IFS="+"; bc <<< "${resultgrep[*]}" )
#	resultavgf[$cnt]=$(echo $total / $num | bc -l)
	
	cnt=$((cnt+1))
done

echo "grep: ${resultgrep[@]}"
#echo "find: ${resultfind[@]}"
