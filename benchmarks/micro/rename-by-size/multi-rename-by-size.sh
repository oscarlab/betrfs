#!/bin/bash

#for fs in btrfs ext4 nilfs2 xfs zfs ftfs 
for fs in ftfs
do
	#for i in 12 13 14 15 16 17 18 19 20 21 22 23 24 #25 26
	for i in  22 23 24 #25 26
	do
		. /home/betrfs/ft-index/benchmarks/fs-info.sh
		/home/betrfs/ft-index/benchmarks/cleanup-fs.sh
		/home/betrfs/ft-index/benchmarks/setup-$fs.sh
		./multifile-rename $fs $i "rename-by-size.csv"
	done
done
