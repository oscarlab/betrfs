#!/bin/bash

for fs in btrfs ext4 ftfs nilfs2 xfs zfs
do
	for i in 12 13 14 15 16 17 18 19 20 21 22 
	do
		. /home/betrfs/ft-index/benchmarks/fs-info.sh
		/home/betrfs/ft-index/benchmarks/cleanup-fs.sh
		/home/betrfs/ft-index/benchmarks/setup-$fs.sh
		./file-rename $fs $i "rename-by-size.csv"
	done
done
