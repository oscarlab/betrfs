#!/bin/bash


for i in 12 13 14 15 16 17 18 19 20 21 22 
do
	. /home/ftfs/ft-index/benchmarks/fs-info.sh
	sudo /home/ftfs/ft-index/benchmarks/cleanup-fs.sh
	sudo /home/ftfs/ft-index/benchmarks/setup-ftfs-parameterized.sh 100000
	./file-rename "BetrFS_v2_100000" $i "result-betrfs-100000"
done

for i in 12 13 14 15 16 17 18 19 20 21 22 
do
	. /home/ftfs/ft-index/benchmarks/fs-info.sh
	sudo /home/ftfs/ft-index/benchmarks/cleanup-fs.sh
	sudo /home/ftfs/ft-index/benchmarks/setup-ftfs-parameterized.sh 0
	./file-rename "BetrFS_v2_0" $i "result-fsync-betrfs-0"
done



