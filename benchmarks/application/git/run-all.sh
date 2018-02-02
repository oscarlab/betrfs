#!/bin/bash

rm git.csv
echo "fs, op, time.s" > git.csv

for fs in nilfs2 xfs zfs ftfs
do
	for i in {0..4}
	do
		../../setup-"$fs".sh
		./run-git-clone-diff.sh $fs git.csv
		../../cleanup-fs.sh
	done
done
