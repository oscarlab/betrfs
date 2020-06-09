#!/bin/bash


. ../../fs-info.sh

outfile=$FT_HOMEDIR/benchmarks/results/git.csv
echo "fs, op, time.s" > $outfile

#for fs in nilfs2 xfs zfs ftfs
#for i in {0..0}
#do
#		sudo -E ../../setup-ftfs.sh
		./run-git-clone-diff.sh ftfs $outfile
#		sudo ../../cleanup-fs.sh
#done
