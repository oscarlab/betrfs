#!/bin/bash


. ../../fs-info.sh

outfile=$FT_HOMEDIR/benchmarks/results/git.csv
echo "fs, op, time.s" > $outfile

./run-git-clone-diff.sh ftfs $outfile $1
