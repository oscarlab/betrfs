#!/bin/bash
set -x
FT_HOMEDIR=/home/ftfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh
#. $FT_HOMEDIR/benchmarks/.rootcheck
if [ -d $mntpnt/linux-3.11.10 ]; then
:
else 
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi
(cd $FT_HOMEDIR/benchmarks/; sudo ./clear-fs-caches.sh)
if [ "$1" = "" ] 
then
filename='wait.c'
else
filename=$1
fi
time find $mntpnt/linux-3.11.10 -name $filename>/dev/null 2>&1
