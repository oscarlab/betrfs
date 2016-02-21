#!/bin/bash
set -x
FT_HOMEDIR=/home/ftfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh
if [ -d $mntpnt/linux-3.11.10 ]; then
:
else 
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi
(cd $FT_HOMEDIR/benchmarks/; sudo ./clear-fs-caches.sh)
if [ "$1" = "" ] 
then
copydir=copydir
else
copydir=$1
fi
#mkdir $mntpnt/$copydir>/dev/null 2>&1
#if [ $? -ne 0 ]; then
# echo "failed to create the $copydir, exiting..."
#else
ls $mntpnt
 time mv $mntpnt/linux-3.11.10 $mntpnt/$copydir>/dev/null 2>&1
#fi
