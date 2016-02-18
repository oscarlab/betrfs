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
keyword=cpu_to_be64
else
keyword=$1
fi
time grep -r $keyword $mntpnt/linux-3.11.10>/dev/null 2>&1
