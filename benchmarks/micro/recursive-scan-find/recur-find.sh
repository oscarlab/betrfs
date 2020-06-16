#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

. ../../fs-info.sh

set -x
. $FT_HOMEDIR/benchmarks/fs-info.sh
if [ -d $mntpnt/linux-3.11.10 ]; then
:
else
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi
sudo -E ../../clear-fs-caches.sh

if [ "$1" = "" ]
then
filename='wait.c'
else
filename=$1
fi


/usr/bin/time -p find $mntpnt/linux-3.11.10 -name $filename>/dev/null
