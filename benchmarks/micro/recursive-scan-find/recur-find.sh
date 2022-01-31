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

if [ "$#" -eq 1 ] ; then
    if [ "$1" != "--force-hdd" ] && [ "$1" != "--force-ssd" ]; then
        echo "Invalid argument!"
        exit 1
    fi
fi

sudo -E ../../clear-fs-caches.sh $1

filename='wait.c'

/usr/bin/time -p find $mntpnt/linux-3.11.10 -name $filename>/dev/null
