#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

. ../../fs-info.sh

set -x
. $FT_HOMEDIR/benchmarks/fs-info.sh
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh --big

if [ "$#" -eq 1 ] ; then
    if [ "$1" != "--force-hdd" ] && [ "$1" != "--force-ssd" ]; then
        echo "Invalid argument!"
        exit 1
    fi
fi

sudo -E ../../clear-fs-caches.sh $1

/usr/bin/time -p rm -fr $mntpnt/big-linux > /dev/null
