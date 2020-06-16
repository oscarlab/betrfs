#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

. ../../fs-info.sh

if [ -d $mntpnt/linux-3.11.10 ]; then
:
else
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi

sudo -E ../../clear-fs-caches.sh

if [ "$1" = "" ]
then
keyword=cpu_to_be64
else
keyword=$1
fi

/usr/bin/time -p grep -r $keyword $mntpnt/linux-3.11.10 > /dev/null
