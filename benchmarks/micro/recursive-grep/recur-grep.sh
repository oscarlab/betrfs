#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e
#echo "**************************************************"

. ../../fs-info.sh

#echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"

if [ -d $mntpnt/linux-3.11.10 ]; then
:
else
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi

#echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

if [ "$#" -eq 1 ] ; then
    if [ "$1" != "--force-hdd" ] && [ "$1" != "--force-ssd" ]; then
        echo "Invalid argument!"
        exit 1
    fi
fi

sudo -E ../../clear-fs-caches.sh $1

keyword=cpu_to_be64

/usr/bin/time -p grep -r $keyword $mntpnt/linux-3.11.10 > /dev/null
