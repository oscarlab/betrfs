#!/bin/bash
set -x

. ../../fs-info.sh
support=$FT_HOMEDIR/benchmarks/support-files

if [ "$#" -eq 3 ] ; then
    if [ "$3" != "--force-hdd" ] && [ "$3" != "--force-ssd" ]; then
        echo "Invalid argument!"
        exit 1
    fi
else
    echo "Need three arguments: fstype, output and [--force-ssd|--force-hdd]!"
    exit 1
fi

if [ ! -e $support/$clone_repo ] ; then
    cd $support; git clone https://github.com/torvalds/linux.git; cd -
fi

if [ -e $mntpnt/$clone_repo ]; then
    echo "Stop! $mntpnt/$clone_repo already exists."
    echo "Please delete and run again."
    exit 17
fi

sudo -E ../../clear-fs-caches.sh $3

cd $mntpnt
clonetime=$(TIMEFORMAT="%R"; (time (git clone $support/$clone_repo 2> /dev/null)) 2>&1)
cd -

sudo -E ../../clear-fs-caches.sh $3

cd $mntpnt/$clone_repo
difftime=$(TIMEFORMAT="%R"; (time (git diff --patch v4.7 v4.14 > patch)) 2>&1)
cd -

echo "$1", clone, $clonetime >> $2
echo "$1", diff,  $difftime >> $2
