#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

. ../../fs-info.sh

support=$FT_HOMEDIR/benchmarks/support-files
src=linux-3.11.10.tar.xz
dst=linux-3.11.10.tar.xz

echo "starting $support"

if [ ! -e $mntpnt/$dst ]; then
    cp $support/$src $mntpnt/$dst
fi

if [ "$#" -eq 1 ] ; then
    if [ "$1" != "--force-hdd" ] && [ "$1" != "--force-ssd" ]; then
        echo "Invalid argument!"
        exit 1
    fi
fi

sudo -E ../../clear-fs-caches.sh $1

cd $mntpnt;
/usr/bin/time -p  tar -xf $dst
cd -

sudo -E ../../clear-fs-caches.sh $1

cd $mntpnt;
/usr/bin/time  -p tar -zcvf ./linux.tar.gz ./linux-3.11.10/ > /dev/null
cd -

