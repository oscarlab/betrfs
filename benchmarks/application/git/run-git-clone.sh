#!/bin/bash

. ../../fs-info.sh
. ../../.ismounted
support=$HOME/$repo/benchmarks/support-files

if [ ! -e $support/$clone_repo ] ; then
    cd $support; git clone https://github.com/torvalds/linux.git; cd -
fi

if [ -e $mntpnt/$clone_repo ]; then
    echo "Stop! $mntpnt/$repo already exists."
    echo "Please delete and run again."
    exit 17
fi

sudo ../../clear-fs-caches.sh

cd $mntpnt; time git clone $support/$clone_repo
