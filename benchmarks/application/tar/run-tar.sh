#!/bin/bash

. ../../fs-info.sh
. ../../.ismounted

support=$HOME/$repo/benchmarks/support-files
src=linux-3.11.10.tar.xz
dst=linux-3.11.10.tar.xz

if [ ! -e $mntpnt/$dst ]; then
    cp $support/$src $mntpnt/$dst
fi

sudo ../../clear-fs-caches.sh

cd $mntpnt; time tar -xf $dst

cd -

sudo ../../clear-fs-caches.sh

cd $mntpnt; time tar -zcvf ./linux.tar.gz ./linux-3.11.10/ > /dev/null
