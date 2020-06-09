#!/bin/bash

. ../../fs-info.sh
. ../../.ismounted

support=${FT_HOMEDIR}/benchmarks/support-files

if [ ! -e $mntpnt/$clone_repo ]; then
    if [ ! -e $support/$clone_repo ] ; then
	cd $support; git clone https://github.com/torvalds/linux.git; cd -
    fi
    cd $mntpnt; git clone $support/$clone_repo; cd -
fi

sudo ../../clear-fs-caches.sh

cd $mntpnt/$clone_repo
/usr/bin/time -p git diff --patch v4.7 v4.14 > patch
cd -
