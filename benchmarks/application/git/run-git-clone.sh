#!/bin/bash
set -x

. ../../fs-info.sh

support=${FT_HOMEDIR}/benchmarks/support-files

##################################
#### Clone the repo if not exist #
##################################
if [ ! -e $support/$clone_repo ] ; then
    cd $support; git clone https://github.com/torvalds/linux.git; cd -
fi

if [ -e $mntpnt/$clone_repo ]; then
    echo "Stop! $mntpnt/$repo already exists."
    echo "Please delete and run again."
    exit 17
fi

sudo ../../clear-fs-caches.sh

cd $mntpnt;
/usr/bin/time -p git clone $support/$clone_repo
cd -

