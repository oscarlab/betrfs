#/bin/bash

. ../../fs-info.sh

exe=bin/x86_64-linux-gnu/lat_fs

if [ ! -e $exe ]; then
    stat $exe
    echo "Stop! $exe does not exist. run make"
    exit 1
fi

fsdir=$mntpnt/lat_fs

if [ ! -e $fsdir ]; then
    mkdir -p $fsdir
fi

$exe $fsdir