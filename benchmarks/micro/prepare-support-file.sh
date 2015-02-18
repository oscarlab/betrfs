#!/bin/bash
set -x
FT_HOMEDIR=/home/ftfs/ft-index/
. $FT_HOMEDIR/benchmarks/fs-info.sh
pushd $PWD
cd $FT_HOMEDIR/benchmarks/support-files
if [ -d linux-3.11.10 ] 
then :
else
    tar -xf linux-3.11.10.tar.xz
fi

if [ -d $mntpnt/linux-3.11.10 ]
then :
else
    cp -r linux-3.11.10 $mntpnt
fi 
popd
