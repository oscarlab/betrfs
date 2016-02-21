#!/bin/bash
set -x
FT_HOMEDIR=/home/ftfs/ft-index/
. $FT_HOMEDIR/benchmarks/fs-info.sh || exit 1
pushd $PWD || exit 1
cd $FT_HOMEDIR/benchmarks/support-files || exit 1
if [ -d linux-3.11.10 ] 
then :
else
    tar -xf linux-3.11.10.tar.xz || exit 1
fi

if [ -d $mntpnt/linux-3.11.10 ]
then :
else
    cp -r linux-3.11.10 $mntpnt || exit 1
fi 
popd || exit 1
exit 0
