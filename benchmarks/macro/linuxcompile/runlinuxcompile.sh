#!/bin/bash
set -x
FT_HOMEDIR=/home/ftfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh
#. $FT_HOMEDIR/benchmarks/.rootcheck
if [ -d $mntpnt/linux-3.11.10 ]; then
:
else
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi

cd $mntpnt/linux-3.11.10/
make clean
make mrproper
cp $FT_HOMEDIR/linux-3.11.10/qemu-utils/kvm-config $mntpnt/linux-3.11.10/.config
pushd .
cd $FT_HOMEDIR/benchmarks/; sudo ./clear-fs-caches.sh
popd
time make -j4 >/dev/null 2>&1
