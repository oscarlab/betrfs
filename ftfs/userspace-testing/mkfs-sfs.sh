#!/bin/sh

CURR_DIR=`pwd`
sb_dev=$1
SFS_DIR=$2
ssd_opt=$3

# prepare file system
cd ${SFS_DIR}
./setup-sfs.sh ${sb_dev} ${SFS_DIR}/temp ${ssd_opt}
result=$?
if [ $result -ne 0 ]; then
       echo "setup-sfs.sh failed: $result"
       exit $result
fi
cd -
lsmod | grep simplefs
umount ${SFS_DIR}/temp
fdisk -l ${sb_dev}
