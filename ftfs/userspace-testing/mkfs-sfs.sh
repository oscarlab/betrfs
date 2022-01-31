#!/bin/sh

CURR_DIR=`pwd`
SFS_DIR=${CURR_DIR}/../../simplefs
sb_dev=$1

# prepare file system 
cd ${SFS_DIR}
./setup-sfs.sh ${sb_dev} ${SFS_DIR}/tmp
cd -
lsmod | grep simplefs
umount ${SFS_DIR}/tmp
modprobe zlib
fdisk -l ${sb_dev}

