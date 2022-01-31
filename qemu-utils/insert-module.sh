#!/bin/bash
set -x
sudo modprobe zlib

if [ "$1" == "nb" ]
then
    sudo insmod /home/betrfs/ft-index/ftfs_fs/ftfs.ko
elif [ "$1" == "sb" ]
then
    sudo insmod /home/betrfs/ft-index/ftfs/ftfs.ko \
	sb_dev=/dev/sdb7 \
	sb_fstype=ext4 \
3	ftfs_test_filename=test.file
elif [ "$1" == "fs" ]
then
    sudo insmod /home/betrfs/ft-index/filesystem/ftfs.ko
else
    echo "($1) Please specify nb, sb, or fs"
fi
