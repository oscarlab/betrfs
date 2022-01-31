#!/bin/sh

DEV=/dev/sdb1

cd  ..
./mount_sfs.sh $DEV
cd -
dd if=/dev/zero of=../tmp/db/ftfs_data_2_1_19.tokudb bs=1G count=10 oflag=dsync
umount ../tmp

echo "Ext2 result"
mkfs.ext2 $DEV
mount $DEV ../tmp
dd if=/dev/zero of=../tmp/ftfs_data_2_1_19.tokudb bs=1G count=10 oflag=dsync
umount ../tmp

echo "Ext4 result"
mkfs.ext2 $DEV
mount $DEV ../tmp
dd if=/dev/zero of=../tmp/ftfs_data_2_1_19.tokudb bs=1G count=10 oflag=dsync
umount ../tmp
