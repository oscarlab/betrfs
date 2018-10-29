#!/bin/bash
################################################################################
# grep_ext4.sh
################################################################################
# performs aged and unaged grep tests on ext4
#
# usage:
# ./grep_ext4.sh path_to_aged aged_blk_device path_to_unaged unaged_blk_device
################################################################################

AGED_PATH=$1
AGED_BLKDEV=$2
UNAGED_PATH=$3
UNAGED_BLKDEV=$4

# remount aged and time a recursive grep
umount $AGED_PATH &>> log.txt
mount $AGED_BLKDEV $AGED_PATH &>> log.txt
AGED="$(TIMEFORMAT='%3R'; time (grep -r "t26EdaovJD" $AGED_PATH) 2>&1)"
SIZE="$(du -s $AGED_PATH | awk '{print $1}')"

# create a new ext4 filesystem, mount it, time a recursive grep and dismount it
mkfs.ext4 -f $UNAGED_BLKDEV &>> log.txt
mount $UNAGED_BLKDEV $UNAGED_PATH &>> log.txt
cp -a $AGED_PATH/* $UNAGED_PATH
umount $UNAGED_PATH &>> log.txt
mount $UNAGED_BLKDEV $UNAGED_PATH
UNAGED="$(TIMEFORMAT='%3R'; time (grep -r "t26EdaovJD" $UNAGED_PATH) 2>&1)"
umount $UNAGED_PATH &>> log.txt

# return the size and times
echo "$SIZE $AGED $UNAGED"
