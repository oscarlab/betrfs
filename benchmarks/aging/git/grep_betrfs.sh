#!/bin/bash
################################################################################
# grep_betrfs.sh
################################################################################
# performs aged grep tests on betrfs
#
# usage:
# ./grep_betrfs.sh path_to_aged aged_blk_device path_to_ftfs_module
#
# notes:
# 1. betrfs aged and unaged performance require separate tests due to lack of
# support for concurrent instances
# 2. betrfs does not report accurate file system size, so ext4 or another file
# system's size should be used as a proxy
################################################################################

AGED_PATH=$1
AGED_BLKDEV=$2
MODULE=$3

# remount aged and time a recursive grep
umount $AGED_PATH &>> log.txt
rmmod $MODULE &>> log.txt
losetup -d /dev/loop0
losetup /dev/loop0 dummy.dev
modprobe zlib
insmod $MODULE sb_dev=$AGED_BLKDEV sb_fstype=ext4 &>> log.txt
mount -t ftfs dummy.dev $AGED_PATH -o max=128

AGED="$(TIMEFORMAT='%3R'; time (grep -r "t26EdaovJD" $AGED_PATH) 2>&1)"

# return the aged time
echo "$AGED"



