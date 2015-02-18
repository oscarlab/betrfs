#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.rootcheck"

sync
echo 3 > /proc/sys/vm/drop_caches # 1 frees pagecache
                                  # 2 frees dentries and inodes
                                  # 3 all
sync

echo "caches freed via /proc/sys/vm/drop_caches"

fstype=`grep "[[:space:]]$mntpnt[[:space:]]" /proc/mounts | cut -d' ' -f3`
echo $fstype

if [[ $fstype == "ext4" || $fstype == "btrfs" || $fstype == "xfs" ]]
then
    umount $mntpnt
    mount -t $fstype $sb_dev $mntpnt
elif [[ $fstype == "zfs" ]]
then
    echo "what do we do with zfs?"
    exit 0
elif [[ $fstype == "ftfs" ]]
then
    echo "removing $module and mounting/unmounting ftfs file system"
    umount $mntpnt
    rmmod $module
    insmod $module sb_dev=$sb_dev sb_fstype=ext4
    mount -t ftfs $dummy_dev $mntpnt
    echo "mounted: $fstype."
    exit 0
else
    echo "unknown fs type mounted: $fstype."
    exit 1
fi
