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

blkdev=`echo $sb_dev | tr -d [:digit:]`
is_rotational=`lsblk -d $blkdev  -o name,rota  | tail -n 1 | awk '{print $2}'`

if [[ $fstype == "ext4" || $fstype == "btrfs" || $fstype == "xfs" || $fstype == "f2fs" ]]
then
    umount $mntpnt
    mount -t $fstype $sb_dev $mntpnt
elif [[ $fstype == "nilfs2" ]]
then
    nilfs-clean -q
    umount $mntpnt
    mount -t $fstype $sb_dev $mntpnt
elif [[ $fstype == "zfs" ]]
then
    zfs umount $mntpnt
    zfs mount datastore/files
elif [[ $fstype == "ftfs" ]]
then
    echo "removing $module and mounting/unmounting ftfs file system"
    umount $mntpnt
    rmmod $module
    if [[ $use_sfs == "true" ]]
    then
        rmmod simplefs
        insmod $FT_HOMEDIR/simplefs/simplefs.ko
        insmod $module
        mount -t ftfs -o max=$circle_size,sb_fstype=sfs,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
    else
        insmod $module
        mount -t ftfs -o max=$circle_size,sb_fstype=ext4,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
    fi
    echo "mounted: $fstype."
    exit 0
else
    echo "unknown fs type mounted: $fstype."
    exit 1
fi
