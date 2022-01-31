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

if [ $# == "0" ]; then
    echo "Warning: No argument provided -- is_rotational will be inferred from underlying disk type."
    echo "If this doesn't match the format (hdd / ssd) declared at mount time, there will be bugs, and they will be strange."
    echo "(Magic string for Jenkins matching: rMv9WreOmX.)"
    is_rotational=`lsblk -d $sb_dev  -o name,rota  | tail -n 1 | awk '{print $2}'`
elif [ $# == 1 ]; then
    if [ $1 == "--force-ssd" ]; then
        is_rotational=0
    elif [ $1 == "--force-hdd" ]; then
        is_rotational=1
    else
        echo "Invalid argument: first argument should be (--force-hdd|--force-ssd)"
        exit 1
    fi
else
    echo "Too many arguments! 0 or 1 required."
    exit 1
fi

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
        insmod $FT_HOMEDIR/simplefs/simplefs.ko sfs_is_rotational=${is_rotational}
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

echo 3 > /proc/sys/vm/drop_caches
