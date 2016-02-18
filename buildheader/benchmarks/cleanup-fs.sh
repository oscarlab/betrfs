#!/bin/bash
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.rootcheck"

fstype=`grep "[[:space:]]$mntpnt[[:space:]]" /proc/mounts | cut -d' ' -f3`

echo "unmounting $fstype"

if [[ $fstype == "ext4" || $fstype == "btrfs" || $fstype == "xfs" ]]
then
    umount $mntpnt
    exit 0
elif [[ $fstype == "ftfs" ]]
then
    umount $mntpnt
    rmmod $module
    losetup -d /dev/loop0
    exit 0
elif [[ $fstype == "zfs" ]]
then
    umount $mntpnt
    zpool destroy -f datastore
    #zpool destroy -f datastore > /dev/null
    #zfs destroy -rf datastore > /dev/null
    exit 0
else
    echo "unknown fs type mounted: $fstype."
    exit 1
fi
