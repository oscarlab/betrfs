#!/bin/bash

# ./remount.sh target
# target is the name of a test partition profile in config.sh
# generally this means "clean" or "aged"

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

profile=$1
. "$DIR/config.sh"

echo "remounting $profile partition of type $fs_type"

case "$fs_type" in
	ext4)
		set -x
		umount $mntpnt 
		mount -t ext4 $partition $mntpnt 
		;;

	xfs)
		set -x
		umount $mntpnt 
		mount -t xfs $partition $mntpnt 
		;;

	btrfs)
		set -x
		umount $mntpnt 
		mount -t btrfs $partition $mntpnt 
		;;

	zfs)
		set -x
		zfs unmount $mntpnt 
		zpool export $datastore 
		zpool import $datastore 
		zfs mount -a 
		;;

	f2fs)
		set -x
		umount $mntpnt
		mount -t f2fs $partition $mntpnt
		;;

	ftfs)
		set -x
		umount $mntpnt 
		rmmod $module
		losetup -d /dev/loop0
		losetup $dummy_dev $DIR/$dummy_file
		modprobe zlib
		insmod $module sb_dev=$partition sb_fstype=ext4
		mount -t ftfs $dummy_dev $mntpnt -o max=$circle_size
		;;

	*)
		echo "Unknown filesystem type $fs_type"
		exit 1
		;;
esac
