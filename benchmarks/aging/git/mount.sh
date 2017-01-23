#!/bin/bash

# ./remount.sh target
# target is the name of a test partition profile in config.sh
# generally this means "clean" or "aged"

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

profile=$1
. "$DIR/config.sh"

echo "mounting $profile partition of type $fs_type"

case "$fs_type" in
	ext4)
		set -x
		mount -t ext4 $partition $mntpnt 
		;;

	xfs)
		set -x
		mount -t xfs -o nouuid $partition $mntpnt 
		;;

	btrfs)
		set -x
		mount -t btrfs $partition $mntpnt 
		;;

	zfs)
		set -x
		zpool import $datastore 
		zfs mount -a 
		;;

	f2fs)
		set -x
		mount -t f2fs $partition $mntpnt
		;;

	ftfs)
		set -x
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
