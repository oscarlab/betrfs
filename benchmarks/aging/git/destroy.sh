#!/bin/bash

# ./destroy.sh target
# target is the name of a test partition profile in config.sh
# generally this means "clean" or "aged"

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

profile=$1
. "$DIR/config.sh"

fs_type=`grep "[[:space:]]$mntpnt[[:space:]]" /proc/mounts | cut -d' ' -f3`

echo "destroying $profile partition of type $fs_type"

case "$fs_type" in
	ext4)
		set -x
		umount $mntpnt 
		;;

	xfs)
		set -x
		umount $mntpnt 
		;;

	btrfs)
		set -x
		umount $mntpnt 
		;;

	zfs)
		set -x
		zfs unmount $mntpnt 
		zpool destroy -f $datastore 
		;;

	f2fs)
		set -x
		umount $mntpnt
		;;

	ftfs)
		set -x
		umount $mntpnt 
		rmmod $module
		losetup -d /dev/loop0
		;;

	*)
		echo "no filesystem mounted at $mntpnt"
		;;
esac
