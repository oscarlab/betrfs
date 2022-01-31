#!/bin/bash

set -eu

SB_DEV=/dev/hdb
DUMMY_FILE=dummy.dev
DUMMY_DEV=/dev/loop0
MNTPNT=mnt

# parse command line arguements. Only support one commandline argument right now, so this is kinda
# overkill, but this scales.
for i in "$@"; do
	case $i in
		--sfs-is-rotational=*)
			if [ -z ${var+SFS_IS_ROTATIONAL} ]; then
				SFS_IS_ROTATIONAL="${i#*=}"
			else
				echo 'Cannot set SFS_IS_ROTATIONAL twice.'
				exit 1
			fi
			shift
			;;

		*)
			echo \'$i\' is not valid argument.
			exit 1
			;;
	esac
done

# set default values
if [ -z ${var+SFS_IS_ROTATIONAL} ]; then
	SFS_IS_ROTATIONAL=$(lsblk -d $SB_DEV -o name,rota | tail -n 1 | awk '{print $2}')
fi

set -eux

# insert kernel modules
cd betrfs-private/simplefs
if ! grep -Eq ^simplefs /proc/modules; then
	sudo insmod simplefs.ko sfs_is_rotational=$SFS_IS_ROTATIONAL
fi
sudo ./mkfs-sfs $SB_DEV
cd ../filesystem
if ! grep -Eq ^ftfs /proc/modules; then
	sudo insmod ftfs.ko
fi

# mount betrfs
cd
touch $DUMMY_FILE
sudo losetup $DUMMY_DEV $DUMMY_FILE
mkdir -p $MNTPNT
sudo mount -t ftfs -o sb_fstype=sfs,d_dev=$DUMMY_DEV,is_rotational=$SFS_IS_ROTATIONAL $SB_DEV $MNTPNT
sudo chown -R $USER: $MNTPNT

# print module addresses for debugging in gdb
sudo cat /sys/module/{simplefs,ftfs}/sections/.text
