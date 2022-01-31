#!/bin/bash
# utility to mount the the xfstests image at /mnt
# you can edit (or just examine) the contents of /mnt, and then use umount-xfstests.sh to write
# your changes back to the image.

source config
set -eux

if [ $EUID -ne 0 ]; then
	echo "Please run as root"
	exit 1
fi

if [ -e img.raw ]; then
	echo "img.raw already exists. Probably need to run ./umount-xfstests.sh first."
	exit 1
fi

if mountpoint -q /mnt; then
	echo "A filesystem is already mounted on /mnt"
	exit 1
fi

qemu-img convert $FSTESTS_DIR/kvm-xfstests/$ROOT_FS img.raw
losetup /dev/loop0 img.raw
mkdir -p /mnt
mount /dev/loop0 /mnt
