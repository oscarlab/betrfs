#!/bin/bash
# utility to replace xfstest image with the image mounted at /mnt using mount-xfstests.sh

source config
set -eux

if [ $EUID -ne 0 ]; then
	echo "Please run as root"
	exit 1
fi

umount -d /mnt
qemu-img convert img.raw $FSTESTS_DIR/kvm-xfstests/$ROOT_FS
rm img.raw
