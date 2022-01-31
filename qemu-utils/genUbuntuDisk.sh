#!/bin/bash
set -eux

if [ -z "$1" ]
then
    echo "target disk omitted, using linuxdisk.raw"
    diskName="linuxdisk.raw"
else
    diskName="$1"
fi

# generate boot disk image of VM
echo "generating 15G .raw disk image: $diskName (ext4 rootfs)"
qemu-img create -f raw $diskName 15G
chmod 777 $diskName
mkfs.ext4 -F $diskName
sudo mount -o loop $diskName /mnt
echo "building for amd64 and \"bionic\" ubuntu release"
sudo debootstrap --arch amd64 \
	--include=software-properties-common,build-essential,emacs,vim,zsh,git \
	bionic /mnt http://us.archive.ubuntu.com/ubuntu

# Ubuntu complains if device files for /dev/hda and /dev/hdb are missing
sudo mknod /mnt/dev/hda b 3 0
sudo mknod /mnt/dev/hdb b 3 64

sudo umount /mnt

# generate southbound disk. This is a separate script since we might download a
# cached image from leeroy2, but we still need to generate the southbound disk.
source genSouthboundDisk.sh
