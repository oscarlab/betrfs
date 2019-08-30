#! /bin/sh
set -x
set -e

if [ -z "$1" ]
then
    echo "target disk omitted, using linuxdisk.raw"
    diskName="linuxdisk.raw"
else
    diskName="$1"
fi

echo "generating 15G .raw disk image: $diskName (ext4 rootfs)"

qemu-img create -f raw $diskName 15G

chmod 777 $diskName

mkfs.ext4 -F $diskName

sudo mount -o loop $diskName /mnt

echo "building for amd64 and \"trusty\" ubuntu release"

#sudo debootstrap --arch amd64 trusty /mnt http://old-releases.ubuntu.com/ubuntu
sudo debootstrap --arch amd64 trusty /mnt http://us.archive.ubuntu.com/ubuntu

#bind /dev to /mnt/dev
sudo mount --bind /dev/ /mnt/dev

sudo cp install-packages.sh /mnt/install-packages.sh

#change the root to /mnt
sudo chmod a+x /mnt/install-packages.sh
sudo chroot /mnt ./install-packages.sh

sudo umount /mnt/dev

# Ubuntu complains if device files for /dev/hda and /dev/hdb are missing
sudo mknod /mnt/dev/hda b 3 0
sudo mknod /mnt/dev/hdb b 3 64

sudo umount /mnt

sbDisk=ftfs-southbound.raw
echo "generating 10G .raw disk image for southbound: $sbDisk (ext4)"
qemu-img create -f raw $sbDisk 10G
mkfs.ext4 -F $sbDisk
