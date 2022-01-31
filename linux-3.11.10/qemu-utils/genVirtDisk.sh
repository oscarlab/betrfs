#! /bin/sh
set -x

mkdir -p mnt
mntpnt=mnt

if [ -z "$1" ]
then
    echo "target disk omitted, using rootfs.raw"
    diskName="rootfs.raw"
else
    diskName="$1"
fi

echo "generating 15G .raw disk image: $diskName (ext4 rootfs)"

qemu-img create -f raw $diskName 15G

chmod 777 $diskName

mkfs.ext4 -F $diskName

sudo mount -o loop $diskName $mntpnt

echo "building for amd64 and \"raring\" ubuntu release"

sudo debootstrap --arch amd64  \
    --include=openssh-server,emacs,git,python,build-essential,cmake,zlib1g-dev,valgrind \
    wheezy $mntpnt
retval=$?
if [$retval -ne 0]; then
    echo "debootstrap failed with exit value $retval"
    exit $retval
fi

# Make root passwordless for convenience.
sudo sed -i '/^root/ { s/:x:/::/ }' $mntpnt/etc/passwd

# Automatically bring up eth0 using DHCP
printf '\nauto eth0\niface eth0 inet dhcp\n' | sudo tee -a $mntpnt/etc/network/interfaces

# Set up my ssh pubkey for root in the VM
sudo mkdir $mntpnt/root/.ssh/
cat ~/.ssh/*_rsa.pub | sudo tee $mntpnt/root/.ssh/authorized_keys

sudo umount $mntpnt

sbDisk=device.raw
echo "generating 10G .raw disk image for southbound: $sbDisk (ext4)"
qemu-img create -f raw $sbDisk 10G
mkfs.ext4 -F $sbDisk
sudo mount -o loop $sbDisk $mntpnt
sudo chown -R $whoami:$whoami $mntpnt
mkdir $mntpnt/dev
touch $mntpnt/dev/null
mkdir $mntpnt/tmp
chmod 1777 $mntpnt/tmp

sudo umount $mntpnt
