#!/bin/bash
# script to create a sample testing environment for betrfs
# This script closely follows the "Sample Testing Environment" wiki.
# Usage: ./create-test-env.sh [ KERNEL_VERSION ]
# Default KERNEL_VERSION defined in local-repo-params.include

set -eux
source local-repo-params.include
# this will also process the KERNEL_VERSION command-line argument

# install dependencies
# debootstrap: not needed if you can download the cached image below.
# expect: automation of the VM setup process.
# bison flex libelf-dev: compiling the kernel.
sudo apt-get install -y \
	qemu \
	debootstrap \
	git \
	build-essential \
	zlib1g-dev \
	gcc-7 \
	g++-7 \
	gcc \
	valgrind \
	cmake \
	expect \
	bison \
	flex \
	libelf-dev \
	libssl-dev

# download kernel if not present
if [ ! -d ../linux-$KERNEL_VERSION ]; then
	wget --no-clobber -P xfstests https://cdn.kernel.org/pub/linux/kernel/v${KERNEL_VERSION:0:1}.x/linux-${KERNEL_VERSION}.tar.xz
	tar -xf xfstests/linux-$KERNEL_VERSION.tar.xz -C ..
fi

# build kernel
cd ../linux-$KERNEL_VERSION
cp ../qemu-utils/kvm-config .config
make olddefconfig
make -j$(nproc --all)
make modules
cd ../qemu-utils

# create boot image (linuxdisk.raw) and southbound image (ftfs-southbound.raw)
# try to download cached image. The cached image is qcow2 format instead of raw
# since the raw image is sparse, and it's hard (impossible?) to transfer sparse
# images over HTTP efficiently without converting to another format first.
set +e
wget --no-clobber --tries=2 https://leeroy2.cs.unc.edu/images/linuxdisk-$KERNEL_VERSION.qcow2
set -e
if [ ! -f linuxdisk-$KERNEL_VERSION.qcow2 ]; then
	./genUbuntuDisk.sh linuxdisk-$KERNEL_VERSION.raw
else
	echo "Downloaded cached image. Converting it to raw format."
	qemu-img convert -f qcow2 -O raw linuxdisk-$KERNEL_VERSION.qcow2 linuxdisk-$KERNEL_VERSION.raw
	./genSouthboundDisk.sh
fi

# set up virtual disk build environment
mkdir -p $MOUNTPOINT
sudo mount -o loop linuxdisk-$KERNEL_VERSION.raw $MOUNTPOINT
cd ../linux-$KERNEL_VERSION
# kvm-config doesn't have any modules enabled, but if we enable any in the
# future, this is the place to install them.
sudo make INSTALL_MOD_PATH=../qemu-utils/$MOUNTPOINT modules_install

# set up your virtual disk configuration files. some of this is optional
cd ../qemu-utils
# remove the first 'x' after root so you can log in and run sudo without a password
sudo sed -i -e 's/^root:x:/root::/g' $MOUNTPOINT/etc/passwd
# set VM's hostname so benchmarking scripts don't complain.
echo 'betrfs.vm' | sudo tee $MOUNTPOINT/etc/hostname
# create symlink for /dev/sdb (if not present) since the benchmarking scripts use that instead of
# hdb. https://askubuntu.com/a/784377
echo 'ACTION=="add", KERNEL=="hdb", SYMLINK+="sdb"' | sudo tee $MOUNTPOINT/etc/udev/rules.d/99-sdb.rules
# inherit host's timezone
sudo ln -sf $(readlink /etc/localtime) $MOUNTPOINT/etc/localtime
sudo umount $MOUNTPOINT

# create new user in VM
# https://fadeevab.com/how-to-setup-qemu-output-to-console-and-automate-using-shell-script/
expect -c 'set timeout -1' \
	-c "spawn ./q $KERNEL_VERSION -serial" \
	-c 'expect "login: "' \
	-c 'send "root\r"' \
	-c 'expect "# "' \
	-c "send \"echo -e 'asd\nasd\n' | adduser $USER --gecos $USER && usermod -aG sudo,adm $USER\r\"" \
	-c 'expect "# "' \
	-c 'send "shutdown -h now\r"' \
	-c 'expect EOF'

# copy repo to guest VM
sudo mount -o loop linuxdisk-$KERNEL_VERSION.raw $MOUNTPOINT
cd $MOUNTPOINT/home/$USER
sudo git clone $(realpath $OLDPWD/..)
sudo mkdir -p betrfs-private/build
sudo cp betrfs-private/qemu-utils/cmake-ft-debug.sh betrfs-private/build

# TODO: copy helper scripts
cd -
sudo cp mount-betrfs.sh $OLDPWD

# disable password for new user and add hostname to hosts
# the latter will prevent sudo from complaining each time you use it (not required)
sudo sed -i -e "s/^$USER:x:/$USER::/g" $MOUNTPOINT/etc/passwd
echo -e "127.0.0.1\t$(cat $MOUNTPOINT/etc/hostname)" | sudo tee -a $MOUNTPOINT/etc/hosts
# automatically login: https://superuser.com/a/1423805/1287825
# this has the side-effect of making it appear that all six ttys are logged in
sudo mkdir -p $MOUNTPOINT/etc/systemd/system/getty@.service.d
cat <<EOF | sudo tee $MOUNTPOINT/etc/systemd/system/getty@.service.d/override.conf
[Service]
ExecStart=
ExecStart=-/sbin/agetty --noclear --autologin $USER %I \$TERM
EOF
# auto login for serial console
sudo mkdir -p $MOUNTPOINT/etc/systemd/system/serial-getty@ttyS0.service.d
cat <<EOF | sudo tee $MOUNTPOINT/etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf
[Service]
ExecStart=
ExecStart=/sbin/agetty --keep-baud 115200,38400,9600 --noclear --autologin $USER %I \$TERM
EOF
sudo umount $MOUNTPOINT

# build all code for VM
sudo ./pullit.sh $KERNEL_VERSION
