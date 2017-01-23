#!/bin/bash

# This script makes sure the system has the requisite programs, especially a
# late enough version of git. It also downloads and untars the necessary linux
# repo.

set -x

apt-get update > /dev/null

# script dependencies
apt-get -y install blktrace > /dev/null

# git dependencies
apt-get -y install make git autoconf libcurl4-gnutls-dev libexpat1-dev gettext libz-dev libssl-dev > /dev/null

# filesystems
apt-get -y install xfsprogs f2fs-tools btrfs-tools > /dev/null
# zfs
sudo apt-get -y install build-essential gawk alien fakeroot gdebi libtool linux-headers-$(uname -r) &> /dev/null
sudo apt-get -y install zlib1g-dev uuid-dev libattr1-dev libblkid-dev libselinux-dev libudev-dev parted lsscsi wget ksh libtool &> /dev/null
git clone https://github.com/zfsonlinux/spl &> /dev/null
git clone https://github.com/zfsonlinux/zfs &> /dev/null
cd spl
git checkout master
sh autogen.sh
./configure
make -s -j$(nproc)
cd ../zfs
git checkout master
sh autogen.sh
./configure
make -s -j$(nproc)
./scripts/zfs-helpers.sh -i
./scripts/zfs.sh

#cd spl
#bash autogen.sh &> /dev/null 2>&1
#cd ../zfs
#bash autogen.sh &> /dev/null 2>&1
#cd ..
#cd spl
#./configure &> /dev/null 2>&1
#make pkg-utils pkg-kmod > /dev/null 2>&1
#for file in *.deb; do gdebi -q --non-interactive $file; done
#cd ../zfs
#./configure &> /dev/null 2>&1
#make pkg-utils pkg-kmod &> /dev/null 2>&1
#for file in *.deb; do gdebi -q --non-interactive $file; done
#cd ..

git clone https://github.com/git/git &> /dev/null
cd git
make configure > /dev/null
./configure --prefix=/usr > /dev/null
make all > /dev/null
make install > /dev/null
git config --global gc.autodetach False
git config --global gc.auto 0
git config --global uploadpack.allowReachableSHA1InWant True
git config --global user.name Ainesh
git config --global user.email ainesh.bakshi@rutgers.edu
