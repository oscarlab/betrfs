#!/bin/bash
## script to pull commits from your local repository to test on a kvm
## virtual disk.

set -eux
# this will also process the KERNEL_VERSION command-line argument
source local-repo-params.include
if [ $EUID -ne 0 ]; then echo "Please run as root"; exit 1; fi
BRANCH=$(git rev-parse --abbrev-ref HEAD)
trap "cd $PWD; sudo umount $MOUNTPOINT; exit \$status" EXIT HUP INT QUIT TERM
status=1 # fail by default

## mount your virtual disk from a loopback device --- a loopback
## device lets you pretend a file as is a block device, so mount that
## block device on your mountpoint
mkdir -p $MOUNTPOINT
mount -o loop $VIRTUALDISK $MOUNTPOINT
chown -R $USERID:$USERID $MOUNTPOINT/home/$USER

## pull, build the ft code, and build the ftfs code
cd $MOUNTPOINT/$REPO; git fetch --all --prune; git checkout "$BRANCH"; git reset --hard origin/$BRANCH; cd -;
cd $MOUNTPOINT/$REPO/build; ./cmake-ft-debug.sh; cd -;
cd $MOUNTPOINT/$REPO/ftfs; make MOD_KERN_SOURCE=$OLDPWD/../linux-$KERNEL_VERSION debug; cd -;
cd $MOUNTPOINT/$REPO/filesystem; make MOD_KERN_SOURCE=$OLDPWD/../linux-$KERNEL_VERSION debug; cd -;
cd $MOUNTPOINT/$REPO/simplefs; make MOD_KERN_SOURCE=$OLDPWD/../linux-$KERNEL_VERSION; cd -;

## copy kernel modules for debugging
cp $MOUNTPOINT/$REPO/simplefs/simplefs.ko simplefs-$KERNEL_VERSION.ko
cp $MOUNTPOINT/$REPO/filesystem/ftfs.ko ftfs-filesystem-$KERNEL_VERSION.ko
cp $MOUNTPOINT/$REPO/ftfs/ftfs.ko ftfs-toku-$KERNEL_VERSION.ko
chown $(logname): *.ko
status=0
