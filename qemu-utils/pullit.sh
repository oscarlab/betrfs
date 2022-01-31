#!/bin/bash

## script to pull commits from your local repository to test on a kvm
## virtual disk.

set -x
## Stop on an error
set -e

source local-repo-params.include

## mount your virtual disk from a loopback device --- a loopback
## device lets you pretend a file as is a block device, so mount that
## block device on your mountpoint
sudo mount -o loop $VIRTUALDISK $MOUNTPOINT
sudo chown -R $USERID:$USERID $MOUNTPOINT/home/$USER

## pull, build the ft code, and build the ftfs code
cd $MOUNTPOINT/$REPO/ftfs; git pull; cd -;
cd $MOUNTPOINT/$REPO/build; ./cmake-ft.sh; cd -;
cd $MOUNTPOINT/$REPO/ftfs; make; cd -;
#cd $MOUNTPOINT/$REPO/ftfs_fs; make; cd -;
cd $MOUNTPOINT/$REPO/filesystem; make; cd -;
cd $MOUNTPOINT/$REPO/simplefs; make; cd -;

## cleanup
sudo umount $MOUNTPOINT
