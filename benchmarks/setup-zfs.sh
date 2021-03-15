#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

set -x

# load zfs 
sudo modprobe zfs

rm -rf /mnt/benchmark/*

# prep file system
mkdir -p $mntpnt
sudo zpool create -f datastore $sb_dev

# mount the file system
sudo zfs create -o mountpoint=$mntpnt datastore/files
chown -R $user_owner $mntpnt
