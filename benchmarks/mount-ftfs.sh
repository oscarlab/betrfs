#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

set -x

touch $DIR/$dummy_file
losetup $dummy_dev $DIR/$dummy_file

touch $DIR/$dummy_file
losetup $dummy_dev $DIR/$dummy_file

# mount the file system
mkdir -p $mntpnt
modprobe zlib
insmod $module
mount -t ftfs -o sb_fstype=ext4,d_dev=$dummy_dev $sb_dev $mntpnt
chown -R betrfs:betrfs $mntpnt
