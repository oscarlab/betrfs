#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

#set -x

echo "Setup Circle Size: $1"
#echo "Setup Circle Size: $circle_size"

# prep file system
$DIR/mkfs.ftfs $sb_dev

# mount the file system
mkdir -p $mntpnt
modprobe zlib
insmod $module
mount -t ftfs -o max=$1,sb_fstype=ext4,d_dev=$dummy_dev $sb_dev $mntpnt
#$circle_size
chown -R betrfs:betrfs $mntpnt
