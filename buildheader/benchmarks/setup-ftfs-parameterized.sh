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
touch $DIR/$dummy_file
losetup $dummy_dev $DIR/$dummy_file 

# mount the file system
mkdir -p $mntpnt
modprobe zlib
insmod $module sb_dev=$sb_dev sb_fstype=ext4
mount -t ftfs $dummy_dev $mntpnt -o max=$1
#$circle_size
chown -R ftfs:ftfs $mntpnt
