#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

#set -x

echo "Setup Circle Size: $circle_size"
sh -c 'echo "7" > /proc/sys/kernel/printk'

# prep file system
$DIR/mkfs.ftfs $sb_dev

sudo sh -c "echo 7 > /proc/sys/kernel/printk"

# mount the file system
mkdir -p $mntpnt
modprobe zlib
echo "Insert module: $module"
insmod $module
mount -t ftfs -o max=$circle_size,sb_fstype=ext4 $sb_dev $mntpnt
chown -R betrfs:betrfs $mntpnt
