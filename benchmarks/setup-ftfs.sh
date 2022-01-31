#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"
. "$DIR/.ismounted"

echo "Setup Circle Size: $circle_size"
sh -c 'echo "7" > /proc/sys/kernel/printk'

# prep file system
$DIR/mkfs.ftfs $sb_dev
touch $DIR/$dummy_file
losetup $dummy_dev $DIR/$dummy_file 

sudo sh -c "echo 7 > /proc/sys/kernel/printk"

# mount the file system
mkdir -p $mntpnt
#modprobe zlib
echo "Insert module: $module"
insmod $module sb_dev=$sb_dev sb_fstype=ext4
mount -t ftfs $dummy_dev $mntpnt -o max=$circle_size
chown -R $user_owner $mntpnt
# imap-test needs this
chmod 777 $mntpnt
