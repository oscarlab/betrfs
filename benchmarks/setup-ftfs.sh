#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

echo "Setup Circle Size: $circle_size"
sh -c 'echo "7" > /proc/sys/kernel/printk'

# prep file system
if [[ $use_sfs == "true" ]]
then
    $DIR/mkfs.ftfs.sfs $sb_dev
else
    $DIR/mkfs.ftfs $sb_dev
fi

touch $DIR/$dummy_file
losetup $dummy_dev $DIR/$dummy_file

# mount the file system
mkdir -p $mntpnt
modprobe zlib

echo "Insert module: $module"
insmod $module

blkdev=`echo $sb_dev | tr -d [:digit:]`
is_rotational=`lsblk -d $blkdev  -o name,rota  | tail -n 1 | awk '{print $2}'`

if [[ $use_sfs == "true" ]]
then
    echo "Mounting ftfs: mount -t ftfs -o max=$circle_size,sb_fstype=sfs,d_dev=$dummy_dev $sb_dev $mntpnt"
    mount -t ftfs -o max=$circle_size,sb_fstype=sfs,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
else
    mount -t ftfs -o max=$circle_size,sb_fstype=ext4,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
fi

chown -R $user_owner $mntpnt
# imap-test needs this
chmod 777 $mntpnt
