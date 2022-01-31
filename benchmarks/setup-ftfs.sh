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

if [ $# == "0" ]; then
    echo "Warning: No argument provided -- is_rotational will be inferred from underlying disk type."
    echo "If this doesn't match the format (hdd / ssd) declared at mount time, there will be bugs, and they will be strange."
    echo "(Magic string for Jenkins matching: rMv9WreOmX.)"
    is_rotational=`lsblk -d $sb_dev  -o name,rota  | tail -n 1 | awk '{print $2}'`
elif [ $# == 1 ]; then
    if [ $1 == "--force-ssd" ]; then
        is_rotational=0
    elif [ $1 == "--force-hdd" ]; then
        is_rotational=1
    else
        echo "Invalid argument: first argument should be (--force-hdd|--force-ssd)"
        exit 1
    fi
else
    echo "Too many arguments! 0 or 1 required."
    exit 1
fi

# prep file system
if [[ $use_sfs == "true" ]]
then
    $DIR/mkfs.ftfs.sfs $1
else
    $DIR/mkfs.ftfs
fi

touch $DIR/$dummy_file
losetup $dummy_dev $DIR/$dummy_file

# mount the file system
mkdir -p $mntpnt

echo "Insert module: $module"
insmod $module

if [[ $use_sfs == "true" ]]
then
    echo "Mounting ftfs: mount -t ftfs -o max=$circle_size,sb_fstype=sfs,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt"
    mount -t ftfs -o max=$circle_size,sb_fstype=sfs,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
else
    echo "Mounting ftfs: mount -t ftfs -o max=$circle_size,sb_fstype=ext4,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt"
    mount -t ftfs -o max=$circle_size,sb_fstype=ext4,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
fi

chown -R $user_owner $mntpnt
# imap-test needs this
chmod 777 $mntpnt
