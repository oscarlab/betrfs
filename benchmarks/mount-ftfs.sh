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

# mount the file system
mkdir -p $mntpnt
modprobe zlib
insmod $module

if [[ $use_sfs == "true" ]]
    mount -t ftfs -o sb_fstype=sfs,d_dev=$dummy_dev $sb_dev $mntpnt
else
    mount -t ftfs -o sb_fstype=ext4,d_dev=$dummy_dev $sb_dev $mntpnt
fi

chown -R $user_owner $mntpnt
