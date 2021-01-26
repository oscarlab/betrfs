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

blkdev=`echo $sb_dev | tr -d [:digit:]`
is_rotational=`lsblk -d $blkdev -o name,rota  | tail -n 1 | awk '{print $2}'`

if [[ $use_sfs == "true" ]]
    mount -t ftfs -o max=$1,sb_fstype=sfs,d_dev=$dummy_dev $sb_dev $mntpnt
    mount -t ftfs -o max=$1,sb_fstype=sfs,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
else
    mount -t ftfs -o max=$1,sb_fstype=ext4,d_dev=$dummy_dev $sb_dev $mntpnt
    mount -t ftfs -o max=$1,sb_fstype=ext4,d_dev=$dummy_dev,is_rotational=$is_rotational $sb_dev $mntpnt
fi

#$circle_size
chown -R $user_owner $mntpnt
