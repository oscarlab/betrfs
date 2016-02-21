#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

set -x

# prep file system
# mkfs -t ext4 $sb_dev
mkfs -t ext4 -E lazy_itable_init=0,lazy_journal_init=0 $sb_dev &&
# mount the file system
mkdir -p $mntpnt && 
mount -t ext4 $sb_dev $mntpnt &&
chown -R ftfs:ftfs $mntpnt
exit $?
