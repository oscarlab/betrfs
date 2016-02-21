#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

set -x

if [ ! -z "$1" ]
then
# prep file system
    $DIR/mkfs.ftfs $sb_dev
fi


modprobe zlib
insmod $southbound_module sb_dev=$sb_dev sb_fstype=ext4
