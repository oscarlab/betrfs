#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Please pass a value for sb_dev and a value for sb_fstype to this script"
    exit 1
fi

sudo losetup /dev/loop0 dummy.dev
sudo mount -t ftfs -o sb_dev=$1,sb_fstype=$2 /dev/loop0 mnt
