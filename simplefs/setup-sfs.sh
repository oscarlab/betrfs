#!/bin/bash
#set -x

if [ $# != 2 ]  && [ $# != 3 ]; then
 echo "need two or three arguments!"
 exit
fi

echo "Dev:${1}"
echo "Mountpt:${2}"
echo "Use SSD:${3}"

mountpt=$2
if grep -qs "$mountpt" /proc/mounts; then
  echo "$0 failed: $mountpt is mounted."
  exit 1
fi

DEV=$1
echo $DEV

if [ $# == 2 ]; then
  is_rotational=`lsblk -d $DEV  -o name,rota  | tail -n 1 | awk '{print $2}'`
elif [ $# == 3 ]; then
  if [ $3 == "--force-ssd" ]; then
    is_rotational=0
    echo "We are forcing ssd-specific test"
  elif [ $3 == "--force-hdd" ]; then
    is_rotational=1
    echo "We are forcing hdd-specific test"
  else
    echo "$0: the 3rd argument has to be --force-ssd or --force-hdd"
    exit 1
  fi
fi

# stop if error occurs and propagate them out
set -eu

sudo mkdir -p $mountpt
sudo rm -rf $mountpt/*
if lsmod | grep -q simplefs; then
	sudo rmmod simplefs
fi

sudo ./mkfs-sfs $DEV
sudo insmod simplefs.ko sfs_is_rotational=${is_rotational}
sudo mount -t sfs $DEV $mountpt
