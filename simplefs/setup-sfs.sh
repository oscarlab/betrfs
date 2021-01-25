#!/bin/bash
set -x

if [ $# != 2 ];
then 
 echo "need two argument!"
 exit 
fi

echo "Dev:${1}"
echo "Mountpt:${2}"

mountpt=$2
if grep -qs "$mountpt" /proc/mounts; then
  echo "$mountpt is mounted."
  exit
fi

make

DEV=$1
echo $DEV
sudo mkdir -p tmp
sudo rm -rf tmp/*
sudo rmmod simplefs  >& /dev/null

sudo ./mkfs-sfs $DEV
sudo insmod simplefs.ko
sudo mount -t sfs $DEV $mountpt
