#!/bin/bash

if [ "$#" -ne 1 ] ; then
	echo "Need fstype as argument!"
	exit 1
fi
FS=$1

sudo ../../cleanup-fs.sh
sudo ../../setup-$FS.sh
./misc-syscall-times -a
./misc-syscall-times -s
./misc-syscall-times -o
./misc-syscall-times -u
./misc-syscall-times -m
./misc-syscall-times -r
./misc-syscall-times -w
./misc-syscall-times -c
