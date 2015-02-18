#!/bin/bash
set -x
. ../../fs-info.sh

if [ -d ../../support-files/jailroot ] ; then
:
else
    sudo debootstrap --arch amd64 precise ../../support-files/jailroot
fi
cp -r ../../support-files/jailroot $mntpnt/jailroot 
sudo mount --bind /dev/ $mntpnt/jailroot/dev/
sudo mount --bind /dev/pts $mntpnt/jailroot/dev/pts
sudo mount --bind /proc $mntpnt/jailroot/proc
sudo mount --bind /sys $mntpnt/jailroot/sys
