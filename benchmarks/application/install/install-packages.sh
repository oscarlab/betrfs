#!/bin/bash

. ../../fs-info.sh
. ../../.rootcheck

instscript=".time-install-packages.sh"

../../clear-fs-caches.sh

cp $instscript $mntpnt/$instscript
chmod a+x $mntpnt/$instscript
chroot $mntpnt ./$instscript
