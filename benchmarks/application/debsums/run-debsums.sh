#!/bin/bash

flag=0

#load the meta data file
. ../../fs-info.sh

#Read the FS param from command line and mount the respective FS

# install deb system on the mounted FS
echo "Installing system..."
. install-system.sh

#install packages on the debiam system
echo "Installing Packages..."
. install-packages.sh

#install debsums package
echo "Install debsums package"
apt-get install -y debsums

#run debsums on the packages on the mounted partition for 5 times in a loop
#and spit the results in a text file
for i  in 1 2 3 4 5
do
	echo "Running debsums on Ext4: $i" >> debsums.txt
	# running the debsums package from the host machine with different root dir
	(time debsums -a -s --root=$mntpnt) &>> debsums.txt
	../../clear-fs-caches.sh
done
