#!/bin/sh

if [ "$#" -ne 1 ] ; then
	echo "Need fstype as argument!"
	exit 1
fi
FS=$1
sudo dovecot stop


sudo ../../../cleanup-fs.sh
sudo ../../../setup-${FS}.sh
sudo cp -r /home/ftfstest/ftfstest/ /mnt/benchmark/
sudo chown -R ftfstest:ftfstest /mnt/benchmark/ftfstest/

#### Stop and Start Again ####
sudo dovecot stop
sudo dovecot
./imap-punish.py
