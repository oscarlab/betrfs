#! /bin/sh
set -e
set -x

if [ $# -neq 2 ]; then
	echo "usage: mkfs-ext4.sh southbound_device mountpoint"
	exit 1
fi

mkfs.ext4 $1

mount $1 $2
cd $2
mkdir db;

fallocate -l 2G     db/log000000000000.tokulog25
fallocate -l 15G    db/ftfs_data_2_1_19.tokudb
fallocate -l 1G     db/ftfs_meta_2_1_19.tokudb
fallocate -l 1M     db/tokudb.directory
fallocate -l 1M     db/tokudb.environment		
touch               db/data
touch               db/environment	
touch               db/logs	
touch               db/recovery
touch               db/temp
fallocate -l 256M   db/tokudb.rollback
fallocate -l 256M   db/test_one_2_1_19.tokudb	
fallocate -l 256M   db/test_two_2_1_19.tokudb
fallocate -l 256M   db/test_three_2_1_19.tokudb

dd if=/dev/zero of=db/log000000000000.tokulog25 count=1 bs=1M conv=notrunc
dd if=/dev/zero of=db/ftfs_data_2_1_19.tokudb   count=1 bs=1M conv=notrunc
dd if=/dev/zero of=db/ftfs_meta_2_1_19.tokudb   count=1 bs=1M conv=notrunc

dd if=/dev/zero of=db/tokudb.directory          count=1 bs=1M conv=notrunc
dd if=/dev/zero of=db/tokudb.environment        count=1 bs=1M conv=notrunc
dd if=/dev/zero of=db/tokudb.rollback           count=1 bs=1M conv=notrunc

dd if=/dev/zero of=db/test_one_2_1_19.tokudb     count=1 bs=1M conv=notrunc
dd if=/dev/zero of=db/test_two_2_1_19.tokudb     count=1 bs=1M conv=notrunc
dd if=/dev/zero of=db/test_three_2_1_19.tokudb   count=1 bs=1M conv=notrunc

cd -
umount $2

modprobe zlib
sudo sh -c "echo 7 > /proc/sys/kernel/printk"

