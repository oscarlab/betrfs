#!/bin/bash

rm *.results

for FS in ftfs btrfs ext4 zfs xfs nilfs2 
do
    sudo ../../cleanup-fs.sh
    sudo ../../setup-${FS}.sh
    for i in 1 2 3 4 5
    do
        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./rename.sh"
        echo $cmd
        (
             $cmd 
        ) &> $FS-rename-${TIME}.results

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi
        sudo ../../clear-fs-caches.sh
        sleep 1
    done
done         

