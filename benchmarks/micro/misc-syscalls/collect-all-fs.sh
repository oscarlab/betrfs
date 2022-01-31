#!/bin/bash

rm *.results

for fs in ftfs zfs xfs nilfs2 btrfs ext4
do
   TIME=`date +"%d-%m-%Y-%H-%M-%S"`
   cmd="./run-all.sh $fs"
   echo $cmd
   (
        $cmd 
   ) | tee -a $fs-misc-syscall-${TIME}.results
done

