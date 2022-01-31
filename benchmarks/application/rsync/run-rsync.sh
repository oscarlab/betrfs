#!/bin/bash

. ../../fs-info.sh

support=$FT_HOMEDIR/benchmarks/support-files
src=linux-3.11.10
dst=rsynced
dst2=rsynced2
copypos=temploc

if [ ! -e $support/$src ]; then
    cd $support; tar -xf $src.tar.xz; cd -
fi

cp -r $support/$src $mntpnt/
sudo -E ../../clear-fs-caches.sh $1

#echo 'Copy from home to benchmark partition'
#sudo perf record -g -- rsync --stats -r -t -S -h $support/$src $mntpnt/$dst
#strace -c -T rsync --stats -r -t -S -h $support/$src $mntpnt/$dst 2&> strace.txt
echo "-------------------------------------------------------------------"
rsync --stats -r -t -S  $mntpnt/$src $mntpnt/$dst
#rsync --stats -r -t -W -h --inplace $support/$src $mntpnt/$dst

sudo -E ../../clear-fs-caches.sh $1

if [ -e $mntpnt/$dst2 ]; then
    cd $mntpnt; rm -r $dst2; cd -
fi

echo "-------------------------------------------------------------------"

#echo 'Copy from benchmark to benchmark partition'
#sudo perf record -g -- rsync --stats -r -t -S -h $mntpnt/$dst $mntpnt/$dst2
#rsync --stats -r -t -S -h $mntpnt/$dst $mntpnt/$dst2
#strace rsync --stats -r -t -S -h $mntpnt/$dst $mntpnt/$dst2 2&> strace.txt
rsync --stats -r -t -W  --inplace $mntpnt/$dst $mntpnt/$dst2
#rsync --stats -r -t -W -h $mntpnt/$dst $mntpnt/$dst2
#if [ -e $support/$copypos ]; then
#    cd $support; rm -r $copypos; cd -
#fi
#sudo ../../clear-fs-caches.sh
#echo 'Copy from benchmark to home partition'
#sudo perf record -g -- rsync --stats -r -t -S -h $mntpnt/$dst $support/$copypos
#rsync --stats -r -t -S -h $mntpnt/$dst $support/$copypos
#rsync --stats -r -t -W -h --inplace $mntpnt/$dst $support/$copypos

# -r recurse into directories
# -t preserve modification times
# -S handle sparse file effectively
# --statsx give some file transfer stats
# -h output numbers in human readable format
