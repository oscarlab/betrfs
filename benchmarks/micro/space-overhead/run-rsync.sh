#!/bin/bash

. ../../fs-info.sh
. ../../.ismounted

if [  -z "$1" ] ; then
    echo "Usage: $0 <fs_type>"
    exit 1
fi

support=$HOME/$repo/benchmarks/support-files
src=linux-3.11.10
dst=rsynced
dst2=rsynced2
copypos=temploc

if [ ! -e $support/$src ]; then
    cd $support; tar -xf $src.tar.xz; cd -
fi

sudo ../../cleanup-fs.sh
sudo rm -rf $mntpnt
sudo ../../setup-$1.sh >> /dev/null

#echo 'Copy from home to benchmark partition'
#sudo perf record -g -- rsync --stats -r -t -S -h $support/$src $mntpnt/$dst
#strace -c -T rsync --stats -r -t -S -h $support/$src $mntpnt/$dst 2&> strace.txt
#rsync --stats -r -t -S -h $support/$src $mntpnt/$dst
l_size=$(rsync --stats -r -t -W -h --inplace $support/$src $mntpnt/$dst | tail -n 1 | sed 's/  */ /g' | cut -d ' ' -f4 | cut -d 'M' -f1)
l_size=$(echo "$l_size*1024" | bc)

usage=$(df $mntpnt | sed 's/  */ /g' | cut -d$'\n' -f2 | cut -d ' ' -f3)
echo -e "${green}filesize(K), usage(K), overhead(K)${NC}"
overhead=$(echo "$usage - $l_size" | bc)
echo -e "${green}$l_size, ${usage}, $overhead${NC}"
