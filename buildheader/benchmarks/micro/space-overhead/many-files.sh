#!/bin/bash

exe="../lmbench-3/bin/x86_64-linux-gnu/lmdd"

if [ ! -e $exe ] ; then
    echo "Stop! You must run 'make' in ../lmbench-3 first!"
    exit 1
fi

if [  -z "$1" ] ; then
    echo "Usage: $0 <fs_type>"
    exit 1
fi
gcc generate-file.c -o generate-file
# benchmark parameters
io_size=4096
mnt="/mnt/benchmark"
input="test.file"
f_size=($((1024*10)) $((1024*1024)) $((1024*1024*10)) $((1204*1024*100)) $((1024*1024*1024)))
t_size=0;
green='\033[0;32m'
NC='\033[0m' # No Color

sudo ../../cleanup-fs.sh
sudo rm -rf $mnt
sudo ../../setup-$1.sh >> /dev/null

echo "Generating files"
for file_size in ${f_size[@]}; do
#set -x
t_size=$((t_size + file_size))
./generate-file $file_size $mnt/$input$file_size #uses urandom
done
#ls -l $mnt >> /dev/null #to initiate flush
#sleep 3
#stdbuf -o0  head -c $file_size /dev/urandom | stdbuf -i0 -o0 tee $mnt/$input > /dev/null
#$exe if=internal of=$mnt/$input bs=$io_size count=$(($file_size / $io_size)) opat=1 fsync=1
usage=$(df $mnt | sed 's/  */ /g' | cut -d$'\n' -f2 | cut -d ' ' -f3)
echo -e "${green}filesize(K), usage(K), overhead(K)${NC}"
echo -e "${green}$(($t_size / 1024)), ${usage}, $((-$t_size / 1024 + $usage))${NC}"

