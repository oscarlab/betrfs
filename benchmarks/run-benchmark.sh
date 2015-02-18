#!/bin/bash
set -x
#do the req setup.
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
. "$DIR/.rootcheck"
. "$DIR/.mountcheck"

cd ~/ft-index/build
./cmake-ft.sh
cd ../filesystem
make clean
make

cd ~/paper/fast15/data/
shopt -s nullglob
for f in *.csv; 
do 
mv $f $f.old;
done

cd ../benchmarks

#The runtests scripts should update the corresponding .csv files in the paper repo 
./micro/runmicrotests.sh
./macro/runmacrotests.sh
./application/runapptests.sh

#Email out the generated graphs.
cd ~/paper/fast15/text/
make graphs
