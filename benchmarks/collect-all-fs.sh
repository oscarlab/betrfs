#!/bin/bash
set -e

. fs-info.sh

if [ ! -f "/usr/bin/ministat" ]
then
   sudo apt-get install ministat
fi

cp -r  $resultdir ${resultdir}-old
rm -rf $resultdir/*.csv

#### tokubench ###
cd micro/tokubench
make
./collect-all-fs.sh
cd -

#### dir ops ###
cd micro/recursive-grep
./collect-all-fs.sh
cd -

cd micro/recursive-scan-find
./collect-all-fs.sh
cd -

cd micro/recursive-dir-delete
./collect-all-fs.sh
cd -

#### large write
cd micro/large-file
make
./collect-all-fs.sh
cd -
