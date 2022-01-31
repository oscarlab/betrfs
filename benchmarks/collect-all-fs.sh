#!/bin/bash
set -e

. fs-info.sh

if [ ! -f "/usr/bin/ministat" ]
then
   sudo apt-get install ministat
fi

cp -r  $resultdir ${resultdir}-old
rm -rf $resultdir/*.csv

#### large write
cd micro/large-file
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

#### tokubench ###
cd micro/tokubench
./collect-all-fs.sh
cd -

#### applications ###
cd macro/mailserver/imap-test/
./collect-all-fs.sh
cd -

cd application/git
./collect-all-fs.sh
cd -

cd application/rsync
./collect-all-fs.sh
cd -

cd application/tar
./collect-all-fs.sh
cd -

### filebench ###
cd macro/filebench
./collect-all-fs.sh
cd -
