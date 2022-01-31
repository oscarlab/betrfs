#!/bin/bash

. ../../fs-info.sh

FS=$1
TEST=$2
SEQ=`seq ${test_num}`


for i in $SEQ
do
  ../../cleanup-fs.sh ;
  ../../setup-${FS}.sh ;
  ./run-large-file-write.sh -f $TEST -s $((80*1024*1024*1024));
  ./run-large-file-read.sh -f $TEST -s $((80*1024*1024*1024));
done

#for i in $SEQ
#do
#  ../../cleanup-fs.sh ;
#  ../../setup-${FS}.sh ;
#  ./run-large-file-write.sh -f $TEST;
#  ./run-large-file-read.sh -f $TEST;
#done

for i in $SEQ
do
  ../../cleanup-fs.sh ;
  ../../setup-${FS}.sh ;
  ./run-large-file-write.sh -f $TEST;
  ./run-large-file-random_write.sh $TEST;
done

for i in $SEQ
do
  ../../cleanup-fs.sh ;
  ../../setup-${FS}.sh ;
  ./run-large-file-write.sh -f $TEST;
  ./run-large-file-random_write_4k.sh $TEST;
done

#for i in $SEQ
#do
#  ../../cleanup-fs.sh ;
#  ../../setup-${FS}.sh ;
#  ./run-large-file-write.sh $TEST;
#  ./run-large-file-random_read.sh ${TEST} 4;
#done

#for i in $SEQ
#do
#  ../../cleanup-fs.sh ;
#  ../../setup-${FS}.sh ;
#  ./run-large-file-write.sh $TEST;
#  ./run-large-file-random_read.sh ${TEST} 4096;
#done

