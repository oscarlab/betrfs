#!/bin/bash

FILESYS=$1

sudo ./clean-mount.sh $1 > /dev/null
result=`./run-large-file-write.sh | grep "^write"`
echo "$FILESYS, $result"
result=`./run-large-file-read.sh \"first\" | grep "^10737" |  awk '{print $6", "$8}'`
echo "$FILESYS, read, seq, 10737.418240, $result"
result=`./run-large-file-random_write.sh | grep "^write"`
echo "$FILESYS, $result"
result=`./run-large-file-read.sh \"second\" | grep "^10737" |  awk '{print $6", "$8}'`
echo "$FILESYS, read, seq, 10737.418240, $result"
printf "\n"
