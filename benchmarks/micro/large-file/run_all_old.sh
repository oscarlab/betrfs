FILESYS=ftfs

result=`./run-large-file-write.sh | grep "^write"`
echo "$FILESYS, $result"
result=`./run-large-file-read.sh | grep "^10737418240" |  awk '{print $6", "$8}'`
echo "$FILESYS, read, seq, 10737.41824, $result"
result=`./run-large-file-random_write.sh | grep "^write"`
echo "$FILESYS, $result"
