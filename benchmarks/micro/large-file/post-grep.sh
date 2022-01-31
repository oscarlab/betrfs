FS=zfs
grep "write, seq" $FS-large-write-*.results | grep -v before  | grep 1073 > $FS-write.tmp
grep "write" $FS-random-write-*.results > $FS-random.tmp  
grep MB $FS-seq-read-*.results > $FS-read.tmp


FS=ftfs
grep "write, seq" $FS-large-write-*.results | grep -v before  | grep 1073 > $FS-write.tmp
grep "write" $FS-random-write-*.results > $FS-random.tmp  
grep MB $FS-seq-read-*.results > $FS-read.tmp


FS=nilfs2
grep "write, seq" $FS-large-write-*.results | grep -v before  | grep 1073 > $FS-write.tmp
grep "write" $FS-random-write-*.results > $FS-random.tmp  
grep MB $FS-seq-read-*.results > $FS-read.tmp



