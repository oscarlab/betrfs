#!/bin/bash

. ../../fs-info.sh

SEQ=`seq ${test_num}`

for x in ${!allfs[*]}
do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}

    sudo ../../cleanup-fs.sh
    sudo ../../setup-${FS}.sh
    for i in $SEQ
    do
        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./recur-grep.sh"
        echo $cmd
        (
             $cmd 
        ) &> $resultdir/$TEST-grep-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi

        sed -i 's/real/result.'${TEST}', grep,/'  $resultdir/$TEST-grep-${TIME}.csv

        sudo ../../clear-fs-caches.sh
        sleep 1
    done
done         

