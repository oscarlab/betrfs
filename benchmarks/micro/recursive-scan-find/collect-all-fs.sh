#!/bin/bash

. ../../fs-info.sh

SEQ=`seq ${test_num}`

for x in ${!allfs[*]}
do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}

    for i in $SEQ
    do
        sudo ../../cleanup-fs.sh
        sudo ../../setup-${FS}.sh

        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./recur-find.sh"
        echo $cmd
        (
             $cmd 
        ) &> $resultdir/$TEST-find-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi

        sed -i 's/real/result.'${TEST}', find,/'  $resultdir/$TEST-find-${TIME}.csv

        sleep 1
    done
done         

