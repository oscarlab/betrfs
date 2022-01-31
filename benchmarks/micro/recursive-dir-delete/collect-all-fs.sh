#!/bin/bash

. ../../fs-info.sh

SEQ=`seq ${rm_rf_num}`

for x in ${!allfs[*]}
do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}
    for i in $SEQ 
    do
        sudo ../../cleanup-fs.sh
        sudo ../../setup-${FS}.sh
        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./recur-del.sh"
        echo $cmd
        (
             $cmd 
        ) &> $resultdir/$TEST-del-${TIME}.csv

        ERROR=$?
        if [ $ERROR -ne 0 ] ; then
             echo "got error $ERROR"
             exit 1
        fi

        sed -i 's/real/result.'${TEST}', rmdir,/'  $resultdir/$TEST-del-${TIME}.csv

        sudo ../../clear-fs-caches.sh
        sleep 1
    done
done         
