#!/bin/bash

. ../../fs-info.sh

for x in ${!allfs[*]}
do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}


    for i in 1 2 3 #4 5
    do
        sudo ../../cleanup-fs.sh
        sudo ../../setup-${FS}.sh

        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./run-rsync.sh"
        echo $cmd
        (
             $cmd 
        ) &> ${resultdir}/${TEST}-rsync-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi

        RES1=`grep "bytes/sec"  ${resultdir}/${TEST}-rsync-${TIME}.csv  | head -n 1 | sed 's/,//g' | awk '{print $2/1000000 ,$5/1000000, $7/1000000}' | sed 's/^/result.'${TEST}' rsync /'`
        RES2=`grep "bytes/sec" ${resultdir}/${TEST}-rsync-${TIME}.csv  | tail -n 1 | sed 's/,//g' | awk '{print $2/1000000, $5/1000000, $7/1000000}' | sed 's/^/result.'${TEST}' rsync-inplace /'`

        echo $RES1 >> $resultdir/${TEST}-rsync-${TIME}.csv 
        echo $RES2 >> $resultdir/${TEST}-rsync-${TIME}.csv 

        sudo ../../clear-fs-caches.sh
        sleep 1
    done
done
