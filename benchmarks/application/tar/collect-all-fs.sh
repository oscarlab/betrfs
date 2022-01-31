#!/bin/bash

set -x

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
        cmd="./run-tar.sh"
        echo $cmd
        (
             $cmd 
        ) &>  $resultdir/$TEST-tar-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi
        sleep 1
        LINE_NUM=`wc -l ${resultdir}/${TEST}-tar-${TIME}.csv | awk '{print $1}'`
        HALF_NUM=$(($LINE_NUM/2))

        sed -i '1,'$HALF_NUM' s/real/result.'${TEST}', untar,/'  ${resultdir}/$TEST-tar-${TIME}.csv
        sed -i ''$HALF_NUM','$LINE_NUM' s/real/result.'${TEST}', tar,/' ${resultdir}/$TEST-tar-${TIME}.csv
    done
done         

