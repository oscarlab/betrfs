#!/bin/bash

. ../../../fs-info.sh

for x in ${!allfs[*]}
do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}

    for i in 1 2 3 #4 5
    do
        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./run-test.sh $FS"
        echo $cmd
        (
             $cmd 
        ) &> $resultdir/$TEST-mailserver-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi
        sleep 1

        sed -i '$ s/^/result.'${TEST}', /'  $resultdir/$TEST-mailserver-${TIME}.csv
    done
done
