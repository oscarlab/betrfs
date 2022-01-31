#!/bin/bash

. ../../fs-info.sh

for x in ${!allfs[*]}
do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}

    for i in 1 2 3 4 5
    do
        sudo ../../cleanup-fs.sh
        sudo ../../setup-${FS}.sh
        TIME=`date +"%d-%m-%Y-%H-%M-%S"`
        cmd="./run-git-clone.sh"
        echo $cmd
        (
             $cmd 
        ) &>  $resultdir/$TEST-git-clone-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi
        sleep 1

        sed -i 's/real/result.'${TEST}', git.clone,/'  $resultdir/$TEST-git-clone-${TIME}.csv

        cmd="./run-git-diff.sh"
        echo $cmd
        (
             $cmd 
        ) &> $resultdir/$TEST-git-diff-${TIME}.csv

        if [ $? != 0 ] ; then
             echo "got error $?"
             exit 1
        fi
        sleep 1

        sed -i 's/real/result.'${TEST}', git.diff,/'  $resultdir/$TEST-git-diff-${TIME}.csv

    done
done         

