#!/bin/bash

set -eu

NUM_FILES=3000000
NUM_THREADS=4

source ../../fs-info.sh

SEQ=$(seq ${tokubench_num})
make

for x in ${!allfs[*]}; do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}
    for i in $SEQ; do
        sudo ../../cleanup-fs.sh
        sudo ../../setup-${FS}.sh
        ./run-benchmark-fs-threaded-param.sh $NUM_THREADS $NUM_FILES $TEST
    done
done

