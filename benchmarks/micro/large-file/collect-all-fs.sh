#!/bin/bash

set -eu

source ../../fs-info.sh
make

for x in ${!allfs[*]}; do
    FS=${allfs[${x}]}
    TEST=${alltest[${x}]}

    rm -rf ${TEST}-*.csv
    ./test.sh $FS $TEST
done
