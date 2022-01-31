#!/bin/bash

source fs-info.sh

echo -e "Name                   N          Min           Max        Median           Avg        Stddev"

for x in ${!alltest[*]}; do
    TEST=${alltest[${x}]}
    grep "result."  results/${TEST}-threaded-*.csv  | awk '{print $5}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}'.tokubench/g'
done



dir_ops_bench=(grep find del)

for x in ${!alltest[*]}; do
    for b in ${!dir_ops_bench[*]}; do
        curr_bench=${dir_ops_bench[${b}]}
        TEST=${alltest[${x}]}
        grep "result."  results/${TEST}-${curr_bench}-*.csv  | awk '{print $3}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}'/g'
    done
done

large_file_bench=(seq-write10g seq-read10g seq-write80g seq-read80g rand-read-4 rand-read-4096 rand-write-4 rand-write-4096)

for x in ${!alltest[*]}; do
    for b in ${!large_file_bench[*]}; do
        curr_bench=${large_file_bench[${b}]}
        TEST=${alltest[${x}]}
        grep "result."  results/${TEST}-${curr_bench}-*.csv  | awk '{print $5}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}'/g'
    done
done


for x in ${!alltest[*]}; do
        curr_bench=tar
        TEST=${alltest[${x}]}
        grep "result."  results/${TEST}-${curr_bench}-*.csv | grep -v untar  | awk '{print $3}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}'/g'
        grep "result."  results/${TEST}-${curr_bench}-*.csv | grep  untar    | awk '{print $3}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.un${curr_bench}'/g'
done

for x in ${!alltest[*]}; do
        curr_bench=rsync
        TEST=${alltest[${x}]}
        grep "result."  results/${TEST}-${curr_bench}-*.csv | grep -v inplace  | awk '{print $5}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}'/g'
        grep "result."  results/${TEST}-${curr_bench}-*.csv | grep  inplace    | awk '{print $5}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}-inplace'/g'
done

for x in ${!alltest[*]}; do
        curr_bench=git
        TEST=${alltest[${x}]}
        grep "result."  results/${TEST}-${curr_bench}-*.csv | grep -v dir |  grep  "clone"  | awk '{print $3}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}-clone'/g'
        grep "result."  results/${TEST}-${curr_bench}-*.csv | grep -v dir | grep  "diff"    | awk '{print $3}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}-diff'/g'
done

for x in ${!alltest[*]}; do
        curr_bench=mailserver
        TEST=${alltest[${x}]}
        grep -a "result."  results/${TEST}-${curr_bench}-*.csv | grep  "imap-publish"  | awk '{print 80000/$3}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}'/g'
done

for x in ${!alltest[*]}; do
	workloads=(oltp fileserver webproxy webserver)
	for workload in "${workloads[@]}"; do
		TEST=${alltest[${x}]}
		grep 'IO Summary' results/${TEST}-${workload}-*.csv | awk '{print $6}' | ministat -n | tail -n 1 | sed 's/x/'${TEST}.${workload}-ops'/g'
	done
done
