. fs-info.sh

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

large_file_bench=(seq-write seq-read rand-read-4 rand-read-4096 rand-write-4 rand-write-4096)

for x in ${!alltest[*]}; do
    for b in ${!large_file_bench[*]}; do
        curr_bench=${large_file_bench[${b}]}
        TEST=${alltest[${x}]}
        grep "result."  results/${TEST}-${curr_bench}-*.csv  | awk '{print $5}' | ministat -n -w 74 | tail -n 1 | sed   's/x/'${TEST}.${curr_bench}'/g'
    done
done
