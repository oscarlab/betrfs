#!/bin/bash

set -eu
set -o pipefail

source ../../fs-info.sh

workloads=(
	oltp
	fileserver
	webproxy
	webserver
)

for workload in ${workloads[@]}; do
	for x in ${!allfs[*]}; do
		FS=${allfs[${x}]}
		TEST=${alltest[${x}]}

		# our filebench experiments take 60 seconds, so run at least 5 times.
		for i in $(seq 5); do
			TIME=$(date +"%d-%m-%Y-%H-%M-%S")
			cmd="sudo ./run-test.sh $workload $FS"
			echo $cmd
			$cmd |& tee $resultdir/$TEST-${workload}-${TIME}.csv
			RET=$?

			if [ $RET -ne 0 ]; then
				 echo "got error $RET"
				 exit 1
			fi
			sleep 1

			# handle post processing in postprocess.sh
		done
	done
done
