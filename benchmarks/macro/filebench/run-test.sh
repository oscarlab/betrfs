#!/bin/bash
# Usage: sudo ./run-test.sh workload FS [--force-ssd | --force-hdd]
# This script needs to be run as root so ASLR can be enabled and disabled.
set -eu

print_usage() {
	echo "Usage: ./run-test.sh workload FS [--force-ssd | --force-hdd]"
	echo $@
	exit 1
}

# check command line arguments
if [ $# -ne 2 ]; then
	[ $# -eq 3 ] || print_usage
	[[ $3 =~ --force-(hdd|ssd) ]] || print_usage "$3 is not a valid force parameter."
	FORCE=$3
else
	# set FORCE so set -e can be used
	FORCE=
fi

WORKLOAD=$1
FS=$2

[ -e ${WORKLOAD}.f ] || print_usage "${WORKLOAD}.f does not exist."
[ -e ../../setup-${FS}.sh ] || print_usage "Setup script missing."
source ../../.rootcheck

# setup and teardown in each round
../../cleanup-fs.sh
if ../../clear-fs-caches.sh $FORCE; then
	echo "SANITY CHECK: calling ../../clear-fs-caches.sh should have failed since no FS is mounted."
	exit 1
fi
../../setup-${FS}.sh $FORCE

# filebench has problems with address space layout randomization, so disable it
# https://github.com/filebench/filebench/issues/112#issuecomment-327864993
old_value=$(cat /proc/sys/kernel/randomize_va_space)
echo 0 > /proc/sys/kernel/randomize_va_space

#### Check if filebench exist or not
if ! command -v filebench &> /dev/null; then
	./install.sh
fi

filebench -f ${1}.f

# restore old random_vs_space value
echo $old_value > /proc/sys/kernel/randomize_va_space
