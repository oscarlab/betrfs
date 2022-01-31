#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

#pass tests to be run as a parameter

if [ $# -ne 2 ]; then
	echo "Pass Tests to be run in a file as arg1 and '--force-hdd' or '--force-ssd' as arg2"
	exit 1
fi

if [ "$2" != "--force-hdd" -a "$2" != "--force-ssd" ]; then
	echo "The second argument is invalid"
	exit 1
fi

set -x
#do the req setup.
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"

cd ../benchmarks
sudo rm -rf results
mkdir results
main_dir=$FT_HOMEDIR/benchmarks

while read -r line || [[ -n "$line" ]]; do
	if [[ ${line:0:1} == "#" ]]; then
		continue
	fi
	if [[ ${line:0:1} == "." ]]; then
		$line $2
		cd $main_dir
	else
		sudo -E ./cleanup-fs.sh $2
		sudo -E ./setup-ftfs.sh $2
		cd $line
	fi

done < $1
