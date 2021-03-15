#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

#pass tests to be run as a parameter

if [ $# -eq 0 ]
	then
		echo "Pass Tests to be run in a file as arg"
		exit 1
fi


set -x
#do the req setup.
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

. "$DIR/fs-info.sh"
. "$DIR/.hostcheck"
#. "$DIR/.rootcheck"
#. "$DIR/.mountcheck"

## Let's make a different script to build if needed; I don't think we need to build twice in CI
#cd ../build #copy cmake file to build from qemu utils in linux folder in ft-index
#./cmake-ft.sh
#cd ../ftfs
#sudo -s make clean
#sudo -s make

cd ../benchmarks

sudo rm -rf  results
mkdir results

main_dir=$FT_HOMEDIR/benchmarks

#
while read -r line || [[ -n "$line" ]]; do
    if [[ ${line:0:1} == "#" ]]; then
        continue
	fi
	if [[ ${line:0:1} == "." ]]; then
        /bin/bash $line
        cd $main_dir
    else
        yes | sudo -E /bin/bash ./cleanup-fs.sh
        sudo -E ./setup-ftfs.sh
        cd $line
    fi

done < $1

echo "The ceiling is the roof"
