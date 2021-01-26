#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -ex

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

cd ../benchmarks

sudo rm -rf  results
mkdir results

main_dir=${FT_HOMEDIR}/benchmarks

# This loop reads the input file (e.g., perf_tests) and
# executes each test
while read -r line || [[ -n "$line" ]]; do
    if [[ ${line:0:1} == "#" ]]; then
        # ignore comments
        continue
    elif [[ ${line:0:1} == "." ]]; then
        # Run the script, assuming it starts with a dot
        /bin/bash $line
        cd $main_dir
    else
        # Do the cleanup since the last test, and reset the fs state
        yes | sudo -E /bin/bash ./cleanup-fs.sh
        sudo -E ./setup-ftfs.sh
        # Change to the working directory for the next test
        cd $line
    fi

done < $1

# Final cleanup
yes | sudo -E /bin/bash ./cleanup-fs.sh

# Famous Michael Jordan quote we don't expect to see otherwise,
# Jenkins looks for this to check for completion (vs. dying in cleanup)
echo "The ceiling is the roof"
