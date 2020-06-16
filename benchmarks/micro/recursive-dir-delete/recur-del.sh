#!/bin/bash

# DEP: Since this is used in CI now, quit if a command fails and propagate out the error
set -e

. ../../fs-info.sh

set -x
. $FT_HOMEDIR/benchmarks/fs-info.sh
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh --big
sudo -E ../../clear-fs-caches.sh

/usr/bin/time -p rm -fr $mntpnt/big-linux > /dev/null
