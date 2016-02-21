#!/bin/bash
set -x
FT_HOMEDIR=/home/ftfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh
#. $FT_HOMEDIR/benchmarks/.rootcheck
#. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
(cd $FT_HOMEDIR/benchmarks/; sudo ./clear-fs-caches.sh)
time rm -fr $mntpnt/linux-3.11.10>/dev/null 2>&1
