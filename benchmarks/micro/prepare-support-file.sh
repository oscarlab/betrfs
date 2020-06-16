#!/bin/bash
set -x
set -e

. $FT_HOMEDIR/benchmarks/fs-info.sh || exit 1

DIR=`pwd`

cd $FT_HOMEDIR/benchmarks/support-files || exit 1

#### Untar the source ####
if [ -d linux-3.11.10 ]
then :
else
    tar -xf linux-3.11.10.tar.xz || exit 1
fi

#### Cp to benchmark directory ####

# "big" parameter specifies that we want a larger directory,
#   we use several source trees
if [ $# -eq 1 ] && [ $1 = "--big" ]
then :
    for I in {1..2}; do
        if [ -d $mntpnt/big-linux/$I/linux-3.11.10 ]
        then :
        else
            echo "copy dir to $mntpnt/big-linux/$I"
            mkdir -p $mntpnt/big-linux/$I || exit 1
            cp -r linux-3.11.10 $mntpnt/big-linux/$I || exit 1
        fi
    done
else
    if [ -d $mntpnt/linux-3.11.10 ]
    then :
    else
        echo "copy dir to $mntpnt"
        cp -r linux-3.11.10 $mntpnt || exit 1
    fi
fi

#### Go to previous directory ####
cd $DIR
