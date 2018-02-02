#!/bin/bash

FT_HOMEDIR=/home/ftfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh
#. $FT_HOMEDIR/benchmarks/.rootcheck
if [ -d $mntpnt/linux-3.11.10 ]; then
:
else
. $FT_HOMEDIR/benchmarks/micro/prepare-support-file.sh
fi
(cd $FT_HOMEDIR/benchmarks/; sudo ./clear-fs-caches.sh)
if [ "$1" = "" ]
then
keyword=cpu_to_be64
else
keyword=$1
fi
time grep -r $keyword $mntpnt/linux-3.11.10>/dev/null 2>&1
set +x
index=0
declare -a stack=()
function recursive_traverse() {
	declare -a stack=("${!2}")
	for file in $1/*
	do
    		if [ -d "${file}" ] ; then
			stack[$index]="${file}"
			index=$((index + 1))
			if [ "$index" -eq 5 ] ; then
				for f in ${stack[$index-1]}/*
				do
					if [ -f "${f}" ] ; then
						echo $f ${stack[0]}
						mv $f ${stack[0]}
						break
					fi
				done
				index=0
			fi 
        		recursive_traverse "${file}" stack[@]
    		fi
	done
}

recursive_traverse "$mntpnt/linux-3.11.10" stack[@]
cd $FT_HOMEDIR/benchmarks/; sudo ./clear-fs-caches.sh
time grep -r $keyword $mntpnt/linux-3.11.10>/dev/null 2>&1


