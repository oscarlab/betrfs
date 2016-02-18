#!/bin/bash

. ../../fs-info.sh
. ../../.ismounted

support=$HOME/$repo/benchmarks/support-files

if [ ! -e $mntpnt/$clone_repo ]; then
    if [ ! -e $support/$clone_repo ] ; then
	cd $support; git clone git@camilla.oscar.cs.stonybrook.edu:$clone_repo.git; cd -
    fi
    cd $mntpnt; git clone $support/$clone_repo; cd -
fi

sudo ../../clear-fs-caches.sh


# first commit where file system compiles vs current commit (8/31/14)

cd $mntpnt/$clone_repo; time git diff --patch  6b93052128d62bf0e87620db78a27269eb00ccbf 637d301e7dea09f10d11b7570e86de3f06f2470c > patch

#cd $mntpnt/$repo; time git diff --quiet  6b93052128d62bf0e87620db78a27269eb00ccbf 637d301e7dea09f10d11b7570e86de3f06f2470c
