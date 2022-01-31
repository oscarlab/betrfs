#! /bin/bash
set -e
set -x

# This script is provided as a one step build script for the betrfs project.
# It will build the ft code, as well as sfs and both ftfs modules.
# It accepts one optional flag, '-d', which makes it compile everything
# in debug mode. It otherwise builds in benchmarking/release mode by default.

MODE=""

if [ "$1" = "-d" ] ; then
	MODE="debug"
	echo "building debug version"
elif [ "$1" = "-c" ] ; then
	MODE="bench"
	echo "clean build release version"
	rm -rf build
elif [ "$1" ] ; then
	echo "-d or -c is the only paramter accepted by this script"
	exit
else
	MODE="bench"
	echo "building benchmarking/release version"
fi

mkdir -p build

cp qemu-utils/cmake-ft-$MODE.sh build/.
cd build
./cmake-ft-$MODE.sh
cd -

cd ftfs
make -j$(nproc --all) $MODE
cd -
cd filesystem
make -j$(nproc --all) $MODE
cd -
cd simplefs
make -j$(nproc --all)
cd -
