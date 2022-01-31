#! /bin/bash
set -e
set -x

# This script is provided as a one step build script for the betrfs project.
# It will build the ft code, as well as sfs and both ftfs modules.

mkdir -p build
mkdir -p ft-install

cp cmake-ft.sh build/.
cd build
./cmake-ft.sh
cd -

cd filesystem
make $MODE
cd -
