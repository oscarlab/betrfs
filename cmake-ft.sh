#!/bin/bash

## This script will compile the ft-index code using cmake.
## If you are not an expert on cmake, here is some advice:
##   do an out-of-source build.
##
##  mkdir build
##  cp cmake-ft.sh build/.
##  cd build
##  ./cmake-ft.sh
##

set -x

TYPE=RELEASE
#TYPE=RELWITHDEBINFO
#TYPE=DEBUG
CC=gcc-4.7 CXX=g++-4.7 cmake \
     -D CMAKE_BUILD_TYPE=$TYPE \
     -D USE_BDB=OFF \
     -D BUILD_TESTING=OFF \
     -D CMAKE_INSTALL_PREFIX=../ft-install/ \
     -D BUILD_FOR_LINUX_KERNEL_MODULE=ON \
     ..

cmake --build . --target install