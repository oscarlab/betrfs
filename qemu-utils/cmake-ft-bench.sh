#!/bin/bash

set -eux

#TYPE=RELWITHDEBINFO
TYPE=RELEASE
CC=gcc-7 CXX=g++-7 cmake \
     -D CMAKE_BUILD_TYPE=$TYPE \
     -D USE_BDB=OFF \
     -D BUILD_FOR_LINUX_KERNEL_MODULE=ON \
     ..

# The above cmake command will generate Makefiles in build/, which we can now
# directly run using make(1).
make -j$(nproc --all)
