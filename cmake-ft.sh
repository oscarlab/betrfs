#!/bin/bash

set -x

#TYPE=RELWITHDEBINFO
#TYPE=DEBUG
TYPE=RELEASE
CC=gcc-7 CXX=g++-7 cmake \
     -D CMAKE_BUILD_TYPE=$TYPE \
     -D USE_BDB=OFF \
     -D BUILD_TESTING=OFF \
     -D CMAKE_INSTALL_PREFIX=../ft-install/ \
     -D BUILD_FOR_LINUX_KERNEL_MODULE=ON \
     ..

cmake --build . --target install


