#!/bin/bash


set -x

#TYPE=RELWITHDEBINFO
TYPE=RELEASE
CC=gcc CXX=g++ cmake \
     -D CMAKE_BUILD_TYPE=$TYPE \
     -D USE_BDB=OFF \
     -D USE_TDB=ON \
     -D BUILD_TESTING=OFF \
     -D CMAKE_INSTALL_PREFIX=../ft-install/ \
     -D BUILD_FOR_LINUX_KERNEL_MODULE=ON \
     ..

cmake --build . --target install


