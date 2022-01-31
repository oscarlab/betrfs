#!/bin/bash

set -eux

sudo apt install -y bison flex
wget https://github.com/filebench/filebench/releases/download/1.5-alpha3/filebench-1.5-alpha3.tar.gz
tar zxvf filebench-1.5-alpha3.tar.gz

cd filebench-1.5-alpha3
./configure
make
sudo make install
cd -

rm -rf filebench-1.5-alpha3.tar.gz filebench-1.5-alpha3
