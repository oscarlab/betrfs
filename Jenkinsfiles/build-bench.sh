#!/bin/bash

vagrant ssh -c "set -e; cd /oscar/betrfs; wget https://auku.cs.unc.edu/depot/linux-source-3.11.10-ftfs_20180618.00_all.deb; sudo dpkg -i linux-source-3.11.10-ftfs_20180618.00_all.deb; tar -xf /usr/src/linux-source-3.11.10-ftfs.tar.bz2; mv linux-source-3.11.10-ftfs linux-3.11.10; mkdir -p build; cp qemu-utils/cmake-ft.sh build/; cd build; sed -i 's/DEBUG/RELEASE/g' cmake-ft.sh; ./cmake-ft.sh; cd ../filesystem; make"
result=$?

exit ${result}
