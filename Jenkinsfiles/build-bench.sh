#!/bin/bash

touch console.out
sudo chown libvirt-qemu console.out

vagrant up > /dev/null
vagrant ssh -c "set -e; cd /oscar/betrfs; wget https://auku.cs.unc.edu/depot/linux-source-3.11.10-ftfs_20180618.00_all.deb; sudo dpkg -i linux-source-3.11.10-ftfs_20180618.00_all.deb; tar -xf /usr/src/linux-source-3.11.10-ftfs.tar.bz2; mv linux-source-3.11.10-ftfs linux-3.11.10; cd linux-3.11.10; cp ../qemu-utils/jenkins.config.3.11.10 .config; make oldconfig; make -j 4; cd ..; mkdir -p build; cp qemu-utils/cmake-ft.sh build/; cd build; sed -i 's/DEBUG/RELEASE/g' cmake-ft.sh; ./cmake-ft.sh; cd ../filesystem; make"
result=$?

vagrant destroy -f
cat console.out

exit ${result}
