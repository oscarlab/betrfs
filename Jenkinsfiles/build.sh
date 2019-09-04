#!/bin/bash

touch console.out
sudo chown libvirt-qemu console.out

vagrant up > /dev/null
vagrant ssh -c "cd /oscar/betrfs; wget https://auku.cs.unc.edu/depot/linux-source-3.11.10-ftfs_20180618.00_all.deb; sudo dpkg -i linux-source-3.11.10-ftfs_20180618.00_all.deb; tar -xf /usr/src/linux-source-3.11.10-ftfs.tar.bz2; mv linux-source-3.11.10-ftfs linux-3.11.10; mkdir -p build; cp qemu-utils/cmake-ft.sh build/; cd build; ./cmake-ft.sh; cd ../ftfs; make; rsync -vrlD /oscar/betrfs/ /vagrant/"
result=$?

vagrant destroy -f
cat console.out

exit ${result}
