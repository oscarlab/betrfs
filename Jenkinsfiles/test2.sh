#!/bin/bash

mkdir test2
cd test2
cp ../Vagrantfile.testing Vagrantfile
touch console.out
sudo chown libvirt-qemu console.out

vagrant up > /dev/null
vagrant ssh -c 'cd /oscar/betrfs/ftfs/userspace-testing/; awk '\''NR % 5 == 2'\'' /oscar/betrfs/ftfs/userspace-testing/all.tests > test2.tests; sudo ./run-tests.py test2.tests --sfs'
result=$?

vagrant destroy -f
cat console.out

exit ${result}
