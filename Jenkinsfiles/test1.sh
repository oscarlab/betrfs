#!/bin/bash

mkdir test1
cd test1
cp ../Vagrantfile.testing Vagrantfile
touch console.out
sudo chown libvirt-qemu console.out

vagrant up
vagrant ssh -c 'cd /oscar/betrfs/ftfs/userspace-testing/; head -125 /oscar/betrfs/ftfs/userspace-testing/all.tests > test1.tests; sudo ./run-tests.py test1.tests --sfs'
result=$?

vagrant destroy -f
cat console.out

exit ${result}
