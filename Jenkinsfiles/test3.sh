#!/bin/bash

mkdir test3
cd test3
cp ../Vagrantfile.testing Vagrantfile
touch console.out
sudo chown libvirt-qemu console.out

vagrant up > /dev/null
vagrant ssh -c 'cd /oscar/betrfs/ftfs/userspace-testing/; tail -n +251 /oscar/betrfs/ftfs/userspace-testing/all.tests | head -125 > test3.tests; sudo ./run-tests.py test3.tests --sfs'
result=$?

vagrant destroy -f
cat console.out

exit ${result}
