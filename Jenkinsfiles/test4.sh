#!/bin/bash

mkdir test4
cd test4
cp ../Vagrantfile.testing Vagrantfile
touch console.out
sudo chown libvirt-qemu console.out

vagrant up > /dev/null
vagrant ssh -c 'cd /oscar/betrfs/ftfs/userspace-testing/; tail -n +376 /oscar/betrfs/ftfs/userspace-testing/all.tests | head -125 > test4.tests; sudo ./run-tests.py test4.tests --sfs'
result=$?

vagrant destroy -f
cat console.out

exit ${result}
