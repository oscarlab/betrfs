#!/bin/bash

mkdir test5
cd test5
cp ../Vagrantfile.testing Vagrantfile
touch console.out
sudo chown libvirt-qemu console.out

vagrant up > /dev/null
vagrant ssh -c 'cd /oscar/betrfs/ftfs/userspace-testing/; awk '\''NR % 5 == 0'\'' /oscar/betrfs/ftfs/userspace-testing/all.tests > test5.tests; sudo ./run-tests.py test5.tests'
exit $?
