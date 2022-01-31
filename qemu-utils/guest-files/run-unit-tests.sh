#!/bin/bash

modprobe zlib

cd ft-index-testing; git pull || { echo 'git pull failed\n'; shutdown -h now; }; . shuffle.bool; cd -;
cp ft-index-testing/tests-to-run.list ft-index/ftfs/userspace-testing/todays-tests || { echo 'failed to copy test list'; shutdown -h now; };

cd ft-index; git pull || { echo 'git pull failed\n'; shutdown -h now; };  cd -;
cd ft-index/build; ./cmake-ft.sh || { echo 'building ft code failed\n'; shutdown -h now; }; cd -;
cd ft-index/ftfs; make || { echo 'building ftfs module failed\n'; shutdown -h now; }; cd -;

echo "starting tests for today $(/bin/date)"

cd ft-index/ftfs/userspace-testing; python run-tests.py todays-tests ${SHUFFLE_TESTS}

echo "done tests for today $(/bin/date)"

shutdown -h now
