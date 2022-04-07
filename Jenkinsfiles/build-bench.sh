#!/bin/bash

vagrant ssh -c "set -e; cd /oscar/betrfs; mkdir -p build; cp qemu-utils/cmake-ft.sh build/; cd build; sed -i 's/DEBUG/RELEASE/g' cmake-ft.sh; ./cmake-ft.sh; cd ../filesystem; make"
result=$?

exit ${result}
