#!/bin/bash

. ../install/install-system.sh
. ../install/install-packages.sh


for i  in 1 2 3 4 5
do
	time apt-get install -y debsums >> debsum.txt
	../../clear-fs-caches.sh
done


