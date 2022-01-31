#! /bin/bash

sudo ./clean-mount.sh
sudo ./run-large-file-write.sh
sudo chmod a+w /mnt/benchmark/large.file
sudo ../../clear-fs-caches.sh
time rm /mnt/benchmark/large.file
