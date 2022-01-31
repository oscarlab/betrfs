#!/bin/bash

set -x
mkdir -p mnt
sudo mount -o loop ftfs-southbound.raw mnt
cd mnt; sudo rm -rf *; mkdir db; mkdir dev; touch dev/null; mkdir tmp; chmod 1777 tmp; cd ..;
sleep 1
sudo umount mnt
