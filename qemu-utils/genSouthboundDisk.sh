#!/bin/bash
set -eux

# generate southbound disk image, ie. the disk image on which BetrFS will be
# mounted in the VM.
sbDisk=ftfs-southbound.raw
echo "generating 10G .raw disk image for southbound: $sbDisk (ext4)"
qemu-img create -f raw $sbDisk 10G
mkfs.ext4 -F $sbDisk
