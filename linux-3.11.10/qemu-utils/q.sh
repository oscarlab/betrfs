#!/bin/bash

[ -f console.log ] && mv console.log console-last.log
[ -f stdout.log ] && mv stdout.log stdout-last.log

set -x

kvm -smp 2 -cpu host -m 2G \
    -kernel ../arch/x86/boot/bzImage \
    -drive file=./rootfs.raw,if=virtio \
    -drive file=./device.raw,if=virtio \
    -append "serial=tty1 console=ttyS0 root=/dev/vda rw --no-log" \
    -curses -s -serial file:console.log \
    -enable-kvm \
    -net nic,model=virtio,macaddr=52:54:00:12:34:56 \
    -net user,hostfwd=tcp:127.0.0.1:8448-:22
