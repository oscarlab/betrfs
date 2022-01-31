#!/bin/bash

sudo losetup /dev/loop0 dummy.dev
sudo mount -t ftfs /dev/loop0 mnt
