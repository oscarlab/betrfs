#!/bin/bash

. ../../fs-info.sh

time debootstrap --arch amd64 precise $mntpnt
