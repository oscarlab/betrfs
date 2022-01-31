#!/bin/bash
set -e

source config
source common

# This script builds an image for xfstests, based on the variables set in config
# The output file is named <xfstests-commit>-<xfstest-bld-commit>.img
# This is primarily tested on Ubuntu 18.04, but, in principle, should work on newer versions

# Only build if the image does not already exist
if [[ -f $FSTESTS_DIR/kvm-xfstests/test-appliance/$IMG_NAME ]]; then
    echo "$IMG_NAME already exists.  Delete if you want to re-create."
    exit 1
fi

#d. copy the precompiled test appliance image
#This image contains the code for the xfstests and their precomiled binaries
# If the xfstests directory doesn't already exist, clone it:
clone_xfstests

# Set up a custom image configuration
# Use the sed if they ever start putting this in the config by default
# sed "s/XFSTESTS_COMMIT=.*/XFSTESTS_COMMIT=${XFSTESTS_COMMIT}/g" ${FSTESTS_DIR}/config > ${FSTESTS_DIR}/config.custom
# And set the FIO version
sed "s/FIO_COMMIT=.*/FIO_COMMIT=${FIO_COMMIT}/g" ${FSTESTS_DIR}/config > ${FSTESTS_DIR}/config.custom
echo "XFSTESTS_COMMIT=${XFSTESTS_COMMIT}" >> ${FSTESTS_DIR}/config.custom

# Add the kmod package to our image
echo "gen_image_args+=\"-a 'kmod'\"" >> ${FSTESTS_DIR}/config.custom

## Build the root image
cd ${FSTESTS_DIR}

# Make the build chroot if it is missing; this should probably not change
# from version to version, but may wish to blow it away if one has any doubts
if [ ! -e /chroots/stretch-amd64 ]; then
    echo "Setting up build environment"
    sudo ./setup-buildchroot --arch=amd64 --noninteractive
fi
echo "Doing all the things"
./do-all --chroot=stretch-amd64
cd -

# Mount the root image
echo Configuring root image...
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/"
# Rename root_fs.img
mv root_fs.img $IMG_NAME
mkdir -p mnt
sudo modprobe nbd max_part=8
sudo qemu-nbd --connect=/dev/nbd0 $IMG_NAME
sudo mount /dev/nbd0 mnt/
cd -

# Put ftfs if /etc/modules, so that the test appliance loads it at boot
echo "ftfs" | sudo tee -a "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/etc/modules"

# Set up the dummy device that ftfs requires (for now)
sudo touch $FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/dev/loop0
sudo touch $FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/dummy.dev
echo "losetup /dummy.dev /dev/loop0" | sudo tee -a "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/etc/rc.local"

cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/"
sudo umount mnt
sudo qemu-nbd --disconnect /dev/nbd0
cd -
