#!/bin/bash
set -e

source config

#The xfstests environment is currently setup on glittering-example
#under /var/lib/fstests

#1. Setting up the framework

#a. Check to proceed only if the script is being run for the first time OR there have been commits since the last run
git remote update
UPSTREAM=${1:-'@{u}'}
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse "$UPSTREAM")
if [[ ($LOCAL = $REMOTE) && (-e $FSTESTS_DIR/kvm-xfstests/summary.log) ]]; then
	echo "Repository Up-to-date since last run. Exiting."
	exit
fi

#b. Pull the latest code and build the kernel
cp $KERNEL_CONFIG ../../.config
git pull; cd ../../; make -j16; make modules -j4; cd -;

#c. Build the filesystem
#Make sure the kernel source in the Makefile (defined in ft-index/filesystem/Makefile) points to the above kernel
cd ../../../; mkdir -p build; cp linux-3.11.10/qemu-utils/cmake-ft.sh build/.; cd -; 
cd ../../../build; ./cmake-ft.sh; cd -;
cd ../../../filesystem; make; cd -;

#d. copy the precompiled test appliance image
#This image contains the code for the xfstests and their precomiled binaries
# If the xfstests directory doesn't already exist, clone it:
if [ ! -d $FSTESTS_DIR ]; then
	git clone git://git.kernel.org/pub/scm/fs/ext2/xfstests-bld.git $FSTESTS_DIR;
fi

if [ ! -e $FSTESTS_DIR/kvm-xfstests/test-appliance/root_fs.img ]; then
    wget -O $FSTESTS_DIR/kvm-xfstests/test-appliance/root_fs.img https://www.dropbox.com/s/0vnzsi0hi6ybykm/root_fs.img?dl=1
fi

#e. install the modules on the guest root filesystem and copy ftfs.ko to root_fs.img
echo Setting up root image...
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/"
mkdir -p mnt
sudo modprobe nbd max_part=8
sudo qemu-nbd --connect=/dev/nbd0 root_fs.img
sudo mount /dev/nbd0 mnt/
cd -

echo Installing modules
cd ../../
sudo make INSTALL_MOD_PATH="$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt" modules_install
cd -

echo Installing ftfs.ko
sudo cp ../../../filesystem/ftfs.ko "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/root/"

echo Cleaning up
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/" 
sudo umount mnt
sudo qemu-nbd --disconnect /dev/nbd0
cd -

#f. replace the config under fstests/kvm-xfstests
cp config "$FSTESTS_DIR/kvm-xfstests/"

#g. setup the disk images to be tested
cd "$FSTESTS_DIR/kvm-xfstests/"
rm -rf disks/
./setup
cd -

#(setup vdb to be mounted as betrfs)
cd "$FSTESTS_DIR/kvm-xfstests/disks"
mkdir -p mnt
sudo mount -t ext4 vdb mnt/
cd -

cd "$FSTESTS_DIR/kvm-xfstests/disks/mnt"
sudo rm -rf *;
sudo mkdir db;
sudo mkdir dev;
sudo touch dev/null;
sudo mkdir tmp;
sudo chmod 1777 tmp;
cd -;

cd "$FSTESTS_DIR/kvm-xfstests/disks"
sudo umount mnt
cd -;

#h. copy a few scripts from the ft-index repository
cp kvm-xfstests "$FSTESTS_DIR/kvm-xfstests/"
cp ../email-results.py "$FSTESTS_DIR/kvm-xfstests/"
cp ../../../unit_tests_email "$FSTESTS_DIR/kvm-xfstests/xfstests_email"

#i. Seed ftfs test files from ext4
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/files/root/fs"
cp -rf ext4 ftfs
cd -

#2. Run the xfstests

cd "$FSTESTS_DIR/kvm-xfstests/"
TESTS_TO_RUN=$(echo ${TESTS_TO_RUN} | sed -e 's/#generic\/[0-9]*,//g')
sudo ./kvm-xfstests -c 4k $TESTS_TO_RUN
cd -

#3. Important References

#https://docs.google.com/presentation/d/14MKWxzEDZ-JwNh0zNUvMbQa5ZyArZFdblTcF5fUa7Ss/edit#slide=id.p
#https://github.com/tytso/xfstests-bld/blob/master/Documentation/kvm-quickstart.md
#https://www.kumari.net/index.php/system-adminstration/49-mounting-a-qemu-image

#4. Points to note
#a. All the xfstests code is located under /root in root_fs.img which is a qemu-image.
#b. All the changes I have done in the code assume that "vdb" is the primary disk image mounted using betrfs and loop device "loop0"
