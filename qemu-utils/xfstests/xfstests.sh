#!/bin/bash
set -ex

# see Jenkinsfiles/xfstests.Dockerfile for dependencies

## Check argument count
if [ $# != 1 ] && [ $# != 2 ]; then
	echo "Need one or two arguments: [--force-hdd | --force-ssd] or kernel_version!"
	echo "Example: xfstests.sh --force-hdd 4.19.99"
	exit 1
fi

## Set KERNEL_VERSION if there is a second argument
if [[ $# -eq 2 ]]; then
	KERNEL_VERSION=$2
fi

# source config only after setting KERNEL_VERSION since config uses that variable
source config
source common

# check for --force-{hdd,ssd} only after sourcing config since MNTOPTS is defined in config
## Make sure the first argument is what is expected
if [ $1 == "--force-hdd" ]; then
	MNTOPTS="$MNTOPTS,is_rotational=1"
elif [ $1 == "--force-ssd" ]; then
	MNTOPTS="$MNTOPTS,is_rotational=0"
else
	echo "The argument should be --force-hdd or --force-ssd!"
	exit 1
fi


#1. Setting up the framework

#a. Check to proceed only if the script is being run for the first time OR there have been commits since the last run

# Download the kernel from kernel.org if not present elsewhere on the system
if [[ ! -d ${LINUX_SOURCE} ]]; then
	KERNEL_DOWNLOADED=true
	mkdir ${LINUX_SOURCE}
	wget --no-clobber --no-verbose https://cdn.kernel.org/pub/linux/kernel/v${MAJOR_KERNEL_VERSION}.x/linux-${KERNEL_VERSION}.tar.xz
	tar -xf linux-${KERNEL_VERSION}.tar.xz -C ${LINUX_SOURCE}/..
fi

if [[ $HOST != "betrfs-dev"* || ${KERNEL_DOWNLOADED} = true ]]; then
	#b. Pull the latest code and build the kernel
	if [[ -f $KERNEL_CONFIG ]]; then
		cp $KERNEL_CONFIG ${LINUX_SOURCE}/.config
	else
		cp kernel-3.11.10.config ${LINUX_SOURCE}/.config
	fi
	cd ${LINUX_SOURCE}; make olddefconfig; make -j16; make modules -j4; cd -;
else
	## Jenkins with pre-built kernel: do nothing.
	# Jenkinsfiles/xfstests-hdd-4.19.Jenkinsfile handles this case.
	: # this is a no-op
fi

#c. Build the filesystem
#Make sure the kernel source in the Makefile (defined in ft-index/filesystem/Makefile) points to the above kernel
cd ../../; mkdir -p build; cp qemu-utils/cmake-ft-debug.sh build/.; cd -;
cd ../../build; ./cmake-ft-debug.sh; cd -;
cd ../../filesystem; make MOD_KERN_SOURCE=$LINUX_SOURCE debug; cd -;
cd ../../simplefs; make MOD_KERN_SOURCE=$LINUX_SOURCE; cd -;


#d. Download or build the image.
echo Testing whether $IMG_NAME is present
if [ ! -e $FSTESTS_DIR/kvm-xfstests/test-appliance/$IMG_NAME ]; then
	# First, try to download a pre-build.

	# We still need to set up the xfstest environment outside the image for scripts
	#  (No need to compile anything if we use a prebuilt image)
	clone_xfstests

	echo Trying to download $IMG_NAME from Leeroy Jenkins II
	# Don't die if wget fails
	set +e
	wget --no-verbose -O $FSTESTS_DIR/kvm-xfstests/test-appliance/$IMG_NAME https://leeroy2.cs.unc.edu/images/$IMG_NAME
	set -e
	# If that failed, it will leave an empty file.  rebuild it locally
	if [ ! -s $FSTESTS_DIR/kvm-xfstests/test-appliance/$IMG_NAME ]; then
		rm -f $FSTESTS_DIR/kvm-xfstests/test-appliance/$IMG_NAME
		echo Rebuilding image
		# If you want to store this image for reuse, you need to manually scp it to leeroy2.cs.unc.edu:/var/www/html/images
		./build-img.sh
	fi
fi



#e. install the modules on the guest root filesystem and copy ftfs.ko to root_fs.img
echo Setting up root image...
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/"
mkdir -p mnt
qemu-img convert $IMG_NAME img.raw
sudo mount -o loop img.raw mnt
cd -

echo Installing modules
cd $LINUX_SOURCE
sudo make INSTALL_MOD_PATH="$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt" modules_install
cd -

echo Installing ftfs.ko - kernel version is $KERNEL_VERSION
sudo mkdir -p "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/lib/modules/$KERNEL_VERSION/kernel/ftfs"
sudo mkdir -p "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/lib/modules/$KERNEL_VERSION/kernel/simplefs"
sudo mkdir -p "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/usr/local/bin"

cd $FSTESTS_DIR/../
sudo cp ../../filesystem/ftfs.ko "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/lib/modules/$KERNEL_VERSION/kernel/ftfs/ftfs.ko"
sudo cp ../../simplefs/simplefs.ko "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/lib/modules/$KERNEL_VERSION/kernel/simplefs/simplefs.ko"
sudo cp ../../simplefs/mkfs-sfs "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/usr/local/bin/"

#YZJ: Leave them here for future debugging
#sudo cp /bin/kmod "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/bin/"
#sudo cp /sbin/insmod "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/sbin/"
#sudo cp /sbin/lsmod "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/sbin/"
#sudo cp /sbin/modprobe "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/sbin/"
#sudo cp /sbin/modinfo "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/sbin/"

# Put simplefs if /etc/modules, so that the test appliance loads it at boot
# XXX: Would really be nice if this were one module
# XXX: Next time we roll forward xfs versions, put this in build-img
echo "simplefs" | sudo tee -a "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/etc/modules"

sudo depmod -a -b $FSTESTS_DIR/kvm-xfstests/test-appliance/mnt $KERNEL_VERSION

# Set up the xfstests environment
# We re-copy this every time, because this could change (but probably won't)
sudo cp -r ftfs "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/root/fs"

# XXXXX: Hack - eventually rework as a patch, or figure out how to do this more cleanly
#        Note that part of rolling forward versions involves getting this file from
#        xfstest code and applying similar changes (at least until we find a better way
#        to make these changes.
#
#        The key issues:
#          * Passing in mount options to a new FS doesn't work without changes
#          * The scratch device by default is the same FS as the test device.  BetrFS only
#            supports one mount at a time, so we need to use ext4 as the scratch device
sudo cp rc "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/root/xfstests/common/"
input="$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/root/runtests.sh"
if ! sudo grep -q "echo FULL RESULTS" "$input"; then
	#sudo sed -i "$(( $(wc -l < $input ) - 1 )) iecho FULL RESULTS\necho\necho\ntail -n +1 \$RESULTS/\$FS/results-\$i/generic/*.full" "$input" # Use this line for full output if needed, different (more  																				detailed) from genericxxx.out
	sudo sed -i "$(( $(wc -l < $input ) - 1 )) iecho FULL RESULTS\necho\necho\ntail -n +1 \$RESULTS/\$FS/results-\$i/generic/*.out.bad" "$input"
fi

# XXXXXX Hack: copy in mkfs.btrfs, just for now.  Don't ask unless you want to see a grown man weep.
sudo cp /bin/*btrfs* "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/sbin/"

## The default .bashrc hangs in jenkins, because of tty resizing. :(
# We re-copy this every time, because this could change (but probably won't)
echo Installing modified .bashrc
sudo cp dot.bashrc "$FSTESTS_DIR/kvm-xfstests/test-appliance/mnt/root/.bashrc"

echo Cleaning up
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/"
sudo umount mnt
qemu-img convert img.raw $IMG_NAME
rm -f img.raw
cd -

#f. replace the config under fstests/kvm-xfstests
cp config "$FSTESTS_DIR/kvm-xfstests/config.kvm"
sed -i -e 's/LINUX_SOURCE=`realpath ../LINUX_SOURCE=`realpath ..\/..\/../g' "$FSTESTS_DIR/kvm-xfstests/config.kvm"

#g. setup the disk images to be tested
cd "$FSTESTS_DIR/kvm-xfstests/"
rm -rf disks/
./setup
cd -

#h. copy a few scripts from the ft-index repository
cp kvm-xfstests "$FSTESTS_DIR/kvm-xfstests/"

#i. Seed ftfs test files from ext4
cd "$FSTESTS_DIR/kvm-xfstests/test-appliance/files/root/fs"
cp -rf ext4 ftfs
cd -



#2. Run the xfstests

cd "$FSTESTS_DIR/kvm-xfstests/"
XFSTESTS_FLAVOR=kvm
TESTS_TO_RUN=$(echo ${TESTS_TO_RUN} | sed -e 's/#generic\/[0-9]*,//g')
sudo KERNEL_VERSION=$KERNEL_VERSION MNTOPTS="$MNTOPTS" ./kvm-xfstests -c 4k $TESTS_TO_RUN
# Propagate the return value out
exit $?


#3. Important References

#https://docs.google.com/presentation/d/14MKWxzEDZ-JwNh0zNUvMbQa5ZyArZFdblTcF5fUa7Ss/edit#slide=id.p
#https://github.com/tytso/xfstests-bld/blob/master/Documentation/kvm-quickstart.md
#https://www.kumari.net/index.php/system-adminstration/49-mounting-a-qemu-image

#4. Points to note
#a. All the xfstests code is located under /root in root_fs.img which is a qemu-image.
#b. All the changes I have done in the code assume that "vdb" is the primary disk image mounted using betrfs and loop device "loop0"
