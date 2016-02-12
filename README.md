General BetrFS Design
---------------------

The BetrFS prototype (*) fits into the Linux storage stack as follows:

    ________________
    |              |
    |      VFS     |
    |______________|
    ________________
    |              |
    |    BetrFS *  |
    |______________|
    ________________
    |              |
    |   B^e Tree * |
    |______________|
    ________________
    |              |
    |     ext4     |
    |______________|


Like any other file system, BetrFS is registered with the VFS as a
file system during module load. But BetrFS has a stacked file system
design. When you mount BetrFS, it loads a B^e-tree index from a
separate kernel file system (the "southbound" file system, here
ext4). For this reason, you must specify (at BetrFS mount time) the
device and file system type where the B^e-tree index image
resides. This will be explained in detail in the 'Mounting BetrFS'
section below.


Repository layout
-----------------

BetrFS code is contained in the filesystem/ directory. Its primarily
role is to implement the BetrFS schema and convert VFS operations into
B^e-tree operations. The BetrFS kernel module code can be found in
filesystem/ftfs_module.c, and the primary files that deal with the
VFS->B^e-tree mapping are filesystem/ftfs_super.c and
filesystem/ftfs_bstore.c

The B^e-tree implementation in BetrFS is from the open-source fractal
tree index provided by TokuTek (TokuDB). There have been slight
modifications to TokuDB in order for the kernel port to be successful,
but the TokuDB code has remained largely unchanged. The ft/, src/,
portability/, util(s)/, include/, locktree/, and cmake_modules/
directories contain most of the TokuDB code and configuration files.

To port the B^e-tree to the kernel, we reimplemented the userspace
libraries used by TokuDB that were not compatible with the
kernel. These can be found in the filesystem/ directory
```
  ftfs_pthreads.*
  ftfs_files.*
  ftfs_stat.*
  ftfs_stdio.*
  ftfs_error.*
  ftfs_assert.*
  ftfs_compress.*
  ftfs_random.*
  toku_linkage.c
  ...
```
We import TokuDB as a binary blob, and overwrite TokuDB symbols using
symbols from these files.

The linux-3.11.10 directory contains the kernel required to run
BetrFS. A modified kernel is required for a few reasons:
  1. At the time of writing BetrFS, the kernel did not support direct
  IO from non-userspace buffers.
  2. TokuDB relies on errno for error handling. Instead of modifying
  all of the TokuDB code to explicitly pass error numbers, we
  augmented the Linux 'struct task_struct' with an error number
  field. This field is unused by code outside of BetrFS.
  3. To interface with ext4, we maintain a private file table and file
  system namespace for TokuDB files. We had to export a few functions
  that were already in the kernel, but unavailable for our uses.

The ftfs/ directory contains a simple module that can be used to run
all of the TokuDB regression tests within the kernel. It is not
necessary to run BetrFS, but can be useful for testing enhancements to
the data structures.


Compiling the code
------------------

Apply the provided patch (linux-3.11.10.diff) to the 3.11.10 Linux kernel
available from https://www.kernel.org/pub/linux/kernel/v3.x/linux-3.11.10.tar.bz2.
There are many guides on how to do this, so please read one if you have
never compiled your own kernel. An abbreviated version;

	```
    wget https://www.kernel.org/pub/linux/kernel/v3.x/linux-3.11.10.tar.gz
	  tar -xvf linux-3.11.10.tar.gz
	  cd linux-3.11.10
	  patch < ../linux-3.11.10.diff
      make oldconfig
      make
      make modules
      make modules_install
      make install
  ```

The next step is to build TokuDB. TokuDB uses cmake, and it is very finicky.
You must have the right versions of gcc, and g++: gcc-4.7, g++-4.7.

For those unfamiliar with cmake, I would suggest an out-of-source
build. The following commands should work:

  ``` Shell
    mkdir build
    cd build
    CC=gcc-4.7 CXX=g++-4.7 cmake \
      -D CMAKE_BUILD_TYPE=$TYPE \
      -D USE_BDB=OFF \
      -D USE_TDB=ON \
      -D BUILD_TESTING=OFF \
      -D CMAKE_INSTALL_PREFIX=../ft-install/ \
      -D BUILD_FOR_LINUX_KERNEL_MODULE=ON \
      ..

    cmake --build . --target install
  ```

After building TokuDB, you can finally build the actual BetrFS code.
  ``` Shell
      cd filesystem/
      make
  ```


Mounting the file system
------------------------
You need a couple of things:

  1. A device formatted with an existing file system to use as your
  "southbound" file system (we have been using ext4, but there is no
  reason it can't be a different file system).

  2. The southbound file system must also be set up with some files
  and directories that TokuDB expects at certain places, including:

    db/
    /dev/null
    /tmp

  3. The compiled module from the filesystem/ folder

  4. A "dummy" device to pass to the mount command (can be an empty
  file set up as a loop device). This is neither read from or written
  to; it is there solely to pass to the mount command.

  5. zlib (on our system, apt-get install zlib1g-dev)

This example code was used to set up the file system on a setup where
we had a second disk with a partition for our "southbound" file system
at /dev/sdb1. Change the parameters to fit your needs.
``` Shell
  USER=betrfs
  REPO=/home/$USER/ft-index
  MODDIR=filesystem
  MODULE=ftfs.ko
  MOUNTPOINT=mnt
  SBDISK=/dev/sdb1

  sudo mkfs.ext4 $SBDISK
  mkdir -p $MOUNTPOINT
  mount -t ext4 $SBDISK $MOUNTPOINT
  cd $MOUNTPOINT;
  rm -rf *;
  mkdir db;
  mkdir dev;
  touch dev/null;
  mkdir tmp;
  chmod 1777 tmp;
  cd -;
  umount $MOUNTPOINT

  cd $REPO/$MODDIR; make; cd -;
  sudo modprobe zlib
  sudo insmod $REPO/$MODDIR/$MODULE sb_dev=$SBDISK sb_fstype=ext4

  touch dummy.dev
  sudo losetup /dev/loop0 dummy.dev
  sudo mount -t ftfs /dev/loop0 $MOUNTPOINT
```