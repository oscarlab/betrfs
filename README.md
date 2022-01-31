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
    |   simplefs   |
    |______________|


Like any other file system, BetrFS is registered with the VFS as a
file system during module load. But BetrFS has a stacked file system
design. When you mount BetrFS, it loads a B^e-tree index from a
separate kernel file system (the "southbound" file system, here
simplefs). For this reason, you must specify (at BetrFS mount time) the
device and file system type where the B^e-tree index image
resides. This will be explained in detail in the
[Mounting BetrFS](#Mounting-BetrFS) section below.


Repository layout
-----------------

BetrFS code is contained in the filesystem/ directory. Its primarily
role is to implement the BetrFS schema and convert VFS operations into
B^e-tree operations. The BetrFS kernel module code can be found in
filesystem/ftfs_module.c, and the primary files that deal with the
VFS->B^e-tree mapping are filesystem/nb_super.c and
filesystem/nb_bstore.c

The B^e-tree implementation in BetrFS is from the open-source fractal
tree index provided by TokuTek (TokuDB). There have been slight
modifications to TokuDB in order for the kernel port to be successful,
but the TokuDB code has remained largely unchanged. The ft/, src/,
portability/, util/, include/, locktree/, and cmake_modules/
directories contain most of the TokuDB code and configuration files.

To port the B^e-tree to the kernel, we reimplemented the userspace
libraries used by TokuDB that were not compatible with the
kernel. Refer to the [README in filesystem/](./filesystem) for more
information.

The ftfs/ directory contains a simple module that can be used to run
all of the TokuDB regression tests within the kernel. It is not
necessary to run BetrFS, but can be useful for testing enhancements to
the data structures.


Compiling the code
------------------

We recommend using Ubuntu 18.04 (bionic), as this is the distribution on
which BetrFS is currently tested and developed.

First you must install a 4.19.99 Linux kernel. A stock kernel is fine,
and other versions may work, but they are not tested.

The next step is to build TokuDB. TokuDB uses cmake, and it is very finicky.
You must have the right versions of gcc, and g++: gcc-7, g++-7.

In addition, you must install valgrind and zlib. On our system,

    apt-get install gcc-7 g++-7 valgrind zlib1g-dev

To build, simply run the build.sh script:

    ./build.sh

This builds for production/benchmarking by default.  To compile
in additional debugging tools and assertions, which may cause
the code to run slower, you may pass in the `-d` argument:

    ./build.sh -d


Mounting BetrFS
------------------------
You need a couple of things:

  1. A device formatted with an existing file system to use as your
  "southbound" file system (we have been using simplefs, but there is no
  reason it can't be a different file system).

  2. The compiled modules from the simplefs/ and filesystem/ directories.

  4. A "dummy" device to pass to the mount command (can be an empty
  file set up as a loop device). This is neither read from or written
  to; it is there solely to pass to the mount command.

Refer to [qemu-utils/mount-betrfs.sh](qemu-utils/mount-betrfs.sh) for the most
up-to-date commands of how to mount BetrFS. In that example, BetrFS is mounted
on `/dev/hdb`. Don't forget to change the parameters at the beginning of the script
to fit your system.
