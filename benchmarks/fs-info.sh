#On Beakers /dev/sda is the fast connector and has the SSD
#  and the root fs is installed on the HDD on /dev/sdb

#sb_dev=/dev/sdb3
sb_dev=/dev/sdb1
dummy_file=dummy.dev
dummy_dev=/dev/loop0
mntpnt=/mnt/benchmark
southbound_module=/home/betrfs/ft-index/ftfs/ftfs.ko
module=/home/betrfs/ft-index/filesystem/ftfs.ko
repo=ft-index
clone_repo=linux
circle_size=128
