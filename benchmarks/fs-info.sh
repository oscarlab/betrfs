#On Beakers /dev/sda is the fast connector and has the SSD
#  and the root fs is installed on the HDD on /dev/sdb

#sb_dev=/dev/sdb3
sb_dev=/dev/sdb
dummy_file=dummy.dev
dummy_dev=/dev/loop0
mntpnt=/mnt/benchmark
defrepo=betrfs
REPO=${REPO:-betrfs}
user_owner=betrfs:betrfs
clone_repo=linux
circle_size=128
CUR_MNTPNT=${CUR_MNTPNT:-/home/betrfs}
FT_HOMEDIR=$CUR_MNTPNT/$REPO
southbound_module=$FT_HOMEDIR/ftfs/ftfs.ko
module=$FT_HOMEDIR/filesystem/ftfs.ko
allfs=(ext4 ftfs)
alltest=(ext4 master)
use_sfs=false
resultdir=$FT_HOMEDIR/benchmarks/results
test_num=3
