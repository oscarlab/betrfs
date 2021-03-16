#On Beakers /dev/sda is the fast connector and has the SSD
#  and the root fs is installed on the HDD on /dev/sdb

sb_dev=/dev/sdb
mntpnt=/mnt/benchmark
REPO=${REPO:-betrfs}
clone_repo=linux
circle_size=128
CUR_MNTPNT=${CUR_MNTPNT:-/home/betrfs}
FT_HOMEDIR=$CUR_MNTPNT/$REPO
module=$FT_HOMEDIR/filesystem/ftfs.ko
allfs=(ext4 ftfs)
alltest=(ext4 master)
use_sfs=false
resultdir=$FT_HOMEDIR/benchmarks/results
