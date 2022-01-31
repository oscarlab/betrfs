#On Beakers /dev/sda is the fast connector and has the SSD
#  and the root fs is installed on the HDD on /dev/sdb

sb_dev=/dev/sdc
dummy_file=dummy.dev
# TODO: dummy_dev is fragile. It would be better to dynamically choose dummy_dev
# using `losetup -f` instead, but the use of dummy_dev in clear-fs-caches.sh
# prevents that right now. So, live with this hack until we refactor the
# benchmarking scripts.
dummy_dev=${dummy_dev:-/dev/loop0}
mntpnt=/mnt/benchmark
defrepo=betrfs
REPO=${REPO:-betrfs}
# This should just be set to whatever your user is
user_owner=betrfs:betrfs
#repo=master/betrfs
clone_repo=linux
circle_size=128
CUR_MNTPNT=${CUR_MNTPNT:-/home/betrfs}
FT_HOMEDIR=$CUR_MNTPNT/$REPO
southbound_module=$FT_HOMEDIR/ftfs/ftfs.ko
module=$FT_HOMEDIR/filesystem/ftfs.ko
# Which filesystems to benchmark (actual name of fs/module)
allfs=(ftfs)
# Name of run, should map one-to-one onto allfs list
# used to name data files and by postprocess.sh
alltest=(master)
use_sfs=true
resultdir=$FT_HOMEDIR/benchmarks/results
test_num=3
# tokubench has its own setting because it has historically been finicky
tokubench_num=7
# rm -rf test num
rm_rf_num=7
