# Configuration file for git test

################################################################################
# General Test Configuration Parameters

test_name=git_~_ssd_20gb_gc_on
total_pulls=10000
pulls_per_grep=100
gc_on=True
keep_traces=False
clear_cache=True
grep_random=8TCg8BVMrUz4xoaU

################################################################################
# System Parameters

user=betrfs:betrfs

################################################################################
# Profiles
# set mntpnt to '' to disable in the test

case "$profile" in
	aged)
		partition=/dev/sda1
		mntpnt=/mnt/research
		fs_type=~
		# zfs only
		datastore=agedstore 
		;;

	clean)
		partition=/dev/sda2
		mntpnt=/mnt/clean
		fs_type=@
		# zfs only
		datastore=cleanstore
		;;

	cleaner)
		partition=/dev/sdb3
		mntpnt=''
		fs_type=ext4
		# ext4 only
		datastore=cleanerstore
		;;

	*)
		echo "Unknown profile $profile"
		exit 1
		;;
esac
	
################################################################################
# betrfs Specific Parameters
# --since only one betrfs filesystem can be mounted at a time, these need not be
# --duplicated as above

dummy_file=dummy.dev
dummy_dev=/dev/loop0
module=/home/betrfs/ft-index/filesystem/ftfs.ko
circle_size=128
