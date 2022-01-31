#!/bin/bash
# Test functionality for:
#  - Creating and writing small files
#  - Renaming and deleting files within a directory
#  - Renaming and deleting directories
#  - Contents persisting across unmounts

# this cleanup() stuff is inspired by xfstests
status=1 # fail by default
trap "_cleanup; exit \$status" 0 1 2 3 15
_cleanup()
{
	if [ -n "$mntpnt" ]; then
		rm -rf "$mntpnt/{*,.*}"
	fi
	if [ $status -ne 0 ]; then
		echo "sanity-test-rename.sh failed."
	fi
}

# verify that the supplied directory has the supplied number of files
# Usage: _num_files dir expected_num_files_in_dir
_num_files()
{
	[ $# -eq 2 ]
	[ "$(ls -1qA "$1" | wc -l)" -eq "$2" ]
}

# verify that the supplied file exists and contains only the supplied string
# Usage: _check_contents file string
_check_contents()
{
	[ -f $1 ]
	grep -q $2 $1
	[ $(stat -c %s $1) -eq ${#2} ]
}

_print_usage()
{
	echo "Usage: ./sanity-test-rename.sh disk_type"
	echo "disk_type = --force-hdd | --force-ssd"
	status=1
	exit 1
}

set -eu
. ../fs-info.sh
[ $# -eq 1 ] || _print_usage
[ "$1" == "--force-hdd" ] || [ "$1" == "--force-ssd" ] || _print_usage
PS4='${LINENO}: '
data="Writing_to_file"
cd $mntpnt

# ensure directory is empty
_num_files . 0

# create file
touch a
[ -f a ]
_num_files . 1

# write to file
printf $data > a
_check_contents a $data
_num_files . 1

# create directory and file in directory
mkdir foo
[ -d foo ]
_num_files . 2
_num_files foo 0
touch foo/tmp
[ -f foo/tmp ]
_num_files foo 1
_num_files . 2

# write to file in directory
printf $data >> foo/tmp
[ -d foo ]
_check_contents foo/tmp $data
_num_files foo 1
_num_files . 2

# try to rmdir an non-empty directory and ensure that it fails
set +e
output=$(rmdir foo 2>&1)
exit_code=$?
set -e
[ $exit_code -ne 0 ]
# [ "$output" == "rmdir: failed to remove ‘foo’: Directory not empty" ]
[ "$output" == "rmdir: failed to remove 'foo': Directory not empty" ]
_num_files foo 1
_num_files . 2

# actually remove the directory and it's contents
rm -rf foo
[ ! -f foo/tmp ]
[ ! -d foo ]
_num_files . 1

# do similar test, except use rm and rmdir instead of rm -rf
mkdir foo
[ -d foo ]
_num_files . 2
_num_files foo 0
printf $data > foo/tmp
_num_files foo 1
_num_files . 2
_check_contents foo/tmp $data
_num_files foo 1
_num_files . 2
set +e
output=$(rmdir foo 2>&1)
exit_code=$?
set -e
[ $exit_code -ne 0 ]
# [ "$output" == "rmdir: failed to remove ‘foo’: Directory not empty" ]
[ "$output" == "rmdir: failed to remove 'foo': Directory not empty" ]
_num_files foo 1
_num_files . 2
rm foo/tmp
_num_files foo 0
[ ! -e foo/tmp ]
rmdir foo
[ ! -e foo ]
_num_files . 1

# Truncate file a to 10 bytes and then delete
truncate -s 10 a
_check_contents a ${data:0:10}
_num_files . 1
cat a > /dev/null
rm a
[ ! -e a ]
_num_files . 0

# try writing to file in top-level directory directly
printf $data > b
_num_files . 1
_check_contents b $data
_num_files . 1

# rename
mv b c
_num_files . 1
[ ! -e b ]
_check_contents c $data
_num_files . 1

# create more files to test for after remounting
cat c > d
_num_files . 2
cp d e
_num_files . 3
_check_contents c $data
_check_contents d $data
_check_contents e $data
_num_files . 3

cd -
sudo -E $FT_HOMEDIR/benchmarks/clear-fs-caches.sh $1

# check that the expected files are around after being remounted
cd $mntpnt
_num_files . 3
_check_contents c $data
_check_contents d $data
_check_contents e $data
_num_files . 3
rm d c
_num_files . 1
rm *
_num_files . 0

echo "It's all good, dawg."
status=0
