#! /usr/bin/python

################################################################################
# run-all-fs.py swaps in the correct config files and runs the test for all
# filesystems

import subprocess
import shlex

def runtest(fs):
	subprocess.check_call(shlex.split("bash destroy.sh aged"))
	subprocess.check_call(shlex.split("bash destroy.sh clean"))
	with open("config-temp.sh", "r") as template:
		config = template.read()
		config = config.replace("~", fs[0])
		config = config.replace("@", fs[1])
	with open("config.sh", "w") as configfile:
		configfile.write(config)
	subprocess.check_call(shlex.split("mkfs.ext4 /dev/sda1"))
	subprocess.check_call(shlex.split("mkfs.ext4 /dev/sda2"))
	subprocess.check_call(shlex.split("python test.py"))

#fs_list = [["btrfs", "btrfs"], ["f2fs", "f2fs"], ["xfs", "xfs"], ["zfs", "zfs"], ["ext4", "ftfs"], ["ftfs", "ext4"]]
#fs_list = [["zfs", "zfs"], ["ext4", "ftfs"], ["ftfs", "ext4"]]
fs_list = [["ftfs", "ext4"]]

for filesystem in fs_list:
	runtest(filesystem)
