#!/usr/bin/python

import subprocess

if __name__ == "__main__":
    command = "sudo insmod ftfs.ko sb_dev=/dev/hdb sb_fstype=ext4"
    ret = subprocess.call(command, shell=True)
    exit(ret)
    
