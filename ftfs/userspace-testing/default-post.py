#!/usr/bin/python

import sys, subprocess

if __name__ == "__main__":
    command = "sudo rmmod ftfs.ko"
    ret = subprocess.call(command, shell=True)
    exit(ret)

