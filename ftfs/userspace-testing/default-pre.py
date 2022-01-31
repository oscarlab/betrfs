#!/usr/bin/python

import subprocess
import sys
import getopt
import json

def usage() :
    print "optional args:"
    print "\t--test=<name of test>: specify test name"
    print "\th, --help: show this dialogue"
    print "\ts, --sfs: use sfs for unit tests"

if __name__ == "__main__":
    fd = open("test-config.json", 'r')
    config_values = json.load(fd)
    fd.close()

    test = ""

    try :
        opts, args = getopt.getopt(sys.argv[1:], "hs", ["help", "sfs", "test="])

    except getopt.GetoptError:
        usage();
        sys.exit(2)

    use_sfs = False
    for opt, arg in opts :
        if opt in ("h", "--help") :
            usage()
        elif opt == "--test" :
            test = arg
        elif opt in ("s", "--sfs"):
            use_sfs = True

    if test != "" :
        print "\n\npre-test {0}.".format(test)

    print "Check dmesg"
    command = "tail -n 50 /var/log/syslog"
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        exit(ret)

    print "Check device type HDD or SSD"
    blkdev = ''.join([i for i in config_values["southbound device"] if not i.isdigit()])
    command="lsblk -d {}  -o name,rota  | tail -n 1".format(blkdev)
    print command
    is_rotational = subprocess.check_output(["lsblk", "-d", blkdev, "-o", "name,rota"])
    is_rotational = is_rotational.replace("\n", " ").split()[3]
    if not is_rotational in ["0", "1"]:
       print "is_rotational: {} is invalid".format(is_rotational)
       sys.exit(2)

    if use_sfs:
       command = "insmod ftfs.ko sb_dev={} sb_fstype=sfs sb_is_rotational={}".format(config_values["southbound device"], is_rotational)
    else:
       command = "insmod ftfs.ko sb_dev={} sb_fstype=ext4 sb_is_rotational={}".format(config_values["southbound device"], is_rotational)

    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        print "command \"{0}\" returning: {1}. exiting...".format(command, ret)
        exit(ret)

    exit(ret)
