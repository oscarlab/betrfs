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
    print "\td, --force-ssd: test ssd specific code"
    print "\tm, --force-hdd: test ssd specific code"

if __name__ == "__main__":
    fd = open("test-config.json", 'r')
    config_values = json.load(fd)
    fd.close()

    test = ""

    try :
        opts, args = getopt.getopt(sys.argv[1:], "hsdm", ["help", "sfs", "force-ssd", "force-hdd", "test="])

    except getopt.GetoptError:
        usage()
        sys.exit(2)
    use_sfs = False
    force_ssd = False
    force_hdd = False
    for opt, arg in opts :
        if opt in ("h", "--help") :
            usage()
        elif opt == "--test" :
            test = arg
        elif opt in ("s", "--sfs"):
            use_sfs = True
        elif opt in ("d", "--force-ssd"):
            force_ssd = True
        elif opt in ("m", "--force-hdd"):
            force_hdd = True

    if test != "" :
        print "\n\npre-test {0}.".format(test)

    print "Check dmesg"
    command = "tail -n 50 /var/log/syslog"
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        exit(ret)

    if force_ssd and not force_hdd:
        is_rotational = "0"
    elif not force_ssd and force_hdd:
        is_rotational = "1"
    elif not force_ssd and not force_hdd:
        print "Check device type HDD or SSD"
        blkdev = ''.join([i for i in config_values["southbound device"] if not i.isdigit()])
        command="lsblk -d {}  -o name,rota  | tail -n 1".format(blkdev)
        print command
        is_rotational = subprocess.check_output(["lsblk", "-d", blkdev, "-o", "name,rota"])
        is_rotational = is_rotational.replace("\n", " ").split()[3]
        if not is_rotational in ["0", "1"]:
            print "is_rotational: {} is invalid".format(is_rotational)
            sys.exit(2)
    else:
        print "ERROR: cannot force both hdd and ssd!"
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
