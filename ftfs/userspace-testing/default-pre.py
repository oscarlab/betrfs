#!/usr/bin/python

import subprocess
import sys
import getopt

def usage() :
    print "optional args:"
    print "\t--test=<name of test>: specify test name"
    print "\th, --help: show this dialogue"
    print "\ts, --sfs: use sfs for unit tests"

if __name__ == "__main__":

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

    if use_sfs:
       command = "insmod ftfs.ko sb_dev=/dev/sdb sb_fstype=sfs"
    else:
       command = "insmod ftfs.ko sb_dev=/dev/sdb sb_fstype=ext4"

    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        print "command \"{0}\" returning: {1}. exiting...".format(command, ret)
        exit(ret)

    exit(ret)
