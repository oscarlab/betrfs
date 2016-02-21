#!/usr/bin/python

import subprocess
import sys
import getopt

def usage() :
    print "optional args:"
    print "\t--test=<name of test>: specify test name"
    print "\th, --help: show this dialogue"

if __name__ == "__main__":

    test = ""

    try :
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "test="])

    except getopt.GetoptError:
        usage();
        sys.exit(2)

    for opt, arg in opts :
        if opt in ("h", "--help") :
            usage()
        elif opt == "--test" :
            test = arg

    if test != "" :
        print "\npost-test {0}.".format(test)

    command = "cat /proc/meminfo"
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        print "cat /proc/meminfo returning: {0}. exiting...".format(ret)
        exit(ret)

    command = "rmmod ftfs.ko"
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        print "command \"{0}\" returning: {1}. exiting...".format(command, ret)
        exit(ret)

    print "removed module, printing /proc/meminfo"
    command = "cat /proc/meminfo"
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR!"
        print "cat /proc/meminfo returning: {0}. exiting...".format(ret)
        exit(ret)

    exit(ret)

