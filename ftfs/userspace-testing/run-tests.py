#!/usr/bin/python

import sys, os, string, subprocess, fileinput, random, json

DEF_INTERP="default-interp.py"
DEF_PRE="default-pre.py"
DEF_POST="default-post.py"
procfile="/proc/toku_test"

use_sfs = False
DEF_PRE_SFS="default-pre.py --sfs"

def format_sfs(cv):
    print "Format SFS"
    command = "./mkfs-sfs.sh {} ${{PWD}}/../../simplefs/tmp".format(cv["southbound device"])
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR! Failed to format SFS"
        exit(ret)

def format_ext4(cv):
    print "Format SFS"
    command = "./mkfs-ext4.sh {} {}".format(cv["southbound device"], cv["ext4mntpnt"])
    ret = subprocess.call(command, shell=True)
    if ret != 0 :
        print "ERROR! Failed to format ext4"
        exit(ret)


def run_one_test(test, interp, pre, post):
    """ the main testing worker
    1. run any pre-test scripts (pre)
    2. run the actual test (test)
    3. interpret the results in order to take appropriate actions (interp)
    4. run any post-test scripts to cleanup state (post)

    """

    # run pre-test script
    command = "./{0} --test={1}".format(pre, test)
    ret = subprocess.call(command, shell=True)
    if ret :
        print "pre test ({0}) script {1} returning nonzero: {2}".format(test, pre, ret)
        return ret

    # run the test
    print "beginning test " + test
    command = "echo {0} > {1}".format(test, procfile)
    ret = subprocess.call(command, shell=True)
    if ret :
        print "\terror echoing test {0}.".format(test)
        print "\treturning nonzero: {0}".format(ret)
        return ret

    # interpret the results (script should read from the proc file internally)
    command = "./{0} --test={1}".format(interp, test)
    ret = subprocess.call(command, shell=True)
    if ret :
        print "\terror interpreting test {0} results.".format(test)
        print "\tinterpreter returning nonzero: {0}.".format(ret)
        return ret

    print "test {0} returned successful".format(test)

    # run post-test script
    command = "./{0} --test={1}".format(post, test)
    ret = subprocess.call(command, shell=True)
    if ret :
        print "\tpost test script {0}".format(test)
        print "\treturning nonzero: {0}".format(ret)
        return ret

    return 0


def run_test_set(test_array):
    success=0
    failure=0
    ret=0

    for test in test_array :
        if (len(test) == 1) :
	    if use_sfs == True:
                ret = run_one_test(test[0], DEF_INTERP, DEF_PRE_SFS, DEF_POST)
            else:
                ret = run_one_test(test[0], DEF_INTERP, DEF_PRE, DEF_POST)
        else :
            ret = run_one_test(test[0], test[1], test[2], test[3])

        if (ret) :
            print "Test failed: " + test[0]
            failure = failure + 1
            # In our current testing environment, there is no reason to keep going if a test fails
            break
        else :
            print "Test passed: " + test[0]
            success = success + 1

    print "Successes: {0}".format(success)
    print "Failures:  {0}".format(failure)
    return failure

def scripts_exist(test_line) :
    return (os.path.exists(test_line[1]) and os.path.exists(test_line[2]) \
            and os.path.exists(test_line[3]))

def validate_testline(test_line) :
    """ make sure the test name is a valid string to pass to our procfile """
    if (len(test_line) == 4) :
        return scripts_exist(test_line)

    return len(test_line) == 1

def run_all_tests(test_file, shuffle) :
    """ expect test_file to have format
    teststring <result interpreter> <pre-test script> <post-test script>
    OR
    teststring

    """
    test_array = []
    testf = open(test_file, "r")
    for line in testf:
        # implement comments
        if line.startswith("#"):
            continue
        test_line = string.split(line)
        if (validate_testline(test_line)):
            test_array.append(test_line)
        else :
            print "Malformed line in test_file:"
            print "\t\"" + line + "\""
            return -1

    if (shuffle) :
        print "Shuffling tests order"
        random.shuffle(test_array)

    return run_test_set(test_array)

##################
# main
##################

def help() :
    print "Arguments: <testfile> [fstype] [-s (optional, shuffles tests)]"

try:
    if (len(sys.argv) <= 1):
        print "run-tests.py : No suffcient number of arguments."
        help()
        exit(-1)
    shuffle = False
    fd = open("test-config.json", 'r')
    config_values = json.load(fd)
    fd.close()
    print "Running tests:", sys.argv
    print "----"

    test_file = sys.argv[1]
    for x in sys.argv[2:]:
       if x == "--sfs":
          use_sfs = True
       if x == "-s":
          shuffle = True;

    if use_sfs:
       format_sfs(config_values) 
    else:
       format_ext4(config_values)

    ret = run_all_tests(test_file, shuffle)
    sys.exit(ret)

except OSError, Argument:
    print Argument;
