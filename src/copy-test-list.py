#!/usr/bin/python

import sys
import string
import subprocess

if len(sys.argv) < 3:
    print "USAGE:", sys.argv[0], "SRC-DIR", "DESTINATION-DIR", "TEST-LIST"
    exit(1)

srcDir = sys.argv[1]
destDir = sys.argv[2]
testList = open(sys.argv[3])

for test in testList:
    commandStr = r'./copy-test.py {0}/{1} {2}/{1}'.format(srcDir, string.rstrip(test), destDir)
    print commandStr
    commandOutput = subprocess.call(commandStr, shell=True)
    print commandOutput
