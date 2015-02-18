#!/usr/bin/python

import sys
import string

if len(sys.argv) < 3:
    print "USAGE:", sys.argv[0], "FILE-TO-COPY", "DESTINATION-FILE"
    exit(1)

sourcename = sys.argv[1]
destname = sys.argv[2]
source=open(sourcename, 'r')
dest=open(destname, 'w')

def isTestFunction(prev, line) :
    if( prev.startswith("int") and line.startswith("test_main")) or "test_main" in line:
        return True
    return False


def funcify(filename) :
    tmp = string.replace(filename, "-", "_")
    print tmp
    tmp = string.rstrip(tmp, ".cc")
    print tmp
    tmp = string.rsplit(tmp, "/")[-1]
    print tmp
    return tmp

def externCFunction(prev, line):
    prev = string.rstrip(prev)
    f = funcify(sourcename)
    
    return "extern \"C\" {0} test_{1}(void);\n{0} test_{1}(void) {2}\n".format("int", f, "{")

def filterOut(line) :
    if "test_main" in line or "parse_args" in line :
        return True
    return False

initialized = False
for line in source:
    if not initialized:
        prev = line
        initialized = True
        continue

    if isTestFunction(prev, line):
        dest.write(externCFunction(prev, line))
    else :
        if not filterOut(prev) :
            dest.write(prev)
    prev = line

dest.write(prev)

source.close
dest.close
