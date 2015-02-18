#!/usr/bin/python

from __future__ import print_function
import sys, os, string, fileinput

def isAlloc(line) :
    return "alloc ptr:" in line

def isFree(line) :
    return "free ptr:" in line


def formatAlloc(line) :
    return "alloc," + line.replace("alloc ptr:", "").replace(" size:", ",").rstrip()


def formatFree(line) :
    return "free," + line.replace("free ptr:", "").replace(" size:", ",").rstrip()

def parseTrace(inputName,outputName):
    output = open(outputName, 'w')
    print("type,ptr,size", file=output)
    with open(inputName) as f:
        for line in f:
            if isAlloc(line):
                print(formatAlloc(line), file=output)
            elif isFree(line):
                print(formatFree(line), file=output)
    output.close()


outputFile = "memory_profile"
inputFile = "allocations.data"

try:
    os.unlink(outputFile)
except OSError:
    pass

parseTrace(inputFile, outputFile)



