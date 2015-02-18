#!/usr/bin/python

#from __future__ import print_function
import sys, os, string, fileinput, operator

def addMalloc(ptr, size, d) :
    if (ptr in d) :
        if d[ptr] == "-1" :
            del d[ptr]
            return 0
        print "{0} already allocated".format(ptr)
        print "malloc broken"
        print d
        return -1
    d[ptr] = size
    return 0

def addFree(ptr, size, d) :
    if (ptr not in d) :
        print "unknown {0} freed".format(ptr)
        print "free broken?"
        d[ptr] = "-1"
        return -1
    del d[ptr]
    return 0

def addMallocSize(size, d) :
    if (size in d) :
        d[size] += 1
    else :
        d[size] = 1

def isAlloc(alloc) :
    return alloc == "alloc"

def isFree(alloc) :
    return alloc == "free"


inputName = "memory_profile"

iput = raw_input('file to parse {0}? ([y]/n):'.format(inputName)).lower()
if not iput == 'y' and not iput == '' :
    inputName = raw_input('please input file to parse:\n\t')

iput = raw_input('track net mallocs/frees? ([y]/n):').lower()
if iput == 'y' or iput == '' :
    with open(inputName) as f:
        f.next()
        allocs = dict()
        for line in f:
            line = line.rstrip()
            alloc,ptr,size = line.split(",")
            if isAlloc(alloc) :
                err = addMalloc(ptr,size,allocs)
            elif isFree(alloc):
                err = addFree(ptr,size,allocs)
            else:
                print "malformatted line: {0}".format(line)
                exit(-1)
            if err :
                print ("allocator behavior is broken")
                print(allocs)
                exit(err)
        print(allocs)

iput = raw_input('sort allocation sizes? ([y]/n):').lower()
if iput == 'y' or iput == '' :
    with open(inputName) as f:
        f.next()
        allocs = dict()
        for line in f:
            line = line.rstrip()
            alloc,ptr,size = line.split(",")
            if isAlloc(alloc) :
                addMallocSize(int(size),allocs)
            elif isFree(alloc):
                continue
            else :
                print "malformatted line: {0}".format(line)
                exit(-1)
        ## sort allocs dictionary by most popular allocation size
        sorted_allocs = sorted(allocs.items(), key=operator.itemgetter(1))
        print(sorted_allocs)

        print
        print

        ## sort allocs dictionary by largest allocation size
        sorted_allocs = sorted(allocs.items(), key=operator.itemgetter(0))
        print(sorted_allocs)
