#!/usr/bin/python

#from __future__ import print_function
import sys, os, string, fileinput, re

infile = raw_input('please input file to parse:\n\t')
outfile = raw_input('please input file to output:\n\t')

with open(infile) as f :
    with open(outfile, 'w') as o :
        for line in f :
            if "]" in line:
                param, value = line.split("]",1)
                o.write(value)
