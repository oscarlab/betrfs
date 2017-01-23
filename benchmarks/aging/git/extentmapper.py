#!/usr/bin/python

# extentmapper creates a list of the extents in a directory structure in posix find order

import fiemap
import os
import subprocess
import argparse
import shlex

def mapextents(targetdir):

 # generate the list of files and directories in the targer
 findoutput = subprocess.check_output(['find', targetdir])
 filelist = findoutput.split('\n')
 filelist.pop() # removes the trailing empty string
 #filelist.pop(0) # removes the root directory, which causes some problems
 
 longestpathlen = len(max(filelist, key=len))
 
 extentlist = []
 
 # for each filepath, fetch the extents with fiemap
 for filepath in filelist:
  thisfile = os.open(filepath, os.O_RDONLY)
  extent = fiemap.get_all_mappings(thisfile)
  for e in extent.extents:
   for i in range(0,e.length/4096):
    extentlist.append(e.physical/4096 + i)
  os.close(thisfile)
 
 minextent = min(extentlist)
 return map(lambda e: e - minextent, extentlist)


