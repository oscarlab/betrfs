#!/usr/bin/python

import sys
import re
from xml.etree import cElementTree as ET

def extractNameXML( line ) :
    try:
        root = ET.fromstring(line)
        name = root.find('name').text
        return name
    except:
        print "error parsing XML: {0}".format(line)
        return ""

def parseFile( inputfile, outputfile ):
    infile = open(inputfile, "r")
    outfile = open(outputfile, "a")
    count = 0
    attempted = []
    completed = []
    for line in infile:
        line = line.rstrip()
        if "<test_descriptor>" in line:
            name = extractNameXML(line)
            attempted.append(name)
        if "<test_completion>" in line:
            name = extractNameXML(line)
            completed.append(name)

    diff = list(set(attempted) - set(completed))

    outfile.write("Total tests completed: " + str(len(completed)) + "\n")
    outfile.write("--------------------------------\n\n");
    outfile.write("attempted tests:\n" + str(diff) + "\n\n")
    outfile.write("completed tests:\n" + str(completed) + "\n\n")
    infile.close()
    outfile.close()

if __name__ == "__main__":
        if(len(sys.argv) < 3):
           print(sys.argv[0] + " inputfile outputfile")
           exit()
        parseFile(sys.argv[1], sys.argv[2]);
