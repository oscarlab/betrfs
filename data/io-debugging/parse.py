#!/usr/bin/python

#from __future__ import print_function
import sys, os, string, fileinput, re


class IORange:
    'the range of a particular IO'

    def _get_start(self):
        return self.__start
    def _set_start(self, value):
        if not isinstance(value, int):
            raise TypeError("start must be set to an integer")
        self.__start = value
    def _get_end(self):
        return self.__end
    def _set_end(self, value):
        if not isinstance(value, int):
            raise TypeError("end must be set to an integer")
        self.__end = value

    def _get_fd(self):
        return self.__fd
    def _set_fd(self, value):
        if not isinstance(value, int):
            raise TypeError("end must be set to an integer")
        self.__fd = value


    start = property(_get_start, _set_start)
    end = property(_get_end, _set_end)
    fd = property(_get_fd, _set_fd)
    op = "null"

    def __init__(self, op, fd, start, end):
        self.op = op
        self._set_fd(fd)
        self._set_start(start)
        self._set_end(end)

    def __repr__(self) :
        return "{0} fd:{1} of size {2}: ({3}, {4})".format(self.op, self.fd, 
                                                    self.end - self.start,
                                                    self.start, self.end)

    def contains(self, other) :
        return self.start <= other.start and self.end >= other.end

    def containedBy(self, other) :
        return other.start <= self.start and other.end >= self.end

    def overlaps(self, other) :
        if (self.start <= other.start and self.end >= other.start and self.end <= other.end and min(self.end, other.end) - max(self.start, other.start) > 0) :
            return True
        if (self.start >= other.start and self.start <= other.end and self.end >= other.end) and min(self.end, other.end) - max(self.start, other.start) > 0 :
            return True
        return False

    def conflicts(self, other) :
        if (self.fd != other.fd) :
            return False
        return self.contains(other) or self.containedBy(other) or self.overlaps(other)

    def conflict(self, other) :
        if self.contains(other) :
            return str(self) + " contains " + str(other)
        if self.containedBy(other) :
            return str(self) + " contained by " + str(other)
        if self.overlaps(other) :
            return str(self) + " and " + str(other) + " overlap by " + str(min(self.end, other.end) - max(self.start, other.start))

def isread(line) :
    return "read" in line

def iswrite(line) :
    return "write" in line

ioranges = []
readranges = []
writeranges = []
infile = raw_input('please input file to parse:\n\t')
outfile = raw_input('please input file to output:\n\t')

with open(infile) as f :
    for line in f :
        if isread(line) :
            m = re.findall('[0-9]+', line)
            if m and len(m) == 3:
                readranges.append(IORange("read", int(m[0]), int(m[1]), int(m[2])))
                ioranges.append(IORange("read", int(m[0]), int(m[1]), int(m[2])))
        elif iswrite(line) :
            m = re.findall('[0-9]+', line)
            if m and len(m) == 3:
                writeranges.append(IORange("write", int(m[0]), int(m[1]), int(m[2])))
                ioranges.append(IORange("write", int(m[0]), int(m[1]), int(m[2])))

with open(outfile,'w') as f :
    f.write("List of all IOs:\n")
    for i in range(1, len(ioranges)) :
        f.write(str(ioranges[i]) + '\n')
    f.write("\nList of overlapping writes:\n")
    for i in range(1, len(writeranges)) :
        if writeranges[i-1].conflicts(writeranges[i]) :
            f.write(writeranges[i-1].conflict(writeranges[i]) + '\n')
    f.write("\nList of overlapping reads:\n")
    for i in range(1, len(readranges)) :
        if readranges[i-1].conflicts(readranges[i]) :
            f.write(readranges[i-1].conflict(readranges[i]) + '\n')
