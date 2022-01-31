#!/usr/bin/python

import random
import math
import sys
import getopt

opt_run = 100
opt_ops = 1000

def usage() :
	print sys.argv[0] + ' -x num of messages'
try :
	opts, args = getopt.getopt(sys.argv[1:], 'x:')
except :
	usage()
	sys.exit(2)

boxsize = 3000
for o, a in opts :
	if o == '-x' :
		boxsize = int(a)
print "%d" % (boxsize)
flagged = [[0] * (boxsize + 1)] * 10

random.seed()

for run in range(opt_run) :
	filename = 'testcase/mr-test%d' % (run)
	fh = open(filename, 'w')

	for i_op in range(opt_ops) :
		rand_boxnum = random.randint(0, 9)
		rand_num = random.randint(1, boxsize)

		fh.write('%d\n%d\n' % (rand_boxnum, rand_num))
		if 0 == flagged[rand_boxnum][rand_num] :
			fh.write('0\n')
			flagged[rand_boxnum][rand_num] = 1
		else :
			fh.write('1\n')
			flagged[rand_boxnum][rand_num] = 0
	fh.close()
