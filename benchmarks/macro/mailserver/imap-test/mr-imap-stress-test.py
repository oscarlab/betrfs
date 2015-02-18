#!/usr/bin/python

import imaplib
import time
import math
import os
import sys
import subprocess
import random

SERVER = ''
USER = ''
PASS = ''

opt_run = 100
opt_ops = 1000

result = []

for run in range(opt_run):
	filename = 'testcase/mr-test%d' % (run)
	fh = open(filename, 'r')
	box = [0] * opt_ops
	msg = [0] * opt_ops
	op = [0] * opt_ops
	for i_op in range(opt_ops) :
		box[i_op] = int(fh.readline())
		msg[i_op] = int(fh.readline())
		op[i_op] = int(fh.readline())
	fh.close()

	t1 = time.time()
	if 0 == os.fork() :
		M = imaplib.IMAP4_SSL(SERVER)
		M.login(USER, PASS)

		for i_op in range(opt_ops) :
			boxname = 'box%d' % (box[i_op])

			M.select(boxname)

			if (0 == op[i_op]) :
				M.store(msg[i_op], '+FLAGS', '(\\FLAGGED)')
			else :
				M.store(msg[i_op], '-FLAGS', '(\\FLAGGED)')
			M.fetch(msg[i_op], '(RFC822 FLAGS)')

			M.close()

		M.logout()
		sys.exit(0)

	os.wait()
	t2 = time.time()
	result.append(t2 - t1)

avg = 0.0
for run in range(opt_run) :
	avg += result[run]
avg /= opt_run

dev = 0.0
mint = result[0]
maxt = result[0]
for run in range(opt_run) :
	dev += math.pow(result[run] - avg, 2)
	if (result[run] > maxt) :
		maxt = result[run]
	if (result[run] < mint) :
		mint = result[run]
dev /= opt_run
dev = math.sqrt(dev)

print "avg: %f, dev: %f, min %f, max %f" % (avg, dev, mint, maxt)
