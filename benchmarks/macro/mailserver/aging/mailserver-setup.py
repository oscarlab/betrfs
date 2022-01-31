#!/usr/bin/python
import imaplib
import sys
import random
import os

SERVER = "localhost"
USER = ["ftfstest1", "ftfstest2", "ftfstest3", "ftfstest4", "ftfstest5", "ftfstest6", "ftfstest7", "ftfstest8", "ftfstest9", "ftfstest10", "ftfstest11", "ftfstest12", "ftfstest13", "ftfstest14", "ftfstest15", "ftfstest16"]
PASS = ["Oscarlab", "oScarlab", "osCarlab", "oscArlab", "oscaRlab", "oscarLab", "oscarlAb", "oscarlaB", "oSCARLAB", "OsCARLAB", "OScARLAB", "OSCaRLAB", "OSCArLAB", "OSCARlAB", "OSCARLaB", "OSCARLAb"]

n_user = 16
n_box = 10
boxsize = 1000
max_msg_len = 32768

print "MAILSERVER setup"

for i in range(n_user) :
	if 0 == os.fork() :
		# Child i
		random.seed()
		M = imaplib.IMAP4_SSL(SERVER)
		M.login(USER[i], PASS[i])
		for j in range(n_box) :
			box = "box%d" % j
			M.delete(box)
			M.create(box)
			print "MAILSERVER setup: child %d box %d" % (i, j)
			for k in range(boxsize) :
				msg_len = random.randint(1, max_msg_len)
				msg = os.urandom(msg_len)
				M.append(box, None, None, msg)
		M.logout()
		sys.exit(0)
for i in range(n_user) :
	os.wait()
sys.exit(0)
