#!/usr/bin/python
import imaplib
import sys
import random
import os
import threading
import time
import types
import subprocess

SERVER = "localhost"
USER = ["ftfstest1", "ftfstest2", "ftfstest3", "ftfstest4", "ftfstest5", "ftfstest6", "ftfstest7", "ftfstest8", "ftfstest9", "ftfstest10", "ftfstest11", "ftfstest12", "ftfstest13", "ftfstest14", "ftfstest15", "ftfstest16"]
PASS = ["oscarlab", "oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab","oscarlab"]

n_user = 2
n_box = 80
boxsize = 1000
max_msg_len = 32768
run_time = 1800
n_top = 8000

def worker_thread(i, n_op_thread, running) :
	m = imaplib.IMAP4_SSL(SERVER)
	m.login(USER[i], PASS[i])
	while not running.isSet() :
		pass
	n_ops = [0] * 3
#	while running.isSet() :
	for i in range(n_top) :
		boxnum = random.randint(1, n_box) - 1
		box = "box%d" % boxnum
		x = m.select(box)
		rand_op = random.randint(1, 2) - 1
		if rand_op == 0 :
			msg_len = random.randint(1, max_msg_len)
			msg = os.urandom(msg_len)
			m.APPEND(box, None, None, msg)
		else : 
			typ, msg_ids = m.search(None, 'ALL')
			msgs = msg_ids[0].split()
			msg_num = random.randint(1, len(msgs)) - 1
			msg = msgs[msg_num]
#			if rand_op == 1 :
			m.store(msg, "+FLAGS", "(\\Deleted)")
			m.expunge()
#			else :
#				typ, data = m.fetch(msg, "(RFC822 FLAGS)")
#				flagged = 0
#				if type(data[0]) is types.NoneType :
#					continue
#				flagged = 0
#				for flag in imaplib.ParseFlags(data[0][0]) :
#					if (flag == "\Flagged") :
#						flagged = 1
#				if flagged :
#					m.store(msg, "-FLAGS", "(\\FLAGGED)")
#				else :
#					m.store(msg, "+FLAGS", "(\\FLAGGED)")
		n_ops[rand_op] = n_ops[rand_op] + 1
	subprocess.call('echo "flush" > /proc/toku_flusher', shell=True)
	m.logout()
	print "Thread %d: append %d delete %d flag change %d" % (i, n_ops[0], n_ops[1], n_ops[2])
	n_op_thread.append(n_ops[0] + n_ops[1] + n_ops[2])

print "MAILSERVER AGEING"
f=open('mailservertime.out','a')

t = []
running = threading.Event()
n_op_thread = []
for i in range(n_user) :
	tmp_t = threading.Thread(target = worker_thread, args = (i, n_op_thread, running,))
	tmp_t.start()
	t.append(tmp_t)

time.sleep(2)
running.set()
t1 = time.time()
#time.sleep(run_time)
#running.clear()
for i in range(n_user):
	t[i].join()
t2 = time.time()
n_op_total = 0
for i in range(n_user) :
	n_op_total = n_op_total + n_op_thread[i]
print "This experiment took %f seconds" % (t2 - t1)
print "%d ops are executed (%f op/s)" % (n_op_total, n_op_total / (t2 - t1))
f.write("Time\t")
f.write(str(t2 - t1) + '\t')
f.write("Nops\t")
f.write(str(n_op_total) + '\n')

sys.exit(0)
