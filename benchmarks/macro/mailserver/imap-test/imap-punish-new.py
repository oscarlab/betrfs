#!/usr/bin/python

#import getpass
import imaplib
import time
import sys
import getopt
import random
import os
import types

SERVER = "localhost"
USER = "ftfstest"
PASS = "ftfstest"

def usage() :
    print sys.argv[0] + \
''': [-t # num threads (1)]
         [ -w # write operation fraction out of 100 (0)]
         [ -m # mailboxes (shared among threads) (1)]
         [ -n # operations (per thread) (1000)]
         [ -h Print this message]
'''

# Get command line options
try :
    opts, args = getopt.getopt(sys.argv[1:], 'hn:m:t:w:x:s')
except :
    usage()
    sys.exit(2)

opt_threads = 4
opt_writes = 50
opt_mailboxes = 10
opt_ops = 1000
opt_setup = 0
boxsize = 2500

for o, a in opts :
    if o == '-h' :
        usage()
        sys.exit(2)
    if o == '-n' :
        opt_ops = int(a)
    if o == '-m' :
        opt_mailboxes = int(a)        
    if o == '-t' :
        opt_threads = int(a)
    if o == '-w' :
        opt_writes = int(a)
    if o == '-s' :
        opt_setup = 1
    if o == '-x' :
	boxsize = int(a)

print "IMAP Punish: %d threads, %d operations, %d mailboxes, %d %% writes" % (opt_threads, opt_ops, opt_mailboxes, opt_writes)

if (opt_setup) :
    # Initialize each mailbox to contain 3,000 messages
    M = imaplib.IMAP4_SSL(SERVER)
    M.login(USER, PASS)
    for i in range (opt_mailboxes) :
        name = "box%d" % i
        M.delete(name)
        M.create(name)
        print "Making box %s" % name
        for j in range (boxsize) :
            M.append(name, None, None, "I love to go a-wandering, along the mountain tracks...")
    M.logout()
    sys.exit(0)

#print "Done making boxes.  Making %d threads" % opt_threads

t1 = time.time()

# Create (fork) threads
for i in range(opt_threads) :
    if 0 == os.fork() :
        # Child
        random.seed()

        bsize = boxsize
        M = imaplib.IMAP4_SSL(SERVER)
        M.login(USER, PASS)
        box = "box%d" % i
        M.select(box)

        for j in range(opt_ops) :
            rand_op = 0
            # Select an operation
            if opt_writes != 0 :
                rand_op = random.randint(1, 100)
                if (rand_op <= opt_writes) :
                    # Choose the specific type of write
                    # 1 = change flag
                    # 2 = delete
                    # 3 = append
                    rand_op = random.randint(1, 3) 
                else :
                    rand_op = 0
            # Select a random uid
            num = random.randint(1, bsize)
                
            #print "Thread %d, op %d, num %d, box %s" % (i, rand_op, num, box)

            if rand_op == 0 :
                typ, data = M.fetch(num, '(RFC822 FLAGS)')
            elif rand_op == 1 : 
                typ, data = M.fetch(num, '(RFC822 FLAGS)')
                flagged = 0
                if type(data[0]) is types.NoneType :
                    continue

                for flag in imaplib.ParseFlags(data[0][0]):
                    if (flag == '\Flagged') :
                        flagged = 1
                if flagged :
                    M.store(num, '-FLAGS', '(\\FLAGGED)')
                else :
                    M.store(num, '+FLAGS', '(\\FLAGGED)')

            elif rand_op == 2:
                M.store(num, '+FLAGS', '(\\Deleted)')
                M.expunge()
                bsize = bsize - 1;
            
            elif rand_op == 3:
                M.append(box, None, None, "I love to go a-wandering, along the mountain tracks...")
                bsize = bsize + 1

        M.logout()
        sys.exit(0)

for i in range(opt_threads) :
    os.wait()

t2 = time.time()

print "This experiment took %f seconds" % (t2 - t1)

# Empty out the mailbox
if 0:
    M = imaplib.IMAP4_SSL(SERVER)
    M.login(USER, PASS)
    for i in range (opt_mailboxes) :
        name = "box%d" % i
        M.delete(name)
    M.logout()

