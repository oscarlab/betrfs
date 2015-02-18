import sys
import os.path
import random
import time
fname = sys.argv[1];
filesize = int(sys.argv[2]);
if not os.path.isfile(fname):
	print fname + ' is not a file'
	sys.exit(2);
file = open(fname, "rb+")
randomwrites = 1000
start = time.time()
for i in range(1, randomwrites):
	pos = random.randint(1,filesize - 4)
	file.seek(pos)
	file.write(bytearray([20, 10, 5, 87]))
end = time.time()
print 'Time Taken: ' + str(end - start)
file.close()
