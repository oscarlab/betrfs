#! /bin/python
import sys

try:
	if(len(sys.argv) <= 1):
		print("check.py should be run with the name of the output file to check.")
		exit()

	check_file = sys.argv[1]
	checkf = open(check_file, "r")
	for line in checkf:
		stop_str = "end trace"
		if stop_str in line:
			print line
except: 
	print "there is a exception, please check the code"
