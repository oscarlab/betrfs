#!/usr/bin/python
# -*- coding: utf-8 -*-
#test.py
#Improved version of the aging grep test with blktrace, backups, and config files. Running without a config file will generate one with default values.

import os.path
import subprocess
import shlex
import time
import os
import sys

# the profile namedtuple (similar to a C struct) contains all the info in a
# profile
from collections import namedtuple
profile = namedtuple("profile", ["name", "mntpnt", "partition"])

# FNULL is used to reroute output of certain subprocesses to /dev/null
FNULL = open(os.devnull, 'w')

# tcolors is used to changed the terminal color
class tcolors:
 bold = '\033[1m'
 pullnumber = '\033[94m'
 initialization = '\033[92m'
 firsttimesetup = '\033[36m'
 end = '\033[0m'
 

################################################################################
# greptest
# runs a wall-timed test of how long it takes to grep a fixed random string
# recursively through the research directory. testno is used to distinguish the
# traces; profile distinguishes the profile (aged/clean/etc)

def greptest(testno, profile):
 if clear_cache == True:
  subprocess.check_call(shlex.split("bash remount.sh " + profile.name))
 time.sleep(1)
 print('blktrace -a read -d ' + profile.partition.rstrip('0123456789') + ' -o ' + test_name + '/' + profile.name + 'blktrace' + str(testno).zfill(4))
 subprocess.Popen('blktrace -a read -d ' + profile.partition.rstrip('0123456789') + ' -o ' + test_name + '/' + profile.name + 'blktrace' + str(testno).zfill(4), shell=True, stdout=FNULL, stderr=subprocess.STDOUT)
 time.sleep(2)
 print('grep -r --binary-files=text "' + grep_random + '" ' + profile.mntpnt)
 start = time.time()
 subprocess.call(shlex.split('grep -r --binary-files=text "' + grep_random + '" ' + profile.mntpnt), stderr=subprocess.STDOUT)
 stop = time.time() 
 time.sleep(2)
 print('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]).strip())
 subprocess.call('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]), shell=True, stdout=FNULL, stderr=subprocess.STDOUT)
 time.sleep(4)
 return (stop - start)

################################################################################
# process_layout
# processes the given blktrace file and computes the layout score

def process_layout(filename):
    print("processing " + filename + " to compute layout score")
    print('blkparse -a issue -f "%S %n\\n" -i ' + filename)
    while os.path.isfile("{}.blktrace.0".format(filename)) == False:
	time.sleep(1)
    time.sleep(1)
    #traceoutputfile = open("{}".format(filename.replace("blktrace","")),"w")
    blktrace_output = subprocess.check_output(shlex.split('blkparse -a issue -f "%S %n\n" -i ' + filename))
    blktrace_lines = blktrace_output.split('\n')
    blktrace_lines.pop() # removes the trailing empty string
    #print(blktrace_lines[-1])
    discont = -1 # number of discontiguous blocks
    total_blocks = 0 # total number of blocks
    last_sector = -1 # the last sector read
    for line in blktrace_lines:
        splitline = line.split()
        if len(splitline) != 0 and splitline[0].isdigit(): # this makes sure we're not at one of blkparse's trailing non-data lines
            sector = int(splitline[0])
            length = int(splitline[1])
            #traceoutputfile.write("{} {}\n".format(sector, length))
            if last_sector != sector:
                discont = discont + 1
            last_sector = sector + length
            total_blocks = total_blocks + length/8
    #traceoutputfile.close()
    if total_blocks != 0:
	return float(1) - float(discont)/float(total_blocks)
    else:
	return float(-1)

################################################################################
# initialization procedure

print(tcolors.initialization + "initializing test")

# check if first_time_setup.sh has been run and run it if not
try:
    subprocess.check_call('git config --global --list | grep "allowreachablesha1inwant"', shell=True, stdout=FNULL, stderr=subprocess.STDOUT)
except Exception, e:
    print(tcolors.firsttimesetup + "improper git configuration detected, running first time setup")
    subprocess.check_call(shlex.split('bash first_time_setup.sh'))
    print(tcolors.end)

# check if config.sh exists, and exit if not
if os.path.isfile('config.sh') == False :
	print "********************"
	print "config.sh doesn't exist"
	print "edit defconfig.sh and save as config.sh"
	print "exiting script"
	print "********************"
	exit()

# load variables from config.sh using the printconfig.sh helper script
print("loading configuration parameters from config.sh")
config = subprocess.check_output(shlex.split("bash printconfig.sh"))
config = config.split('\n')
config.pop() # pop the trailing empty string

# set optional profiles to empty
clean = profile._make(["", "", ""])
cleaner = profile._make(["", "", ""])

for item in config:
	tmp = item.split(" ", 1)
	param = tmp[0]
	value = tmp[1]
	if param == 'test_name':
		test_name = value
	if param == 'total_pulls':
		total_pulls = int(value)	
	if param == 'pulls_per_grep':
		pulls_per_grep = int(value)
	if param == 'grep_random':
		grep_random = value
        if param == 'gc_on':
                if value == "True":
                    gc_on = True
                else:
                    gc_on = False
        if param == 'keep_traces':
                if value == "True":
                    keep_traces = True
                else:
                    keep_traces = False
        if param == 'clear_cache':
                if value == "True":
                    clear_cache = True
                else:
                    clear_cache = False
	if param == 'aged':
		aged = profile._make(["aged", value.split()[0], value.split()[1]])
	if param == 'clean':
		clean = profile._make(["clean", value.split()[0], value.split()[1]])
	if param == 'cleaner':
		cleaner = profile._make(["cleaner", value.split()[0], value.split()[1]])

# set the git gc config option
if gc_on == True:
    print("enabling git gc:")
    print("git config --global --unset gc.auto")
    subprocess.call(shlex.split("git config --global --unset gc.auto"))
else:
    print("disabling git gc:")
    print("git config --global gc.auto 0")
    subprocess.check_call(shlex.split("git config --global gc.auto 0"))

# format the partitions
subprocess.check_call(shlex.split("bash format.sh aged"))
if (clean.name != ""):
	subprocess.check_call(shlex.split("bash format.sh clean"))
if (cleaner.name != ""):
	subprocess.check_call(shlex.split("bash format.sh cleaner"))

# create a dir to hold the results and traces
print("mkdir -p " + test_name)
subprocess.check_call(shlex.split("mkdir -p " + test_name))

# load the revision file, initialize the source location, and load the results file
rev = open('linuxrev.txt')
source = os.path.abspath('linux')
resultfile = open(test_name +'/' + test_name + 'results.csv', 'w')
resultfile.write("pulls_performed filesystem_size aged_time aged_layout_score")
if clean.mntpnt != '':
 resultfile.write(" clean_time clean_layout_score")
if cleaner.mntpnt != '':
 resultfile.write(" cleaner_time cleaner_layout_score")
resultfile.write("\n")

# initialize the target repo on the aged drive
print("initializing repo on target drive")
print("mkdir -p " + aged.mntpnt + "/linux")
subprocess.check_call(shlex.split("mkdir -p " + aged.mntpnt + "/linux"))
print("git init")
subprocess.check_call(shlex.split("git init"), cwd = aged.mntpnt + "/linux", stdout=FNULL, stderr=subprocess.STDOUT)

# make sure blktrace isn't running
try:
 subprocess.call('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]), shell=True)
except Exception, e:
 pass

print('initialization complete' + tcolors.end)


################################################################################
# main loop
for i in range(0, total_pulls + 1): 
 # checkout procedure
 currhash = rev.readline()
 checkout_command = 'git pull --no-edit -q -s recursive -X theirs ' + source + ' '  + currhash.strip()
 print(tcolors.pullnumber + str(i).zfill(6) + tcolors.end + ' ' + checkout_command)
 subprocess.check_call(shlex.split(checkout_command), cwd=aged.mntpnt + '/linux', stdout=FNULL, stderr=subprocess.STDOUT)

 # grep test
 if i % pulls_per_grep == 0:
  resultfile.write(str(i) + " ")
  print(tcolors.bold + '\nrunning aged grep test: ' + str(i/pulls_per_grep) + '\n' + tcolors.end)
  fssize = subprocess.check_output(shlex.split("du -s"), cwd=aged.mntpnt).split()[0]
  resultfile.write(str(fssize) + " ")
  agedresult = False
  #while agedresult == False: 
  agedgrep = greptest(i/pulls_per_grep, aged)
  try:
    aged_layout_score = process_layout(test_name + "/agedblktrace" + str(i/pulls_per_grep).zfill(4))
    #agedresult = True
  except Exception, e:
    aged_layout_score = 1
  resultfile.write(str(agedgrep) + " " + str(aged_layout_score) + " ")

  # clean grep test
  if clean.name != "":
   print(tcolors.bold + '\nrunning clean grep test: ' + str(i/pulls_per_grep) + '\n' + tcolors.end)
   subprocess.check_call(shlex.split("bash format.sh clean"))
   print("cp -a " + aged.mntpnt + "/linux " + clean.mntpnt)
   subprocess.check_output(shlex.split("cp -a " + aged.mntpnt + "/linux " + clean.mntpnt))
   cleanresult = False
   #while cleanresult == False: 
   cleangrep = greptest(i/pulls_per_grep, clean)
   try:
     clean_layout_score = process_layout(test_name + "/cleanblktrace" + str(i/pulls_per_grep).zfill(4))
   except Exception, e:
     clean_layout_score = 1
   resultfile.write(str(cleangrep) + " " + str(clean_layout_score) + " ")
  
  # cleaner grep test
  if cleaner.name != "":
   print(tcolors.bold + '\nrunning cleaner grep test: ' + str(i/pulls_per_grep) + '\n' + tcolors.end)
   subprocess.check_call(shlex.split("bash unmount.sh clean"))
   subprocess.check_call(shlex.split("bash unmount.sh cleaner"))
   print("dd if=" + clean.partition + " of=" + cleaner.partition + " bs=4M")
   subprocess.check_call(shlex.split("dd if=" + clean.partition + " of=" + cleaner.partition + " bs=4M"), stdout=FNULL, stderr=subprocess.STDOUT)
   subprocess.check_call(shlex.split("bash mount.sh clean"))
   subprocess.check_call(shlex.split("bash mount.sh cleaner"))
   cleanergrep = greptest(i/pulls_per_grep, cleaner)
   cleaner_layout_score = process_layout(test_name + "/cleanerblktrace" + str(i/pulls_per_grep).zfill(4))
   resultfile.write(str(cleanergrep) + " " + str(cleaner_layout_score) + " " )

  print(tcolors.bold + '\nresults of grep test ' + str(i/pulls_per_grep) + ':')
  print('grep test completed in ' + str(agedgrep) + ' seconds')
  if clean.name != "":
   print('clean test completed in ' + str(cleangrep) + ' seconds')
  if cleaner.name != "":
   print('cleaner test completed in ' + str(cleanergrep) + ' seconds')
  print('aged layout score: ' + str(aged_layout_score))
  if clean.name != "":
      print('clean layout score: ' + str(clean_layout_score))
  if cleaner.name != "":
      print('cleaner layout score: ' + str(cleaner_layout_score))
  print(tcolors.end)

  if keep_traces == False:
      print("deleting traces")
      print("rm " + test_name + "/*blktrace*")
      subprocess.call("rm " + test_name + "/*blktrace*", shell=True)
  resultfile.write("\n")
  resultfile.flush()

#end of main loop

try:
 subprocess.call(['kill', '-15', subprocess.check_output(["pidof","-s","blktrace"])])
except Exception, e:
 pass

