# -*- coding: utf-8 -*-
#test.py
#Improved version of the aging grep test with blktrace, backups, and config files. Running without a config file will generate one with default values.

import time
import subprocess
import ConfigParser
import os.path
import sys
import json
import argparse

#Global variables: parameters for the test. Assigned by initialize().
WorkingDir = ''
WorkingPartition = ''
GREPRandom = ''
CheckoutsPerGREP = 0
GREPsPerBackup = 0
TotalCheckouts = 0
FSType = ''
PreTestRemount = True
WriteAmpLength = 0

#initialize checks if there is a config file, and reads it if it exists. If no config file exists, it creates one with default values.
def initialize():
 Config = ConfigParser.ConfigParser()
 if os.path.isfile('config.ini'):
  print('Config file found; reading values.')
  Config.read('config.ini')
  try:
   global WorkingDir
   WorkingDir = Config.get('Parameters','WorkingDir')
   global WorkingPartition
   WorkingPartition = Config.get('Parameters','WorkingPartition')
   global GREPRandom
   GREPRandom = Config.get('Parameters','GREPRandom')
   global CheckoutsPerGREP
   CheckoutsPerGREP = Config.getint('Parameters','CheckoutsPerGREP')
   global TotalCheckouts
   TotalCheckouts = Config.getint('Parameters','TotalCheckouts')  
   global FSType
   FSType = Config.get('Parameters','FSType')
   global PreTestRemount
   PreTestRemount = Config.getboolean('Parameters','PreTestRemount')
   global WriteAmpLength
   WriteAmpLength = Config.getint('Parameters','WriteAmpLength')
  except:
   print('An exception occured while reading the config file.')
   sys.exit()
  print('Configuration loaded.')

def cleangreptest(testno):
 subprocess.check_call('rm -rf *', cwd='clean_temp', shell=True)
 subprocess.check_call('cp -a ' + WorkingDir + '/* clean_temp', shell=True)
 subprocess.check_call( '../../cleanup-fs.sh' , shell=True)
 if FSType == 'betrfs':
  subprocess.check_call( '../../setup-ftfs.sh', shell=True)
 else:
  subprocess.check_call( '../../setup-' + FSType + '.sh', shell=True)
 subprocess.check_call('cp -a clean_temp/* ' + WorkingDir, shell=True)
 if(PreTestRemount):
  if(FSType=='betrfs'):
   subprocess.check_call( '../../cleanup-fs.sh' , shell=True)
   subprocess.check_call( '../../mount-ftfs.sh' , shell=True)
  elif(FSType=='zfs'):
   subprocess.call('free && sync && echo 3 > /proc/sys/vm/drop_caches && free', shell=True)
   subprocess.check_call( 'umount ' + WorkingDir, shell=True)
   subprocess.check_call('zfs mount -a', shell=True)
  else:
   subprocess.check_call( '../../cleanup-fs.sh' , shell=True)
   subprocess.check_call('mount ' + WorkingPartition + ' ' + WorkingDir, shell=True)
 else:
  subprocess.call('free && sync && echo 3 > /proc/sys/vm/drop_caches && free', shell=True)
 subprocess.call('blktrace -a read -d ' + WorkingPartition.rstrip('0123456789') + ' -o traces/cleantrace' + str(testno).zfill(6) + ' &', shell=True)
 start = time.time()
 subprocess.call('(grep -r --binary-files=text "' + GREPRandom  + '" ' + WorkingDir +') > /dev/null', stderr=subprocess.STDOUT, shell=True)
 stop = time.time() 
 subprocess.call('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]), shell=True)
 return (stop - start)

##################################################################################
# Main Script Begins Here

initialize()
results= []

for i in range(100): 
 subprocess.check_call('sudo python mailserver-aging.py', shell=True)
 print('Running clean grep test: ' + str(i))
 thisgrep = cleangreptest(i)
 print('clean grep test completed in ' + str(thisgrep) + ' seconds')
 results.append(thisgrep)
 with open('cleanresults.json','w') as outfile:
  json.dump(results, outfile)
 if(PreTestRemount):
  if(FSType=='betrfs'):
   subprocess.check_call( '../../cleanup-fs.sh' , shell=True)
   subprocess.check_call( '../../mount-ftfs.sh' , shell=True)
  elif(FSType=='zfs'):
   subprocess.call('free && sync && echo 3 > /proc/sys/vm/drop_caches && free', shell=True)
   subprocess.check_call( 'umount ' + WorkingDir, shell=True)
   subprocess.check_call('zfs mount -a', shell=True)
  else:
   subprocess.check_call( '../../cleanup-fs.sh' , shell=True)
   subprocess.check_call('mount ' + WorkingPartition + ' ' + WorkingDir, shell=True)
 else:
  subprocess.call('free && sync && echo 3 > /proc/sys/vm/drop_caches && free', shell=True)
 time.sleep(1)
 subprocess.call('blktrace -a write -d ' + WorkingPartition.rstrip('0123456789') + ' -o traces/cleanwritetrace' + str(i).zfill(6) + ' &', shell=True)
 subprocess.call('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]), shell=True)
 time.sleep(1)

try:
 subprocess.call('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]), shell=True)
except Exception, e:
 pass
