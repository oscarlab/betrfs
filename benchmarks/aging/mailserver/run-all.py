# -*- coding: utf-8 -*-
#run-all.py
#Improved version of the aging grep test with blktrace, backups, and config files. Running without a config file will generate one with default values.

import time
import subprocess
from subprocess import CalledProcessError
import ConfigParser
import os.path
import sys

#Global variables: parameters for the test. Assigned by initialize().
WorkingDir = ''
FSType = ''
CleanTest = True

#initialize checks if there is a config file, and reads it if it exists. If no config file exists, it creates one with default values.
def initialize():
 Config = ConfigParser.ConfigParser()
 if os.path.isfile('config.ini'):
  print('Config file found; reading values.')
  Config.read('config.ini')
  try:
   global WorkingDir
   WorkingDir = Config.get('Parameters','WorkingDir')
   global FSType
   FSType = Config.get('Parameters','FSType')
   global CleanTest
   CleanTest = Config.getboolean('Parameters','CleanTest')
  except:
   print('An exception occured while reading the config file.')
   sys.exit()
  print('Configuration loaded.')
 return()

#subprocess.check_call('python populate-conf.py', shell=True)

initialize()

#make sure blktrace isn't running
try:
 subprocess.call('kill -15 ' + subprocess.check_output(["pidof","-s","blktrace"]), shell=True)
except Exception, e:
 pass

#mount fs
if FSType == 'betrfs':
 subprocess.check_call( '../../setup-ftfs.sh', shell=True)
else:
 subprocess.check_call( '../../setup-' + FSType +'.sh', shell=True)

subprocess.check_call('sudo dovecot', shell=True)
#check if benchmark exists
#setup repos
if os.path.isdir('benchmark') == False:
 subprocess.call('sudo bash mailserver-setupdir.sh', shell=True)
 subprocess.call('sudo python mailserver-setup.py', shell=True)
else:
 print 'copying benchmark dir over'
 subprocess.call('cp -a benchmark/* /mnt/benchmark', shell=True) 
 subprocess.call('chown -R ftfstest1:ftfstest1 /mnt/benchmark/ftfstest1', shell=True) 
 subprocess.call('chown -R ftfstest2:ftfstest2 /mnt/benchmark/ftfstest2', shell=True) 
#setup test always
subprocess.check_call('sudo python runtest.py ', shell=True)
#run cleantest.py
if CleanTest == True:
 #mount clean fs
 subprocess.check_call('python runcleantest.py', shell=True)
subprocess.check_call('sudo dovecot stop', shell=True)
