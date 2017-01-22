#resultify.py outputs results to csv

import csv
import json
import subprocess

with open('results.json') as datafile:
 data = json.load(datafile)

resultfile = open('results.csv','wb')
resultwriter = csv.writer(resultfile,delimiter=',')
resultwriter.writerow( ('FS Size', 'Aged Time', 'Clean Time', 'Aged Total Seek Distance', 'Clean Total Seek Distance') )

for i in range(0,len(data)):
 print('Processing test ' + str(i))
 #Compute seeks and sectors read form blktrace
 subprocess.call('blkparse -q -a issue -f "%S %n \n" -i trace' + str(i).zfill(6) + '.blktrace.0 > agedgrep.txt', cwd='traces', shell=True)
 totalseekdistance = 0
 #totalsectors = 0
 with open('traces/agedgrep.txt') as tracefile:
  lastsector = -1
  for line in tracefile:
    splitline = line.split()
    if splitline[0].isdigit():
     sector = int(splitline[0])
     numblocks = int(splitline[1])
     totalseekdistance = totalseekdistance + abs(sector - lastsector)
     #if lastsector != sector:
     # seeks = seeks + 1
     #totalsectors = totalsectors + numblocks
     lastsector = sector + numblocks
 
 #compute clean reads and sectors read from blktrace
 subprocess.call('blkparse -q -a issue -f "%S %n \n" -i cleantrace' + str(i).zfill(6) + '.blktrace.0 > cleangrep.txt', cwd='traces', shell=True)
 cleanseekdistance = 0
 #cleantotalsectors = 0
 with open('traces/cleangrep.txt') as tracefile:
  lastsector = -1
  for line in tracefile:
   splitline = line.split()
   if splitline[0].isdigit():
    sector = int(splitline[0])
    numblocks = int(splitline[1])
    cleanseekdistance = cleanseekdistance + abs(sector - lastsector)
    #if lastsector != sector:
    # cleanseeks = cleanseeks + 1
    #cleantotalsectors = cleantotalsectors + numblocks
    lastsector = sector + numblocks

 resultrow = (data[i] + [totalseekdistance, cleanseekdistance])
 resultwriter.writerow(resultrow)

 subprocess.call('sudo rm -f *.txt', cwd='traces', shell=True) 
