#resultify.py outputs results to csv

import csv
import json
import subprocess

with open('results.json') as datafile:
 data = json.load(datafile)

resultfile = open('results.csv','wb')
resultwriter = csv.writer(resultfile,delimiter=' ')
resultwriter.writerow( ('FS_Size', 'Aged_Time', 'Clean_Time', 'Aged_Layout_Score', 'Clean_Layout_Score') )


for i in range(0,len(data)):
 print('Processing test ' + str(i))
 subprocess.call('blkparse -q -a issue -f "%S %n \n" -i trace' + str(i).zfill(6) + '.blktrace.0 > agedgrep.txt', cwd='traces', shell=True)

 discontiguities = -1
 totalblocks = 0
 with open('traces/agedgrep.txt') as tracefile:
  lastsector = -1
  for line in tracefile:
    splitline = line.split()
    if splitline[0].isdigit():
     sector = int(splitline[0])
     length = int(splitline[1])
     if lastsector != sector:
      discontiguities = discontiguities + 1
     lastsector = sector + length
     totalblocks = totalblocks + length/8

 aged_layout_score = str(float(1) - float(discontiguities)/float(totalblocks))
 
 subprocess.call('blkparse -q -a issue -f "%S %n \n" -i cleantrace' + str(i).zfill(6) + '.blktrace.0 > cleangrep.txt', cwd='traces', shell=True)

 discontiguities = -1
 totalblocks = 0
 with open('traces/cleangrep.txt') as tracefile:
  lastsector = -1
  for line in tracefile:
    splitline = line.split()
    if splitline[0].isdigit():
     sector = int(splitline[0])
     length = int(splitline[1])
     if lastsector != sector:
      discontiguities = discontiguities + 1
     lastsector = sector + length
     totalblocks = totalblocks + length/8

 clean_layout_score = str(float(1) - float(discontiguities)/float(totalblocks))

 resultrow = (data[i] + [aged_layout_score, clean_layout_score])
 resultwriter.writerow(resultrow)

 subprocess.call('sudo rm -f *.txt', cwd='traces', shell=True) 
