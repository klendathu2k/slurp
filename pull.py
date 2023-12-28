#!/usr/bin/env python

import sys
import pathlib
from collections import deque
import sh
import time

cups = sh.Command("./cups.py")

dataset='mdc2'
hostname='lustre'

pipeline = deque([])
count = 0

runnumber = 0
dstname = ""

for line in sys.stdin:
    if 'Fun4AllRolloverFileOutStream' in line:
        print( line.strip() )
        pipeline.append( line.strip() )
        if len(pipeline)>1:

            count=count+1
            
            procline = pipeline.popleft().split()[-1]
            pathname = pathlib.Path(procline).parents[0]

            procline2 = procline.split('/')[-1]

            outfile = procline.split()[-1]
            #print(outfile)


            #print(outfile)
            array=outfile.strip(".prdf").split('-')
            #print(array)            
            dstname=array[0].split('/')[-1]     

            runnumber=int(array[1])
            segment=int(array[2])



            #print( dstname, runnumber, segment )

            # ./cups.py -r 22026 -s 41 -d DST_EVENT_auau23_ana393_2023p009 catalog --ext prdf --path ... --dataset mdc2 --hostname lustre
            # print(f"cups.py -r {runnumber} -s {segment} -d {dstname} catalog --ext prdf --path {pathname} --dataset mdc2 --hostname lustre")

            # Register the file with the file catalog
            cups([ 
                '--run',      f'{runnumber}', 
                '--segment',  f'{segment}', 
                '--dstname',  f'{dstname}', 
                'catalog',
                '--ext',      f'prdf',
                '--path',     f'{pathname}',
                '--dataset',  f'{dataset}',
                '--hostname', f'{hostname}',
                ])

            time.sleep(120)
                
#./cups.py -r ${runnumber} -s 0 -d DST_EVENT_auau23_${build}_${dbtag} finished -e ${status_f4a} --nsegments ${count}

cups([
    '--run',      f'{runnumber}', 
    '--segment',  '0',
    '--dstname',  f'{dstname}',     
    'finished',
    '-e', '-1',
    '--nsegments', f'{count}'m
])
