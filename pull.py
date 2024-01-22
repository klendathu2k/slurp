#!/usr/bin/env python

import sys
import pathlib
from collections import deque
import sh
import time
from io import StringIO

cups = sh.Command("./cups.py")

dataset='mdc2'
hostname='lustre'

pipeline = deque([])
count = 0

runnumber = 0
segment   = 0
dstname = ""

class EventCounter:
    def __init__(self):
        self.nevents=0
        self.firstline=None
        self.lastline=""
    def __call__(self,data):
        self.nevents=self.nevents+1
        self.lastline=data.strip()
        if self.firstline==None:
            self.firstline=self.lastline


# Total number of events produced
nevents = 0

for line in sys.stdin:

    print( line.strip() )

    # Handle PRDF processing
    if 'Fun4AllRolloverFileOutStream' in line:

        pipeline.append( line.strip() )
        if len(pipeline)>1:

            count=count+1
            
            procline = pipeline.popleft().split()[-1]
            pathname = pathlib.Path(procline).parents[0]

            procline2 = procline.split('/')[-1]

            outfile = procline.split()[-1]

            array=outfile.strip(".prdf").split('-')

            dstname=array[0].split('/')[-1]     

            runnumber=int(array[1])
            segment=int(array[2])


            counter = EventCounter()
            sh.dpipe( procline, w=0,s='f',d='n',i=True, _out=counter )
            nevents = nevents + counter.nevents  # an actual event count (via dpipe)


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
                '--nevents',  f'{counter.nevents}',
                ])

            cups([
                '--run',      f'{runnumber}', 
                '--segment',  '0',
                '--dstname',  f'{dstname}',     
                'nevents',
                '--nevents', f'{nevents}',
            ])

            # And this is important... zero out the segment so that the aggregate count at the end iw correct
            segment = 0

    # file DST_CALOR_auau23_ana387_2023p003-00022027-0099.root, entries: 661
    elif "file" in line and ", entries:" in line:

        array = line.strip().split()
        #print(array)
        #print(array[1])

        nevents=array[-1]
        filename=array[1]
        print(filename, nevents)

        filename = filename.split('.')[0]

        dstname, runnumber, segment = filename.split('-')

        # Fall through to the aggregate below...

if runnumber>0: 
    cups([
        '--run',      f'{runnumber}', 
        '--segment',  f'{segment}',
        '--dstname',  f'{dstname}',     
        'finished',
        '-e', '-1',
        '--nsegments', f'{count}',
        '--nevents', f'{nevents}',
    ])
else:
    print("WARNING: ... no output to grep")
