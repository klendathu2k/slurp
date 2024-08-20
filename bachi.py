#!/usr/bin/env python3

"""
bachi (japanese for ramen bowl) ... script for dataset creation, finalization and status updates

Manages the production dataset status table

"""

import pyodbc
import argparse
import pprint
import datetime
import time
import random
import sh
import sys
import signal
import json
import hashlib
import os
import shutil

try:
    statusdb  = pyodbc.connect("DSN=ProductionStatusWrite")
    statusdbc = statusdb.cursor()
except pyodbc.InterfaceError:
    for s in [ 10*random.random(), 20*random.random(), 30*random.random(), 60*random.random(), 120*random.random() ]:
        print(f"Could not connect to DB... retry in {s}s")
        time.sleep(s)
        try:
            statusdb  = pyodbc.connect("DSN=ProductionStatusWrite")
            statusdbc = statusdb.cursor()
        except:
            pass

try:
    statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
    statusdbr = statusdbr_.cursor()
except pyodbc.InterfaceError:
    for s in [ 10*random.random(), 20*random.random(), 30*random.random(), 60*random.random(), 120*random.random() ]:
        print(f"Could not connect to DB... retry in {s}s")
        time.sleep(s)
        try:
            statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
            statusdbr = statusdbr_.cursor()
        except:
            pass
except pyodbc.Error as e:
    print(e)
    exit(1)

parser     = argparse.ArgumentParser(prog='bachi')
subparsers = parser.add_subparsers(dest="subcommand")

parser.add_argument("--blame", default="bachi" )
parser.add_argument("--timestamp"   , 
                    dest="timestamp" , 
                    default=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  ),
                    help="Sets the timestamp, default is now (and highly recommended)" 
                )

def subcommand(args=[], parent=subparsers):
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator

def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)


def getLatestId( tablename, dstname, run, seg=None ):

    cache="cups.cache"

    result  = 0
    query=f"""
    select id,dstname from {tablename} where run={run} order by id desc limit 1;
    """
    results = list( statusdbr.execute(query).fetchall() )

    # Find the most recent ID with the given dstname
    for r in results:
        if r.dstname == dstname:
            result = r.id
            break

    if result==0: print(f"Warning: could not find {dstname} with run={run} seg={seg}... this may not end well.")
    return result


@subcommand([
    argument( "DSTNAME", help="Specifies the dataset name" ),
    argument( "RUN",     help="Specifies the run  (or first/last run) in the dataset", type=int, action="extend", nargs="+" ),
    argument( "--parent", help="Specify the parent of this dataset", default=None ),
])
def created(args):
    """
    Signals the creation of a dataset
    """
    dstname = args.DSTNAME
    run     = args.RUN[0]
    lastrun = 0
    if len( args.RUN ) > 1: 
        lastrun = args.RUN[1]
    blame   = args.blame
    status  = "created "

    now = str( args.timestamp )
    
    upsert=""

    if args.parent:
        upsert=f"""
        insert into dataset_status (   dstname  ,  run ,  lastrun,  revision, created,  status,    blame,  parent )
        values                     ( '{dstname}', {run}, {lastrun},        1, '{now}',  'created', '{blame}', '{args.parent}' )
        on conflict
        on constraint dataset_status_pkey
        do update set
               revision=dataset_status.revision+1,
               created=EXCLUDED.created,
               status=EXCLUDED.status,
               blame=EXCLUDED.blame
        """
    else:
        upsert=f"""
        insert into dataset_status (   dstname  ,  run ,  lastrun,  revision, created,  status,    blame )
        values                     ( '{dstname}', {run}, {lastrun},        1, '{now}',  'created', '{blame}' )
        on conflict
        on constraint dataset_status_pkey
        do update set
               revision=dataset_status.revision+1,
               created=EXCLUDED.created,
               status=EXCLUDED.status,
               blame=EXCLUDED.blame
        """


    print(upsert)

    statusdbc.execute( upsert )
    statusdbc.commit()


@subcommand([
    argument( "DSTNAME", help="Specifies the dataset name" ),
    argument( "RUN",     help="Specifies the run  (or first/last run) in the dataset", type=int, action="extend", nargs="+" )
    
])
def finalized(args):
    """
    Signals the finalization of a dataset
    """
    dstname = args.DSTNAME
    run     = args.RUN[0]
    lastrun = 0
    if len( args.RUN ) > 1: 
        lastrun = args.RUN[1]
    blame   = args.blame
    status  = "finalized"

    now = str( args.timestamp )

    id_ = getLatestId('dataset_status',dstname,run)
    upsert=f"""
    update dataset_status
        set finalized='{now}',
            status='finalized',
            blame='{blame}'
    where id={id_};
    """
    
    statusdbc.execute( upsert )
    statusdbc.commit()

@subcommand([
    argument( "DSTNAME", help="Specifies the dataset name" ),
    argument( "RUN",     help="Specifies the run  (or first/last run) in the dataset", type=int, action="extend", nargs="+" )
    
])
def updated(args):
    """
    Signals that a file is added to the dataset
    """
    dstname = args.DSTNAME
    run     = args.RUN[0]
    lastrun = 0
    if len( args.RUN ) > 1: 
        lastrun = args.RUN[1]
    blame   = args.blame
    status  = "updated"

    now = str( args.timestamp )

    id_ = getLatestId('dataset_status',dstname,run)
    upsert=f"""
    update dataset_status
        set updated='{now}',
            status='updated',
            nsegments=nsegments+1,
            blame='{blame}'
    where id={id_};
    """    

    statusdbc.execute( upsert )
    statusdbc.commit()


@subcommand([
    argument( "DSTNAME", help="Specifies the dataset name" ),
    argument( "RUN",     help="Specifies the run  (or first/last run) in the dataset", type=int, action="extend", nargs="+" )
    
])
def broken(args):
    """
    Signals the finalization of a dataset
    """
    dstname = args.DSTNAME
    run     = args.RUN[0]
    lastrun = 0
    if len( args.RUN ) > 1: 
        lastrun = args.RUN[1]
    blame   = args.blame
    status  = "broken"

    now = str( args.timestamp )
    id_ = getLatestId('dataset_status',dstname,run)

    upsert=f"""
    update dataset_status
        set broken='{now}',
            status='broken',
            blame='{blame}'
    where id={id_};
    """
    
    statusdbc.execute( upsert )
    statusdbc.commit()




def main():

    args=parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)        

if __name__ == '__main__':
    main()
