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

statusdb  = pyodbc.connect("DSN=ProductionStatusWrite")
statusdbc = statusdb.cursor()

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

def getLatestId( dstname, run ):
    # There can (should) be only be one... but we will select the most recent to be safe
    query=f"""
    select id from dataset_status where dstname='{dstname}' and run={run} order by id desc limit 1;
    """
    result = statusdbc.execute(query).fetchone()[0]
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

    upsert=f"""
    update dataset_status
        set finalized='{now}',
            status='finalized',
            blame='{blame}'
    where dstname='{dstname}' and run={run};
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

    upsert=f"""
    update dataset_status
        set updated='{now}',
            status='updated',
            nsegments=nsegments+1,
            blame='{blame}'
    where dstname='{dstname}' and run={run};
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

    upsert=f"""
    update dataset_status
        set broken='{now}',
            status='broken',
            blame='{blame}'
    where dstname='{dstname}' and run={run};
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
