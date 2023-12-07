#!/usr/bin/env python

"""
Common User Production Script
"""

import pyodbc
import sh
import argparse
import pprint
import datetime
import time
import random

fc = pyodbc.connect("DSN=FileCatalog")
fcc = fc.cursor()


"""
cups.py -t tablename state  dstname run segment [-e exitcode -n nsegments]
"""

# https://mike.depalatis.net/blog/simplifying-argparse

parser     = argparse.ArgumentParser(prog='cups')
subparsers = parser.add_subparsers(dest="subcommand")

parser.add_argument( "-t","--table"  , dest="table"     , default="status_dst_calor_auau23_ana387_2023p003",help="Sets the name of the production status table table")
parser.add_argument( "-r","--run"    , dest="run"       , default=None,help="Sets the run number for the update",required=True)
parser.add_argument( "-s","--segment", dest="segment"   , default=None,help="Sets the segment number for the update",required=True)
parser.add_argument( "--timestamp"   , dest="timestamp" , default=str(datetime.datetime.utcnow()),help="Sets the timestamp, default is now (and highly recommended)" )

def subcommand(args=[], parent=subparsers):
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator

def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)

@subcommand()
def submitting(args):
    """
    Executed by slurp when the jobs are being submitted to condor.
    """
    tablename=args.table
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='submitting',timestamp='%s'
             where run=%i and segment=%i
    """%(tablename,timestamp,int(run),int(seg))
    print(query)

@subcommand()
def submitted(args):
    """
    Executed by slurp when the jobs have been submitted to condor.
    """
    tablename=args.table
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='submitted',timestamp='%s'
             where run=%i and segment=%i
    """%(tablename,timestamp,int(run),int(seg))
    print(query)

@subcommand()
def started(args):
    """
    Executed by the user payload script when the job is started
    """
    tablename=args.table
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='started',timestamp='%s'
             where run=%i and segment=%i
    """%(tablename,timestamp,int(run),int(seg))
    print(query)

@subcommand()
def running(args):
    """
    Executed by the user payload script when the job begins executing the payload macro.
    """
    tablename=args.table
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='running',timestamp='%s'
             where run=%i and segment=%i
    """%(tablename,timestamp,int(run),int(seg))
    print(query)


@subcommand([
    argument("-e","--exit",help="Exit code of the payload macro",dest="exit",default=-1),
    argument("-n","--nsegments",help="Number of segments produced",dest="nsegments",default=1),
])
def finished(args):
    """
    Executed by the user payload script when the job finishes executing the payload macro.
    If exit code is nonzer, state will be marked as failed.
    """
    tablename=args.table
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    ec=args.exit
    ns=args.nsegments    
    state=finished
    if int(ec)>0:
        state=failed
    if int(ec)<0:
        ec=0
    
    query="""update %s 
             set status='%s',timestamp='%s',exit_code=%i,nsegments=%i
             where run=%i and segment=%i
    """%(tablename,state,timestamp,int(ec),int(ns),int(run),int(seg))
    print(query)


@subcommand([
    argument( "script",       help="name of the script to exeute"),
    argument( "scriptargs",   help="arguments for the script", nargs="*" ),
])
def execute(args):
    """
    Execute user script.  Exit code will be set to the exit status of the payload
    script, rather than the payload macro.
    """
    
    time.sleep( random.randint(1,10) )

    # We have to go through the parser to run these subcommands, and we don't want
    # to shell out... otherwise we make another DB connection

    # Flag job as started...
    started = parser.parse_args( ["-r",args.run,"-s",args.segment,"started"] ); 
    started.func( started )

    time.sleep( random.randint(1,10) )

    # And immediately drop into running
    running = parser.parse_args( ["-r","12345","-s","6789","running"] );
    running.func(running)

    time.sleep( random.randint(1,10) )

    # Execute the user payload
    print(args)

    time.sleep( random.randint(1,10) )

    # And finally handle the finished state
    finished=parser.parse_args( ["-r","12345","-s","6789","finished","-e","0"] ); 
    finished.func(finished)
    

def main():

    args=parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)        







if __name__ == '__main__':
    main()
