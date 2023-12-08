#!/usr/bin/env python

"""
Common User Production Script
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

fc = pyodbc.connect("DSN=FileCatalog")
fcc = fc.cursor()

"""
cups.py -t tablename state  dstname run segment [-e exitcode -n nsegments]
"""

# Little helper to print st8 to stderr
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# https://mike.depalatis.net/blog/simplifying-argparse

parser     = argparse.ArgumentParser(prog='cups')
subparsers = parser.add_subparsers(dest="subcommand")

parser.add_argument( "--no-update",    dest="noupdate"  , default=False, action="store_true", help="Does not update the DB table")
parser.add_argument( "-t","--table"  , dest="table"     , default="status_dst_calor_auau23_ana387_2023p003",help="Sets the name of the production status table table")
parser.add_argument( "-d","--dstname", dest="dstname"   ,                                                   help="Set the DST name eg DST_CALO_auau1", required=True)
parser.add_argument( "-r","--run"    , dest="run"       , default=None,help="Sets the run number for the update",required=True)
parser.add_argument( "-s","--segment", dest="segment"   , default=None,help="Sets the segment number for the update",required=True)
parser.add_argument( "--timestamp"   , dest="timestamp" , default=str(datetime.datetime.utcnow()),help="Sets the timestamp, default is now (and highly recommended)" )


def handler( signum, frame ):
    from sh import uname
    from sh import ls
    from sh import pwd
    from sh import du
    signame = signal.Signals(signum).name
    eprint(f'Signal handler caught {signame} ({signum})')
    unm = uname("-a")
    eprint(f'{unm}')
    pwd_ = pwd()
    eprint(f'{pwd_}')
    ls_ = ls("-la")
    eprint(f'{ls_}')
    du_ = du("--human-readable","--total","--summarize",".")
    eprint(f'{du_}')
            
# Setup signal handling
signal.signal(signal.SIGINT,  handler)
signal.signal(signal.SIGTERM, handler)
#signal.signal(signal.SIGSTOP, handler)
#signal.signal(signal.SIGKILL, handler)


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
    tablename=args.tabl
    dstname=args.dstname
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='submitting',submitting='%s'
             where dstname=%s  run=%i and segment=%i
    """%(tablename,dstname,timestamp,int(run),int(seg))
    if args.noupdate:
        print(query)

@subcommand()
def submitted(args):
    """
    Executed by slurp when the jobs have been submitted to condor.
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='submitted',submitted='%s'
             where dstname=%s  run=%i and segment=%i
    """%(tablename,dstname,timestamp,int(run),int(seg))
    if args.noupdate:
        print(query)


@subcommand()
def started(args):
    """
    Executed by the user payload script when the job is started
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='started',started='%s'
             where dstname=%s  run=%i and segment=%i
    """%(tablename,dstname,timestamp,int(run),int(seg))
    if args.noupdate:
        print(query)

@subcommand()
def running(args):
    """
    Executed by the user payload script when the job begins executing the payload macro.
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    query="""update %s 
             set status='running',running='%s'
             where dstname=%s  run=%i and segment=%i
    """%(tablename,dstname,timestamp,int(run),int(seg))
    if args.noupdate:
        print(query)

@subcommand([
    argument("-e","--exit",help="Exit code of the payload macro",dest="exit",default=-1),
])
def failed(args):
    """
    Executed by the user payload script when the job begins executing the payload macro.
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=args.run
    seg=args.segment
    code=args.exit
    query="""update %s 
             set status='failed',failed='%s',exit_code=%i
             where dstname=%s  run=%i and segment=%i
    """%(tablename,timestamp,int(code),dstname,int(run),int(seg))
    if args.noupdate:
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
    dstname=args.dstname
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
             set status='%s',%s='%s',exit_code=%i,nsegments=%i
             where dstname=%s  run=%i and segment=%i
    """%(tablename,state,state,timestamp,int(ec),int(ns),dstname,int(run),int(seg))



    if args.noupdate:
        print(query)


@subcommand([
    argument( "script",       help="name of the script to exeute"),
    argument( "scriptargs",   help="arguments for the script", nargs="*" ),
    argument( "--stdout", type=argparse.FileType(mode='a'), help="Open output file to redirect script stdout", default=sys.stdout, dest="stdout" ),
    argument( "--stderr", type=argparse.FileType(mode='a'), help="Open output file to redirect script stderr", default=sys.stderr, dest="stderr" ),
])
def execute(args):
    """
    Execute user script.  Exit code will be set to the exit status of the payload
    script, rather than the payload macro.
    """

#   time.sleep( random.randint(1,10) )


    # We have to go through the parser to run these subcommands, and we don't want
    # to shell out... otherwise we make another DB connection

    # Flag job as started...
    started = parser.parse_args( ["-d", args.dstname, "-r", args.run,"-s",args.segment,"started"] ); 
    started.func( started )

    # And immediately drop into running
    running = parser.parse_args( ["-d", args.dstname, "-r", args.run, "-s", args.segment, "running"] );
    running.func(running)

    #
    # Execute the user payload.  Exit 
    #
    exit_code = 0
    cmd = sh.Command(args.script)    
        
    result = cmd( args.scriptargs, _out=args.stdout, _err=args.stderr, _ok_code=range(1,255) )
    exit_code = result.exit_code
        
    state = "finished"
    if exit_code>0: 
        state = "failed"
            
    finished=parser.parse_args( ["-d", args.dstname, "-r", args.run, "-s", args.segment, state, "-e","%s"%result.exit_code ] ); 
    finished.func(finished)



def main():

    args=parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)        

if __name__ == '__main__':
    main()
