#!/usr/bin/env python
import sh
from time import sleep
from contextlib import redirect_stdout
import sys
import argparse
import pyodbc
from tabulate import tabulate
import signal
import pprint
import os
import datetime
import time
import dateutil.parser
from colorama import Fore, Back, Style, init

init()

# Colorize tables
def colorize(row,fail_color=(Back.RED,Fore.WHITE)):
    color=f"{Fore.GREEN}{Style.BRIGHT}"
    reset=f"{Style.RESET_ALL}{Fore.RESET}"
    if row.num_failed>0:  
        color=f"{fail_color[0]}{fail_color[1]}{Style.BRIGHT}"
        reset=f"{Fore.RESET}{Back.RESET}{Style.RESET_ALL}"
    myrow = [ 
        f"{color}{element}{reset}"   if (element!=None) else ""
        for element in list(row) 
    ]
    return myrow




statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
statusdbr = statusdbr_.cursor()

timestart=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )
time.sleep(1)

# https://mike.depalatis.net/blog/simplifying-argparse

parser     = argparse.ArgumentParser(prog='ramenya2')
subparsers = parser.add_subparsers(dest="subcommand")

parser.add_argument( "-v", "--verbose",dest="verbose"   , default=False, action="store_true", help="Sets verbose output")
#parser.add_argument( '--runs', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[0,999999] )
#parser.add_argument( '--config',help="Specifies the configuration file used by kaedama to specify workflows", default='sphenix_auau23.yaml')
#parser.add_argument( '--rules', nargs='+', default="['all']" )
#parser.add_argument( '--delay', help="Delay between loop executions",default=600)
#parser.add_argument( '--submit', help="Submit jobs to condor",default=True,action="store_true")
#parser.add_argument( '--no-submit', help="No submission, just print the summary information",action="store_false",dest="submit")
#parser.add_argument( '--outputs',help="Information printed at each loop",nargs='+', default=['started'] )
#parser.add_argument( '--once',help="Break out of the loop after one iteration",default=False,action="store_true")

#parser.add_argument( "--no-update",    dest="noupdate"  , default=False, action="store_true", help="Does not update the DB table")
#parser.add_argument( "-t","--table"  , dest="table"     , default="production_status",help="Sets the name of the production status table table")
#parser.add_argument( "-d","--dstname", dest="dstname"   ,                                                   help="Set the DST name eg DST_CALO_auau1", required=True)
#parser.add_argument( "-r","--run"    , dest="run"       , default=None,help="Sets the run number for the update",required=True)
#parser.add_argument( "-s","--segment", dest="segment"   , default=None,help="Sets the segment number for the update",required=True)
#parser.add_argument( "--timestamp"   , dest="timestamp" , default=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  ),
#                     help="Sets the timestamp, default is now (and highly recommended)" )


#def handler( signum, frame ):
#    from sh import uname
#    from sh import ls
#    from sh import pwd
#    from sh import du
#    signame = signal.Signals(signum).name
#    eprint(f'Signal handler caught {signame} ({signum})')
#    unm = uname("-a")
#    eprint(f'{unm}')
#    pwd_ = pwd()
#    eprint(f'{pwd_}')
#    ls_ = ls("-la")
#    eprint(f'{ls_}')
#    du_ = du("--human-readable","--total","--summarize",".")
#    eprint(f'{du_}')
#            
# Setup signal handling
#signal.signal(signal.SIGINT,  handler)
#signal.signal(signal.SIGTERM, handler)
#signal.signal(signal.SIGSTOP, handler)
#signal.signal(signal.SIGKILL, handler)

def clear(): 
    sleep(10)
    print("\033c\033[3J", end='')

def subcommand(args=[], parent=subparsers):
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator

def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)

def query_pending_jobs( conditions="" ):
    print("Summary of jobs which have not reached staus='started'")
    #print("------------------------------------------------------")
    psqlquery=f"""
    select dsttype,prod_id,
    count(run)                        as num_jobs           ,
    avg(age(submitted,submitting))    as avg_time_to_submit ,
    min(age(submitted,submitting))    as min_time_to_submit ,
    max(age(submitted,submitting))    as max_time_to_submit
       
    from   production_status 
    where  status<='started' and submitted>'{timestart}'
       {conditions}
    group by dsttype,prod_id
    order by dsttype desc
    ;
    """    
    results = statusdbr.execute(psqlquery);
    labels  = [ f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
    table = [colorize(r) for r in results]
    print( tabulate( table, labels, tablefmt="psql" ) )


def query_started_jobs(conditions=""):
    print("Summary of jobs which have reached staus='started'")
    #print("--------------------------------------------------")
    psqlquery=f"""
    select dsttype,prod_id,
    count(run)                      as num_jobs,
    avg(age(started,submitting))    as avg_time_to_start,
    count( case status when 'submitted' then 1 else null end )
    as num_submitted,
    count( case status when 'running' then 1 else null end )
    as num_running,
    count( case status when 'finished' then 1 else null end )
    as num_finished,
    count( case status when 'failed' then 1 else null end )
    as num_failed,
    avg(age(ended,started))         as avg_job_duration,
    min(age(ended,started))         as min_job_duration,
    max(age(ended,started))         as max_job_duration,
    sum(nevents)                    as sum_events
       
    from   production_status 
    where  status>='started' and submitted>'{timestart}'
     {conditions}
    group by dsttype,prod_id
    order by dsttype desc
    ;
    """
    results = statusdbr.execute(psqlquery);
    labels  = [ f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
    table = [colorize(r,fail_color=(Back.YELLOW,Fore.BLACK)) for r in results]
    print( tabulate( table, labels, tablefmt="psql" ) )

def query_jobs_by_cluster(conditions=""):
    print("Summary of jobs by condor cluster")
    psqlquery=f"""
            select dsttype,cluster,
               count(run)                      as num_jobs,
               avg(age(started,submitting))    as avg_time_to_start,
               count( case status when 'submitted' then 1 else null end )
                                               as num_submitted,
               count( case status when 'running' then 1 else null end )
                                               as num_running,
               count( case status when 'finished' then 1 else null end )
                                               as num_finished,
               count( case status when 'failed' then 1 else null end )
                                               as num_failed,
               avg(age(ended,started))         as avg_job_duration,
               min(age(ended,started))         as min_job_duration,
               max(age(ended,started))         as max_job_duration,
               sum(nevents)                    as sum_events
       
            from   production_status 
            where  status>='started'   and submitted>'{timestart}'
            {conditions}
            group by dsttype,cluster
            order by dsttype desc
               ;
    """
    results = statusdbr.execute(psqlquery);
    labels  = [ f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
    table = [colorize(r) for r in results]
    print( tabulate( table, labels, tablefmt="psql" ) )



def query_jobs_by_run(conditions=""):
    print("Summary of jobs by condor cluster")
#              count(run)                      as num_jobs,
    psqlquery=f"""
            select dsttype,run,
               avg(age(started,submitting))    as avg_time_to_start,
               count( case status when 'submitted' then 1 else null end )
                                               as num_submitted,
               count( case status when 'running' then 1 else null end )
                                               as num_running,
               count( case status when 'finished' then 1 else null end )
                                               as num_finished,
               count( case status when 'failed' then 1 else null end )
                                               as num_failed,
               avg(age(ended,started))         as avg_job_duration,
               min(age(ended,started))         as min_job_duration,
               max(age(ended,started))         as max_job_duration,
               sum(nevents)                    as sum_events
       
            from   production_status 
            where  status>='started'   and submitted>'{timestart}'
            {conditions}
            group by dsttype,run
            order by run
               ;
    """
    results = statusdbr.execute(psqlquery);
    labels  = [ c[0] for c in statusdbr.description ]

    table = [colorize(r) for r in results]

    print( tabulate( table, labels, tablefmt="psql" ) )


@subcommand([
    argument( '--runs',  nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list.", default=[] ),
    argument( "--loop",  default=False, action="store_true", help="Run submission in loop with default 5min delay"), 
    argument( "--delay", default=300, help="Set the loop delay",type=int),
    argument( "--rules", default=[], nargs="+", help="Sets the name of the rule to be used"),
    argument( "SLURPFILE",   help="Specifies the slurpfile(s) containing the job definitions" )
])
def submit(args):
    """
    Submit a single set of jobs matching the specified rules in the specified definition files.
    """
    go = True

    kaedama  = sh.Command("kaedama.py" )    
    kaedama = kaedama.bake( "submit", "--config", args.SLURPFILE )

    while ( go ):

        if   len(args.runs)==1: kaedama = kaedama.bake( runs=args.runs[0] )
        elif len(args.runs)==2: kaedama = kaedama.bake( "--runs", args.runs[0], args.runs[1] )
        elif len(args.runs)==3: kaedama = kaedama.bake( "--runs", args.runs[0], args.runs[1], args.runs[2] )
        else:                   kaedama = kaedama.bake( "--runs", "0", "999999" )

        # Execute the specified rules
        for r in args.rules:
            kaedama( batch=True, rule=r, _out=sys.stdout )

        clear()
        query_pending_jobs()
        query_started_jobs()
        query_jobs_by_cluster()

        if args.loop==False: break
        time.sleep( args.delay )

query_choices=[
    "submitted",
    "started",
    "running",
    "finished",
    "failed",
]

@subcommand([
    argument( '--runs',  nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[0,999999] ),
    argument( "--loop", default=False, action="store_true", help="Run query in loop with default 5min delay"),
    argument( "--delay", default=300, help="Set the loop delay",type=int),
    argument( "--dstname", default=["all"], nargs="+", help="Specifies one or more dstnames to select on the display query" ),
    argument( "--timestart",default=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0),help="Specifies UTC timestamp (in ISO format, e.g. YYYY-MM-DD) for query.", type=dateutil.parser.parse)

])
def query(args):
    """
    Shows jobs which have reached a given state
    """
    global timestart
    timestart=str(args.timestart)
    go = True
    while ( go ):
        clear()
        query_pending_jobs()
        query_started_jobs()
        query_jobs_by_cluster()
        #query_jobs_by_run()
        if args.loop==False: break
        time.sleep( args.delay )





def main():

    args=parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)        

if __name__ == '__main__':
    main()
