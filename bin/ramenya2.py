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
from tqdm import tqdm
import pydoc
import datetime
import traceback
import yaml

import htcondor
import classad

import signal
import select 

def OnCTRLC(sn,fr):
    pause=1200
    x=input(  f"{Fore.RED}{Style.BRIGHT}You pressed ctrl-c.  {pause/60} min pause started.  Repeat to abort ramenya.  Enter anything else to continue loop.{Style.RESET_ALL}{Fore.RESET}" )
    i, o, e = select.select( [sys.stdin], [], [], 120 )
    if i:
        result=sys.stdin.readline().strip()
        if result=='a':
            exit(0)
        else:
            pass

signal.signal(signal.SIGINT, OnCTRLC)

init()

import fcntl
import os

condor_job_status_map = {
    1:"Idle",
    2:"Running",
    3:"Removing",
    4:"Completed",
    5:"Held",
    6:"Transferring Output",
    7:"Suspended",
}

class FileMutex:
    """
    File-based mutex recommended by Google AI.
    """
    def __init__(self, lock_file_path):
        self.lock_file_path = lock_file_path
        self.lock_file = None

    def acquire(self, blocking=True, timeout=None):
        if self.lock_file is None:
            self.lock_file = open(self.lock_file_path, 'w')
        
        try:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            return True
        except OSError as e:
            if e.errno in (11, 35): #EAGAIN or EWOULDBLOCK
                return False
            raise
    
    def release(self):
        if self.lock_file:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self.lock_file.close()
            self.lock_file = None
    
    def __enter__(self):
        print(f"{Fore.YELLOW}Aquiring mutex...{Fore.RESET}")
        self.acquire()
        print(f"{Fore.GREEN}Got it!{Fore.RESET}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        print("Releasing mutex")

def countdown(t):
    with tqdm(total=t, desc="Countdown", unit="sec", colour="green", ascii=False) as pbar:
        while t:
            mins, secs = divmod(t, 60)
            timer = '{:02d}:{:02d}'.format(mins, secs)
            pbar.set_description(f"Countdown: {timer}")
            sleep(1)
            t -= 1
            pbar.update(1)
    print("It's go time!")
        
args  = None

def no_colorization(row,default_color=("",""),fail_color=("","")):
    return [str(r) for r in row]

# Colorize html tables
def html_colorization(row,default_color=(),fail_color=()):
    color='<font color="green">'
    reset='</font>'
    if getattr( row, 'num_failed', 0)>0:
        color='<font color="red">'
        reset='</font>'
    myrow = [ 
        f"{color}{element}{reset}"   if (element!=None) else ""
        for element in list(row) 
    ]
    return myrow
    


# Colorize tables
def apply_colorization(row,default_color=(Back.RESET,Fore.GREEN),fail_color=(Back.RED,Fore.WHITE)):

    color=f"{default_color[0]}{default_color[1]}{Style.BRIGHT}"
    reset=f"{Fore.RESET}{Back.RESET}{Style.RESET_ALL}"
    if getattr( row, 'num_failed', 0)>0 or "Held" in row or " day " in str(getattr(row,"last_start","")):
        color=f"{fail_color[0]}{fail_color[1]}{Style.BRIGHT}"
        reset=f"{Fore.RESET}{Back.RESET}{Style.RESET_ALL}"
    myrow = [ 
        f"{color}{element}{reset}"   if (element!=None) else ""
        for element in list(row) 
    ]
    return myrow

colorize=apply_colorization
tablefmt="psql"

statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
statusdbr = statusdbr_.cursor()

try:
    statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
    statusdbr = statusdbr_.cursor()
except pyodbc.InterfaceError:
    for s in [ 10*random.random(), 20*random.random(), 30*random.random() ]:
        print(f"Could not connect to DB... retry in {s}s")
        time.sleep(s)
        try:
            statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
            statusdbr = statusdbr_.cursor()
        except:
            exit(0)


timestart=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )
time.sleep(1)

# https://mike.depalatis.net/blog/simplifying-argparse

parser     = argparse.ArgumentParser(prog='ramenya2')
subparsers = parser.add_subparsers(dest="subcommand")

parser.add_argument( "-v", "--verbose",dest="verbose"   , default=False, action="store_true", help="Sets verbose output")
parser.add_argument(       "--html",dest="html"   , default=False, action="store_true", help="Sets html output")
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
    return

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
    try:
        results = statusdbr.execute(psqlquery);
        #labels  = [ f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        labels  = [ c[0] if args.html else f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        table = [colorize(r) for r in results]
        print( tabulate( table, labels, tablefmt=tablefmt ) )
    except pyodbc.OperationalError: 
        print("... could not query the db ... skipping report [OperationError]")
        pass
    except pyodbc.ProgrammingError:
        print("... could not query the db ... skipping report [ProgrammingError]")
        pass
    except pyodbc.Error:
        print("... could not query the db ... skipping report [Error]")
        pass




def query_started_jobs(conditions=""):
    return

    print("Summary of jobs which have reached staus='started'")
    #print("--------------------------------------------------")
    psqlquery=f"""
    select dsttype,
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
    max(started           )         as last_job_started,
    max(ended             )         as last_job_finished,
    sum(nevents)                    as sum_events
       
    from   production_status 
    where  status>='started' and submitted>'{timestart}'
     {conditions}
    group by dsttype
    order by dsttype desc
    ;
    """
    vistable="...could not make db query..."
    try:
        results = statusdbr.execute(psqlquery);
        #labels  = [ f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        labels  = [ c[0] if args.html else f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        table = [colorize(r,fail_color=(Back.YELLOW,Fore.BLACK)) for r in results]
        vistable = tabulate( table, labels, tablefmt=tablefmt ) 
        print( vistable )
    except pyodbc.OperationalError: 
        pass
    except pyodbc.ProgrammingError:
        pass
    return vistable

def query_jobs_by_cluster(conditions=""):
    return

    print("Summary of jobs by condor cluster")
    psqlquery=f"""
            select dsttype,cluster,
               min(run) as min_run,
               max(run) as max_run,
               count(run)                      as num_jobs,
               min(started)                    as earliest_start,
               max(started)                    as last_start,
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
               sum(nevents)                    as sum_events
       
            from   production_status 
            where  status>='started'   and submitted>'{timestart}'  and status<'failed'
            {conditions}
            group by dsttype,cluster
            order by dsttype desc
               ;
    """
    try:
        results = statusdbr.execute(psqlquery);

        labels  = [ c[0] if args.html else f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        table = [colorize(r) for r in results]
        print( tabulate( table, labels, tablefmt=tablefmt ) )
    except pyodbc.OperationalError: 
        print("... could not query the db ... skipping report")
        pass
    except pyodbc.ProgrammingError:
        print("... could not query the db ... skipping report")
        pass


def query_failed_jobs(conditions="", title="Summary of failed jobs by run"):
    return

    print(title)
    psqlquery=f"""
            select dstname,prod_id,string_agg( to_char(run,'FM00000000')||'-'||to_char(segment,'FM0000'),' ' )
            from   production_status 
            where  status='failed'   and submitted>'{timestart}'
            {conditions}
            group by dstname,prod_id
            order by prod_id
               ;
    """
    try:
        results = statusdbr.execute(psqlquery);
        #labels  = [ f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        labels  = [ c[0] if args.html else f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        table = [colorize(r,default_color=(Back.RED,Fore.WHITE)) for r in results]
        print( tabulate( table, labels, tablefmt=tablefmt ) )
    except pyodbc.OperationalError: 
        print("... could not query the db ... skipping report")
        pass
    except pyodbc.ProgrammingError:
        print("... could not query the db ... skipping report")
        pass


def query_jobs_by_run(conditions="", title="Summary of jobs by run" ):
    return

    print(title)
#              count(run)                      as num_jobs,
    psqlquery=f"""
            select dsttype,run,segment,status,cluster,process
            from   production_status 
            where  status>='started'   and submitted>'{timestart}' and status!='finished'
            {conditions}
            order by run
               ;
    """
    #print(psqlquery)
    try:
        results = statusdbr.execute(psqlquery);
        #for r in results:
        #    print(f"{r.cluster} {r.process}")

        #labels  = [ c[0] for c in statusdbr.description ]
        labels  = [ c[0] if args.html else f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ]
        table = [colorize(r) for r in results]

        print( tabulate( table, labels, tablefmt=tablefmt ) )
    except pyodbc.OperationalError: 
        print("... could not query the db ... skipping report")
        pass
    except pyodbc.ProgrammingError:
        print("... could not query the db ... skipping report")
        pass

def query_jobs_held_by_condor(conditions="true", title="Summary of jobs by with condor state",  ):
    # Write connection to DB
    try:
        statusdbw_ = pyodbc.connect("DSN=ProductionStatusWrite")
        statusdbw = statusdbw_.cursor()
    except (pyodbc.InterfaceError, pyodbc.Error,pyodbc.ProgrammingError) as e:
        print("... could not query the db ... skipping report")
        return

    query=f"select id,cluster,process from production_status where {conditions} and status!='failed' limit 1000"
    try:
        results = statusdbw.execute(query);
    except (pyodbc.OperationalError,pyodbc.Error,pyodbc.ProgrammingError) as e: 
        print("... could not query the db ... skipping report")
        del statusdbw_
        del statusdbw
        return

    schedd = htcondor.Schedd() 

    # Query held jobs
    try:
        condor_query = schedd.query(
            constraint=f"JobStatus==5",
            projection=["ClusterId","ProcId","JobStatus","HoldReasonCode","HoldReasonSubcode","HoldReason","EnteredCurrentStatus","ExecutableSize"]
        )
    except htcondor.HTCondorIOError:
        print("... could not query condor.  skipping report ...")
        del statusdbw_
        del statusdbw
        return

    # map each cluster.process to an ID in the production status table 
    c2ps = {}
    for r in results:
        key = f"{r.cluster}.{r.process}"
        c2ps[key]=int(r.id)

    # map each cluster.process to the condor query
    c2cq = {}
    for q in condor_query:
        clusterid = int(q.lookup("ClusterId"))
        processid = int(q.lookup("ProcId"))
        jobstatus = int(q.lookup("JobStatus"))
        enteredcurrentstatus = int(q.lookup("EnteredCurrentStatus"))
        timestamp=datetime.datetime.fromtimestamp(enteredcurrentstatus,datetime.timezone.utc)
        holdreason=None
        if jobstatus==5:
            try:
                holdreason = q.lookup('HoldReason')[:1022] + " $" # save only 1022 characters and terminate with a $
            except KeyError:
                holdreason = "unknown"
        key = f"{clusterid}.{processid}"
        try:
            id_ = c2ps[key]
            c2cq[ id_ ] = {
                'JobStatus' : jobstatus,
                'EnteredCurrentStatus' : timestamp,                
                'HoldReason' : holdreason,
            }
        except KeyError:
            pass

    for i,cq in c2cq.items():

        # First 512 characters of the message
        message=str(cq['HoldReason']).replace("'"," ") [:512]
        enteredcurrentstatus=str(cq['EnteredCurrentStatus'])
        update = f"""
        update production_status
        set status='failed', 
            flags=5,
            message='{message}',
            ended='{enteredcurrentstatus}'
        where id={i};
        """
        print(update)

        statusdbw.execute(update)
        statusdbw.commit()

    # Delete (and drop connection) to the DB
    del statusdbw
    del statusdbw_

    #pprint.pprint( c2ps )
    #pprint.pprint( c2cq )
        
    

def query_jobs_by_condor(conditions="", title="Summary of jobs by with condor state",  ):
    return


    print(title)
#              count(run)                      as num_jobs,
    psqlquery=f"""
            select dsttype,run,segment,status,cluster,process,prod_id
            from   production_status 
            where  status>='started' and submitted>'{timestart}' and status!='finished'
            {conditions}
            order by run
               ;
    """
    try:
        results = statusdbr.execute(psqlquery);
    except pyodbc.OperationalError: 
        print("... could not query the db ... skipping report")
        return
    except pyodbc.ProgrammingError:
        print("... could not query the db ... skipping report")
        return

    schedd = htcondor.Schedd() 
    condor_job_status_map = {
        1:"Idle",
        2:"Running",
        3:"Removing",
        4:"Completed",
        5:"Held",
        6:"Transferring Output",
        7:"Suspended",
    }

    condor_query = schedd.query(
#       constraint=f"(ClusterId=={r.cluster})&&(ProcId=={r.process})",
        projection=["ClusterId","ProcId","JobStatus","HoldReasonCode","HoldReasonSubcode","HoldReason","EnteredCurrentStatus","ExecutableSize"]
    )


    condor_results = {}

    for q in condor_query:
        clusterid = int(q.lookup("ClusterId"))
        processid = int(q.lookup("ProcId"))
        jobstatus = int(q.lookup("JobStatus"))
        execsize  = int(q.lookup("ExecutableSize"))/1024 # MB
        enteredcurrentstatus=int(q.lookup("EnteredCurrentStatus"))
        holdreasoncode    = 0
        holdreasonsubcode = 0
        holdreason        = ""
        if jobstatus==5:
            holdreasoncode    = int(q.lookup("HoldReasonCode"))
            holdreasonsubcode = int(q.lookup("HoldReasonSubcode"))
            holdreason        = q.lookup("HoldReason")

        # Add a dictionary for cluster if needed
        if condor_results.get( clusterid, None )==None:
            condor_results[ clusterid ] = {}

        # Get the cluster entry
        cluster_entry = condor_results[ clusterid ]

        # Add in the processid
        if cluster_entry.get( processid, None )==None:
            cluster_entry[ processid ] = {}

        # Add dictionary for process
        process_entry = cluster_entry[ processid ]

        process_entry["jobstatus"]=condor_job_status_map[jobstatus]
        process_entry["execsize"]=execsize
        process_entry["enteredcurrentstatus"]=str( datetime.datetime.utcfromtimestamp(enteredcurrentstatus) )
        process_entry["holdreasoncode"]=holdreasoncode
        process_entry["holdreasonsubcode"]=holdreasonsubcode        

    merged = []
    for r in results:

        cluster_entry = condor_results.get( int(r.cluster), None )
        if cluster_entry==None:
 #           print(f"{r.cluster} has no cluster entry")
            continue

        process_entry = cluster_entry.get( int(r.process), None )
        if process_entry==None:
            continue

        myresults = [ i for i in r ]

        extend = [
            process_entry["jobstatus"],
            process_entry["enteredcurrentstatus"],
            process_entry["holdreasoncode"],
            process_entry["holdreasonsubcode"],
            ]

        merged.append( myresults + extend )    

    labels  = [ c[0] if args.html else f"{Style.BRIGHT}{c[0]}{Style.RESET_ALL}" for c in statusdbr.description ] + ["condor status","at","hold code","sub code"]
    table = [colorize(r) for r in merged]

    print( tabulate( table, labels, tablefmt=tablefmt ) )

def getArgsForRule( yaml, r ):
    result = []

    # If we didnt load options via yaml, return an empty dict
    if yaml == {}:
        return []

    ruleargs = yaml.get(r, {})

    # Transform into a list of arguments
    for k,v in ruleargs.items():

        flag=k.replace("_","-")
        result.append(f"--{flag}")
        
        # skip if the flag is boolean
        if isinstance( v, bool ):
            continue

        y = str(v)
        
        for x in y.split(' '):
            result.append( str(x) )
            
    return result


@subcommand([
    argument( '--nevents', help="Specifies number of events to submit (defaults to all)", default=0 ),
    argument( '--runs',  nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list.", default=[] ),
    argument( "--loop",  default=False, action="store_true", help="Run submission in loop with default 5min delay"), 
    argument( "--delay", default=300, help="Set the loop delay",type=int),
    argument( "--rules", default=[], nargs="?", help="Sets the name of the rule to be used"),
    argument( "--rules-file", dest="rules_file", default=None, help="If specified, read the list of active rules from the given file on each pass of the loop" ),
    argument( "--rules-yaml", dest="rules_yaml", default=None, help="If specified, executes each rule in turn setting kaedama arguments specific to each rule" ),
    argument( "--timestart",default=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0),help="Specifies UTC timestamp (in ISO format, e.g. YYYY-MM-DD) for query.", type=dateutil.parser.parse),
    argument( "--test",default=False,help=argparse.SUPPRESS,action="store_true"), # kaedama will be submitted in batch mode
    argument( "--experiment-mode", default=None, help="Sets experiment-mode for kaedama", dest="mode" ),
    argument( "--resubmit", default=False, action="store_true", help="Adds the -r option to kaedama" ),
    argument( "--maxjobs", default=10000, help="Maximum number of jobs to submit in one cycle of the loop" ),
    argument( "--maxcondor",default=75000, help="Do not submit if more than maxcondor jobs are in the system.  Terminate the loop if we exceed 150%% of this value.",type=int),
    argument( "--watermark",default=1.05,help="Watermark expressed as a multiple of max condor.  When we exceed this value we exit or cycle the loop depending on the next option" ),
    argument( "--watermark-action", dest="watermark_action",help="Action to take when we exceed the high watermark",default="cycle", choices=['cycle','exit'] ),
    argument( "--dbinput", default=True, help="Sets the dbinput flag for kaedama [defaults to true]",action="store_true"),
    argument( "--no-dbinput", dest="dbinput", help="Unsets the dbinput flag for kaedama",action="store_false"),
    argument( "--verbose",dest="verbose",help="Sets verbosity flag.  Kaedama will be directed to stdout.",action="store_true",default=False),
    argument( "SLURPFILE",   help="Specifies the slurpfile(s) containing the job definitions" )
])
def submit(args):
    """
    Submit a single set of jobs matching the specified rules in the specified definition files.
    """
    go = True

    
    

    global timestart
    timestart=str(args.timestart)
    kaedama  = sh.Command("kaedama.py" )

    # If provided read in yaml rules file.  The rules_yaml will be an empty dictionary... the args.rules_yaml
    # determines whether or not the file is loaded.
    rules_yaml = {}
    runreport = ""
    
    if args.rules_yaml:
        with open( args.rules_yaml ) as stream:
            rules_yaml = yaml.safe_load( stream )
    else:
        
        if args.dbinput:
            kaedama = kaedama.bake( "submit", "--config", args.SLURPFILE, "--nevents", args.nevents, "--maxjobs", int(args.maxjobs), "--dbinput" )
        else:
            kaedama = kaedama.bake( "submit", "--config", args.SLURPFILE, "--nevents", args.nevents, "--maxjobs", int(args.maxjobs), "--no-dbinput" )

        if args.test:
            kaedama = kaedama.bake( "--batch" )
        if args.mode is not None:
            kaedama = kaedama.bake( "--experiment-mode", args.mode )
        if args.resubmit:
            kaedama.bake( "-r ")

        if   len(args.runs)==1: 
            kaedama = kaedama.bake( runs=args.runs[0] )
            runreport = f"runs: {args.runs[0]}"
        elif len(args.runs)==2: 
            kaedama = kaedama.bake( "--runs", args.runs[0], args.runs[1] )
            runreport = f"runs: {args.runs[0]} to {args.runs[1]}"
        elif len(args.runs)==3: 
            kaedama = kaedama.bake( "--runs", args.runs[0], args.runs[1], args.runs[2] )
            runreport = f"runs: {args.runs}"
        else:                   
            kaedama = kaedama.bake( "--runs", "0", "999999" )
            runreport = f"runs: 0 to 999999"

    tracebacks = []
    successes  = []

    mutex = FileMutex(".slurp/__ramenya_lock__")                
    
    while ( go ):

        list_of_active_rules = args.rules
        if args.rules_file:
            with open( args.rules_file, 'r' ) as f:
                list_of_active_rules = [ 
                    line.strip() for line in f.readlines() if '#' not in line 
                ]

        if args.rules_yaml:
            rules_yaml = {} # clear from last loop and reload
            with open( args.rules_yaml ) as stream:
                rules_yaml = yaml.safe_load( stream )            
            list_of_active_rules = rules_yaml.keys()                

        clear()

        print( f"Active rules: {args.SLURPFILE} {args.rules} "  )
        print( f"Defined in file: {args.rules_file}" )
        print( tabulate( [ list_of_active_rules ], ['active rules'], tablefmt=tablefmt ) )
        print( runreport )

        if len(successes)>0:
            print( "Success on last iteration:")
            print( tabulate( [ successes ], ['success'], tablefmt=tablefmt ) )
        successes = []

        if len(tracebacks)>0:
            print( "Failed on last iteration:")
            print( tabulate( [ tracebacks ], ['failures'], tablefmt=tablefmt ) )
        tracebacks = []

        # Verify that there is enough room on condor to submit
        schedd = htcondor.Schedd() 
        ncondor = 1E9

        with mutex:

            try:
                condor_query = schedd.query(
                    projection=["ClusterId","ProcId","JobStatus","sPHENIX_DSTTYPE"]
                )
                ncondor = len(condor_query)
                if ncondor > float(args.watermark) * int(args.maxcondor):
                    if args.watermark_action=="exit":
                        print(f"Exiting b/c there are too many jobs ({ncondor}) in the condor schedd.")
                        exit(0)
                    else:
                        print( "**********************************************************************")
                        print(f"Skipping b/c there are too many jobs ({ncondor}) in the condor schedd.") 
                        print( "**********************************************************************")                       
                        continue

                #dst_counts={}
                #for ad in condor_query:
                #    dst = dst_counts.get( ad['sPHENIX_DSTTYPE'],
                #                          { 'Idle':0,
                #                            'Running':0,
                #                            'Removing':0,
                #                            'Completed':0,
                #                            'Held':0,
                #                            'Stageout':0,
                #                            'Suspended':0
                #                           } )
                #    dst[condor_job_status_map[ad['JobStatus']]]+=1
                #    dst_counts[ ad['sPHENIX_DSTTYPE'] ] = dst                
                #print( tabulate((dst_counts)) )

            except htcondor.HTCondorIOError:
                print("{Fore.RED}... could not query condor, aborting this submission ...{Fore.RESET}")
                del statusdbw_
                del statusdbw                
            
            if ncondor == 1E9:
                print("{Fore.RED}Could not connect to schedd... skipping{Fore.RESET}")

            elif ncondor > args.maxcondor:
                print(f"{Fore.YELLOW}Skipping b/c there are too many jobs ({ncondor}) in the condor schedd.{Fore.RESET}")

            else:

                for r in list_of_active_rules:
                    print( f"{Fore.GREEN}Trying rule ... {r} {datetime.datetime.now().replace(microsecond=0)}{Fore.RESET}" )

                    try:
                        myargs = getArgsForRule( rules_yaml, r )
                        if args.verbose:
                            kaedama( myargs, "--batch", "--rule", r, _out=sys.stdout )
                        else:
                            kaedama( myargs, "--batch", "--rule", r )
                        successes.append(r)
                    except sh.ErrorReturnCode_1:
                        print(traceback.format_exc())
                        tracebacks.append( r )
            

        if args.loop==False: break
        countdown( args.delay )

query_choices=[
    "submitted",
    "started",
    "running",
    "finished",
    "failed",
    "held",
    "none"
]

def query_jobs_none(): pass

fmap = {
    "pending" : query_pending_jobs,
    "started" : query_started_jobs,
    "clusters" : query_jobs_by_cluster,
    "runs" : query_jobs_by_run,
    "failed": query_failed_jobs,
    "condor": query_jobs_by_condor,
    "held"  : query_jobs_held_by_condor,
    "none"  : query_jobs_none,
}



@subcommand([
    argument( '--runs',  nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[0,999999] ),
    argument( "--loop", default=False, action="store_true", help="Run query in loop with default 5min delay"),
    argument( "--delay", default=300, help="Set the loop delay",type=int),
    argument( "--dstname", default=["all"], nargs="+", help="Specifies one or more dstnames to select on the display query" ),
    argument( "--reports", default=["started"], nargs="+", help="Queries the status DB and produces summary reports",choices=fmap.keys()),
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
        for fname in args.reports:
            fmap[fname]()
        if args.loop==False: break
        for i in tqdm( range( args.delay * 10), desc="Next poll" ):
            time.sleep(0.1)


@subcommand([
    argument( "dstname", help="Specifies the failed dst name to be removed" ),
    argument( "run",     help="Specifies the failed run number to be removed", type=int ),
    argument( "--segment", help="Specifies the failed segment number to be removed.  If not specified, all segments will be removed.", default=None, type=int ),    
    argument( "--ext", help="File extention to be removed", default="root" )
])
def remove(args):
    """
    """
    statusdbw_ = pyodbc.connect("DSN=ProductionStatusWrite")
    statusdbw  = statusdbw_.cursor()

    filecatw_  = pyodbc.connect("DSN=FileCatalogWrite;UID=phnxrc")
    filecatw   = filecatw_.cursor()

    segment_condition = ""
    if args.segment!=None:
        segment_condition = f" and segment={args.segment}"

    file_segment='%'
    if args.segment!=None:
        file_segment = f'-{args.segment:04}'

    # Remove matching files
    files_query=f"""
    select * from files where lfn like '{args.dstname}_{args.run:08}{file_segment}.{args.ext}';
    """
    files=filecatw.execute(files_query).fetchall()
    for f in files:
        try: 
            os.remove(f.full_file_path) 
            print(f"{f.lfn}: {f.full_file_path} removed")
        except OSError as error: 
            print(error) 
            print(f"{f.lfn}: {f.full_file_path} already gone")

    files_query=f"""
    delete from files where lfn like '{args.dstname}_{args.run:08}{file_segment}.{args.ext}';
    """
    print(files_query)
    filecatw.execute(files_query);
    filecatw.commit()

    # Remove matching datasets
    datasets_query=f"""
    delete from datasets where filename like '{args.dstname}_{args.run:08}{file_segment}.{args.ext}';
    """
    print(datasets_query)
    filecatw.execute(datasets_query);
    filecatw.commit()

# TODO: cleanup the condor queue
#    status_query=f"""
#    select cluster,process from production_status where dstname='{args.dstname}' and run={args.run} {segment_condition};
#    """    
#    print(status_query)
#    results=filecatw.execute(status_query).fetchall()
#    print(results)
#    for r in results:
#        print( r.cluster + " " + r.process )

    # Remove matching production status
    status_query=f"""
    delete from production_status where dstname='{args.dstname}' and run={args.run} {segment_condition};
    """
    print(status_query)
    statusdbw.execute(status_query);
    statusdbw.commit()



def noodles( args_=None ):

    global colorize
    global tablefmt
    global args

    if args_ == None:
        args=parser.parse_args()
    else:
        args=parser.parse_args( args_ )

    if args.html:
        colorize = no_colorization
        tablefmt = "html"
    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)        


if __name__ == '__main__':
    noodles()
