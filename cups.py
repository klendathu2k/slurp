#!/usr/bin/env python3

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
import json
import hashlib
import os
import shutil
import platform
from pathlib import Path

MAXDSTNAMES = 100

prod_mode = Path("CUPS_PRODUCTION_MODE").is_file()
test_mode = Path("CUPS_TESTBED_MODE").is_file()
if ( prod_mode ):
    print("Found production mode")
    dsnprodr = 'Production_read'
    dsnprodw = 'Production_write'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'    
elif ( test_mode ):
    print("Found testbed mode")
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'
else:
    print("NOTICE: Neither production nor testbed mode set.  Default to testbed.  YMMV.")
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'


def printDbInfo( cnxn, title ):
    name=cnxn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv=cnxn.getinfo(pyodbc.SQL_SERVER_NAME)
    print(f"Connected {name} from {serv} as {title}")

cnxn_string_map = {
    'fcw'         : f'DSN={dsnfilew};UID=phnxrc',
    'fcr'         : f'DSN={dsnfiler};READONLY=True;UID=phnxrc',
    'statr'       : f'DSN={dsnprodr};UID=argouser',
    'statw'       : f'DSN={dsnprodw};UID=argouser',
}    

def dbQuery( cnxn_string, query, ntries=10 ):

    # Some guard rails
    assert( 'delete' not in query.lower() )    

    lastException = "noexception"
    
    # Attempt to connect up to ntries

    start  = datetime.datetime.now(datetime.timezone.utc)        

    ntries = 1
    curs = None

    name = "noconnection" # cnxn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv = "noconnection" # cnxn.getinfo(pyodbc.SQL_SERVER_NAME)
    conn = None

    for itry in range(0,ntries):
        try:
            conn = pyodbc.connect( cnxn_string )
            curs = conn.cursor()
            curs.execute( query )
            break
                
        except Exception as E:
            ntries = ntries + 1
            lastException = str(E)
            delay = (itry + 1 ) * random.random()
            time.sleep(delay)

    if conn:
        name = conn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
        serv = conn.getinfo(pyodbc.SQL_SERVER_NAME)        
        

    finish = datetime.datetime.now(datetime.timezone.utc)        
            
    return curs, ntries, start, finish, lastException, name, serv
            

#
# Production status connection
#
#try:
#    statusdb  = pyodbc.connect("DSN=ProductionStatusWrite")
#    statusdbc = statusdb.cursor()
#except (pyodbc.InterfaceError,pyodbc.OperationalError) as e:
#    for s in [ 10*random.random(), 20*random.random(), 30*random.random(), 60*random.random(), 120*random.random() ]:
#        print(f"Could not connect to DB... retry in {s}s")
#        time.sleep(s)
#        try:
#            statusdb  = pyodbc.connect("DSN=ProductionStatusWrite")
#            statusdbc = statusdb.cursor()
#            break
#        except:
#            pass
#    else:
#        print(sys.argv())
#        print(e)
#        exit(1)

#try:
#    statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
#    statusdbr = statusdbr_.cursor()
#except pyodbc.InterfaceError:
#    for s in [ 10*random.random(), 20*random.random(), 30*random.random() ]:
#        print(f"Could not connect to DB... retry in {s}s")
#        time.sleep(s)
#        try:
#            statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
#            statusdbr = statusdbr_.cursor()
#        except:
#            exit(0)
#except pyodbc.Error as e:
#    print(e)
#    exit(1)

def md5sum( filename ):
    file_hash=None
    with open( filename, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)
    return file_hash.hexdigest()
    

"""
cups.py -t tablename state  dstname run segment [-e exitcode -n nsegments]
"""

# Little helper to print st8 to stderr
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# https://mike.depalatis.net/blog/simplifying-argparse

parser     = argparse.ArgumentParser(prog='cups')
subparsers = parser.add_subparsers(dest="subcommand")

parser.add_argument( "-v", "--verbose",dest="verbose"   , default=False, action="store_true", help="Sets verbose output")
parser.add_argument( "--no-update",    dest="noupdate"  , default=False, action="store_true", help="Does not update the DB table")
parser.add_argument( "-t","--table"  , dest="table"     , default="production_status",help="Sets the name of the production status table table")
parser.add_argument( "-d","--dstname", dest="dstname"   ,                                                   help="Set the DST name eg DST_CALO_auau1", required=True)
parser.add_argument( "-r","--run"    , dest="run"       , default=None,help="Sets the run number for the update",required=True)
parser.add_argument( "-s","--segment", dest="segment"   , default=None,help="Sets the segment number for the update",required=True)
parser.add_argument( "--timestamp"   , dest="timestamp" , default=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  ),
                     help="Sets the timestamp, default is now (and highly recommended)" )


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

def getLatestId( tablename, dstname, run, seg ):

    cupsid=os.getenv('cupsid')
    if cupsid and tablename=='production_status':
        return cupsid

    print("[CUPS FATAL]: cupsid is not defined")
    exit(0) # operating without a cupsid is now a fatal error



@subcommand()
def info( args ):
    start = datetime.datetime.now(datetime.timezone.utc)        
    #printDbInfo( statusdb,   "Production Status DB [write]" )
    #printDbInfo( statusdbr_, "Production Status DB [write]" )
    cupsid=os.getenv('cupsid')
    print(f"Working with cupsid={cupsid}")
    print("Printing arguments")
    for arg in vars(args):
        print(f"{arg}: {getattr(args, arg)}")
    finish = datetime.datetime.now(datetime.timezone.utc)        

    return 'result', 0, start, finish, 'success', '....', '....'

    
# Method appears to be deprecated...
#def update_production_status( update_query, retries=10, delay=10.0 ):
#    print(update_query)
#    for itry in range(0,retries):
#        time.sleep( delay * (itry + 1 ) * random.random() )
#        try:
#            with pyodbc.connect("DSN=ProductionStatusWrite") as statusdb:
#                curs=statusdb.cursor()
#                curs.execute(update_query)
#                curs.commit()
#                print(f"Applied after {itry+1} attempts")
#                return
#        except:
#            print(f"Failed {itry+1} attempts...")
#
#    print("Update failed")
    


@subcommand()
def started(args):
    """
    Executed by the user payload script when the job is started
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=int(args.run)
    seg=int(args.segment)
    id_ = getLatestId( tablename, dstname, run, seg )
    node=platform.node()
    update = f"""
    update {tablename}
    set 
         status='started',
         started='{timestamp}',
         execution_node='{node}'
    where id={id_}

    """

    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv

@subcommand([
    argument(     "--nsegments",help="Number of segments produced",dest="nsegments",default=1),
])
def running(args):
    """
    Executed by the user payload script when the job begins executing the payload macro.
    May be used to update the number of produced segments.
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=int(args.run)
    seg=int(args.segment)
    nsegments=int(args.nsegments)
    id_ = getLatestId( tablename, dstname, run, seg )
    update = f"""
    update {tablename}
    set status='running',running='{timestamp}',nsegments={nsegments}
    where id={id_}
    """


    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv


#_______________________________________________________________________________________________________
@subcommand([
    argument("-e","--exit",help="Exit code of the payload macro",dest="exit",default=-1),
    argument(     "--nsegments",help="Number of segments produced",dest="nsegments",default=1),
    argument(     "--nevents",  help="Number of events produced",dest="nevents",type=int,default=0),
    argument(     "--inc", help="If set, increments the number of events",dest="inc",default=False,action="store_true"),
])
def finished(args):
    """
    Executed by the user payload script when the job finishes executing the payload macro.
    If exit code is nonzer, state will be marked as failed.
    """
    tablename=args.table
    dstname=args.dstname
    timestamp=args.timestamp
    run=int(args.run)
    seg=int(args.segment)
    id_ = getLatestId( tablename, dstname, run, seg )
    ec=int(args.exit)
    ns=int(args.nsegments)
    ne=int(args.nevents)
    state='finished'
    if ec>0:
        state='failed'
    update = None
    if args.inc:
        update = f"""
        update {tablename}
        set status='{state}',ended='{timestamp}',nsegments={ns},exit_code={ec},nevents=nevents+{ne}
        where id={id_}
        """
    else:
        update = f"""
        update {tablename}
        set status='{state}',ended='{timestamp}',nsegments={ns},exit_code={ec},nevents={ne}
        where id={id_}
        """

    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv
        
#_______________________________________________________________________________________________________
@subcommand([
    argument("-e","--exit",help="Exit code of the payload macro",dest="exit",default=-1),
])
def exitcode(args):
    """
    Executed by the user payload script when the job finishes executing the payload macro.
    If exit code is nonzer, state will be marked as failed.
    """
    tablename=args.table
    dstname=args.dstname
    run=int(args.run)
    seg=int(args.segment)
    id_ = getLatestId( tablename, dstname, run, seg )
    ec=int(args.exit)
    state='finished'
    if ec>0:
        state='failed'
    update = f"""
    update {tablename}
    set status='{state}',exit_code={ec}
    where id={id_}
    """

    conn, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv
    

#_______________________________________________________________________________________________________
@subcommand([
    argument(     "--nevents",  help="Number of events produced",dest="nevents",type=int,default=0),
    argument(     "--inc",  help="Sets increment mode",default=False,action="store_true"),
])
def nevents(args):
    """
    Updates the number of events processed
    """
    tablename=args.table
    dstname=args.dstname
    run=int(args.run)
    seg=int(args.segment)
    id_ = getLatestId( tablename, dstname, run, seg )
    ne=int(args.nevents)
    update=None
    if args.inc:
        update = f"""
        update {tablename}
        set nevents=nevents+{ne}
        where id={id_}
        """
    else:
        update = f"""
        update {tablename}
        set nevents={ne}
        where id={id_}
        """

    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv

@subcommand([
])
def getinputs(args):
    """
    Retrieves the list of input files
    """
    tablename=args.table
    dstname=args.dstname
    run=int(args.run)
    seg=int(args.segment)
    id_ = getLatestId( tablename, dstname, run, seg )
    query = f"""
    select inputs from {tablename} where id={id_} limit 1
    """
    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], query )
    if curs:
        for result in curs:
            flist = str(result[0]).split(',')
            for f in flist:
                print(f)

    return 'result', ntries, start, finish, ex, nm, sv


#_______________________________________________________________________________________________________
@subcommand([
    argument(     "--files",  help="List of input files (and/or ranges)",dest="files",nargs="*"),
])
def inputs(args):
    """
    Updates the number of events processed
    """
    tablename=args.table
    dstname=args.dstname
    run=int(args.run)
    seg=int(args.segment)
    id_ = getLatestId( tablename, dstname, run, seg )
    inputs = 'unset'
    if len(args.files)>0:
        inputs=' '.join(args.files)
    update = f"""
    update {tablename}
    set inputs='{inputs}'
    where id={id_}
    """

    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv

#_______________________________________________________________________________________________________
@subcommand([
    argument( "--replace",    help="remove and replace existing entries.", action="store_true", default=True ),
    argument( "--no-replace", help="remove and replace existing entries.", action="store_false", dest="replace" ),
    argument( "--ext", help="file extension, e.g. root, prdf, ...", default="prdf", choices=["root","prdf"] ),
    argument( "--path", help="path to output file", default="./" ),
    argument( "--hostname", help="host name of the filesystem", default="lustre", choices=["lustre","gpfs"] ),
    argument( "--dataset", help="sets the name of the dataset", default="mdc2" ),
    argument( "--nevents", help="sets number of events", default=0 )
])
def catalog(args):
    """
    Add the file to the file catalog.  
    """

    print("[CUPS WARNING: catalog is deprecated")
    return
    

@subcommand([
    argument( "message", help="Message to be appended to the production status entry" ),
    argument( "--flag",    help="Adds a value to the flags", default='0' ),
    argument( "--logsize", help="Sets the log file size", default='0' ),
])
def message(args):
    """
    Sets message field on the production status table
    """
    flaginc=int(args.flag)
    id_ = getLatestId( args.table, args.dstname, int(args.run), int(args.segment) )
    update = f"update {args.table} set message='{args.message}',flags=flags+{flaginc},logsize={args.logsize}  where id={id_};"
    curs, ntries, start, finish, ex, nm, sv = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()

    return 'result', ntries, start, finish, ex, nm, sv


#_______________________________________________________________________________________________________
@subcommand([
    argument( "filename", help="Name of the file to be staged out"),
    argument( "outdir",   help="Output directory" ),
    argument( "--retries", help="Number of retries before silent failure", type=int, default=5 ),
    argument( "--hostname", help="host name of the filesystem", default="lustre", choices=["lustre","gpfs"] ),
    argument( "--nevents",  help="Number of events produced",dest="nevents",type=int,default=0),
    argument(     "--inc", help="If set, increments the number of events",dest="inc",default=False,action="store_true"),
    argument( "--dataset", help="sets the name of the dataset", default="test" ),
    argument( "--dsttype", help="sets the sphenix dsttype", default=None ),
    argument( "--prodtype", dest="prodtype", help="sets the production type of the job...", required=True, choices=["many","only"] )
])
def stageout(args):
    """
    Stages the given file out to the specified 
    """
    md5true  = md5sum( args.filename )                    # md5 of the file we are staging out
    sztrue   = int( os.path.getsize(f"{args.filename}") ) # size in bytes of the file

    # Stage the file out to the target directory.
    if args.verbose:
        print("Copy back file")

    # Copy the file
    try:
        shutil.copy2( f"{args.filename}", f"{args.outdir}" )
        print(".... copy back finished ....")
    except Exception as e:
        print(f"ERROR: Failed to copy file {args.filename} to {args.outdir}.  Aborting stageout.")
        return

    sz  = int( os.path.getsize(f"{args.outdir}/{args.filename}") ) 

    if args.verbose:
        print("Checksum before and after")
        print(md5true)
        print(md5check)

    attempt = 0

    # TODO: switch to an update mode rather than a delete / replace mode.
    timestamp= args.timestamp
    run      = int(args.run)
    seg      = int(args.segment)
    host     = args.hostname
    nevents  = args.nevents

    # n.b. not the slurp convention for dsttype
    dstname  = args.dstname
    dsttype='_'.join( dstname.split('_')[-2:] )

    if args.dsttype != None:        dsttype = args.dsttype
                
    md5=md5true

    # Strip off any leading path 
    filename=args.filename.split('/')[-1]

    # Insert into files primary key: (lfn,full_host_name,full_file_path)
    if args.verbose:        print("Insert into files")

    insert=f"""
    insert into files (lfn,full_host_name,full_file_path,time,size,md5) 
    values ('{filename}','{host}','{args.outdir}/{filename}','now',{sz},'{md5}')
    on conflict
    on constraint files_pkey
    do update set 
    time=EXCLUDED.time,
    size=EXCLUDED.size,
    md5=EXCLUDED.md5
    ;
    """
    if args.verbose:        print(insert)


    # insert into files ...
    insfiles, ntries_files, start_files, finish_files, ex_files, nm_files, sv_files = dbQuery( cnxn_string_map[ 'fcw' ], insert )
    

    # Insert into datasets primary key: (filename,dataset)
    if args.verbose:        print("Insert into datasets")
    insert=f"""
    insert into datasets (filename,runnumber,segment,size,dataset,dsttype,events)
    values ('{filename}',{run},{seg},{sz},'{args.dataset}','{dsttype}',{args.nevents})
    on conflict
    on constraint datasets_pkey
    do update set
    runnumber=EXCLUDED.runnumber,
    segment=EXCLUDED.segment,
    size=EXCLUDED.size,
    dsttype=EXCLUDED.dsttype,
    events=EXCLUDED.events
    ;
    """
    if args.verbose:        print(insert)

    # insert into datasets
    insdsets, ntries_dsets, start_dsets, finish_dsets, ex_dsets, nm_dsets, sv_dsets = dbQuery( cnxn_string_map[ 'fcw' ], insert )    
            
    print(".... insert into datasets executed ....")
    
    if insfiles and insdsets:
        insfiles.commit()
        insdsets.commit()
    

    # Add to nevents in the production status
    if args.verbose:
        print("Update nevents")

    tablename=args.table
    dstname=args.dstname
    run=int(args.run)
    seg=int(args.segment)
    if args.prodtype=="many": seg=0
    id_ = getLatestId( tablename, dstname, run, seg )
    update=None

    if args.inc:
        update = f"""
        update {tablename}
        set nevents=nevents+{args.nevents},nsegments=nsegments+1,message='last stageout {filename}'
        where id={id_}
        """
        #            where dstname='{dstname}' and id={id_} and run={run} and segment={seg};
    else:
        update = f"""
        update {tablename}
        set nevents={args.nevents},nsegments=nsegments+1,message='last stageout {filename}'
        where id={id_}
        """
        #            where dstname='{dstname}' and id={id_} and run={run} and segment={seg};

    curs, ntries_stat, start_stat, finish_stat, ex_stat, nm_stat, sv_stat = dbQuery( cnxn_string_map[ 'statw' ], update )
    if curs:
        curs.commit()
        
    # and remove the file
    if args.verbose:
        print("Cleanup file")        

    # Could cache the files here ...
    os.remove(  f"{filename}")

    return 'result.files', ntries_files, start_files, finish_files, ex_files, nm_files, sv_files, 'result.datasets', ntries_dsets, start_dsets, finish_dsets, ex_dsets, nm_dsets, sv_dsets, 'result.status', ntries_stat, start_stat, finish_stat, ex_stat, nm_stat, sv_stat    


@subcommand([
])
def stagein(args):
    """
    """
    print("Not implemented")


#_______________________________________________________________________________________________________
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

    print("[CUPS WARNING]: execute is deprecated")
    return

#_______________________________________________________________________________________________________
@subcommand([
    argument( "--qafile", help="Read the given QA file and save as a jsonb entry in the production quality table" )
])
def quality(args):

    print("[CUPS WARNING]: quality is deprecated")
    return

    
def main():

    cupsid=os.getenv('cupsid', 0)    

    args=parser.parse_args()
    if args.subcommand is None:
        parser.print_help()
        return

    result = []

    result = args.func(args)

    with open( 'cups.stat', 'a' ) as stats:

        for r in [result[i:i + 7] for i in range(0, len(result), 7)]:
            stats.write( f"{args.subcommand},{cupsid},{args.dstname},{args.run},{args.segment},{platform.node()}" )
            for x in r:
                stats.write(",")
                stats.write(str(x))
            stats.write("\n")

if __name__ == '__main__':
    main()
