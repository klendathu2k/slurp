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

# Production status ... 
statusdb  = pyodbc.connect("DSN=ProductionStatusWrite")
statusdbc = statusdb.cursor()

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
    query=f"""
    select id from {tablename} where dstname='{dstname}' and run={run} and segment={seg} order by id desc limit 1;
    """
    result = statusdbc.execute(query).fetchone()[0]
    return result

# The submitting and submitted states are handled internally by slurp, and should not be 
# set by the running job.

#@subcommand()
#def submitting(args):
#    """
#    Executed by slurp when the jobs are being submitted to condor.
#    """
#    tablename=args.table
#    dstname=args.dstname
#    timestamp=args.timestamp
#    run=int(args.run)
#    seg=int(args.segment)
#    id_ = getLatestId( tablename, dstname, run, seg )
#    update = f"""
#    update {tablename}
#    set status='submitting',submitting='{timestamp}'
#    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
#    """
#    if args.noupdate:
#        print(update)
#    else:
#        print(update)
#        statusdbc.execute( update )
#        statusdbc.commit()

#@subcommand()
#def submitted(args):
#    """
#    Executed by slurp when the jobs have been submitted to condor.
#    """
#    tablename=args.table
#    dstname=args.dstname
#    timestamp=args.timestamp
#    run=int(args.run)
#    seg=int(args.segment)
#    id_ = getLatestId( tablename, dstname, run, seg )
#    update = f"""
#    update {tablename}
#    set status='submitted',submitted='{timestamp}'
#    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
#    """
#    if args.noupdate:
#        print(update)
#    else:
#        print(update)
#        statusdbc.execute( update )
#        statusdbc.commit()

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
    update = f"""
    update {tablename}
    set status='started',started='{timestamp}'
    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
    """
    if args.noupdate:
        print(update)
    else:
        statusdbc.execute( update )
        statusdbc.commit()

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
    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
    """
    if args.noupdate:
        print(update)
    else:
        print(update)
        statusdbc.execute( update )
        statusdbc.commit()

#_______________________________________________________________________________________________________
@subcommand([
    argument("-e","--exit",help="Exit code of the payload macro",dest="exit",default=-1),
    argument(     "--nsegments",help="Number of segments produced",dest="nsegments",default=1),
    argument(     "--nevents",  help="Number of events produced",dest="nevents",type=int,default=0)
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
    update = f"""
    update {tablename}
    set status='{state}',ended='{timestamp}',nsegments={ns},exit_code={ec},nevents={ne}
    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
    """
    if args.noupdate:
        print(update)
    else:
        print(update)
        statusdbc.execute( update )
        statusdbc.commit()

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
    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
    """
    if args.noupdate:
        print(update)
    else:
        print(update)
        statusdbc.execute( update )
        statusdbc.commit()


#_______________________________________________________________________________________________________
@subcommand([
    argument(     "--nevents",  help="Number of events produced",dest="nevents",type=int,default=0),
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
    update = f"""
    update {tablename}
    set nevents={ne}
    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
    """
    if args.noupdate:
        print(update)
    else:
        print(update)
        statusdbc.execute( update )
        statusdbc.commit()

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
    where dstname='{dstname}' and run={run} and segment={seg} and id={id_}
    """
    if args.noupdate:
        print(update)
    else:
        print(update)
        statusdbc.execute( update )
        statusdbc.commit()


# files
# lfn | full_host_name | full_file_path | time | size | md5 
# datasets
# filename | runnumber | segment | size | dataset | dsttype | events 

#parser.add_argument( "--ext", help="file extension, e.g. root, prdf, ...", default="prdf" )
#parser.add_argument( "--path", help="path to output file", default="./" )

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
    # TODO: switch to an update mode rather than a delete / replace mode.
    replace  = args.replace
    tablename= args.table
    dstname  = args.dstname
    timestamp= args.timestamp
    run      = int(args.run)
    seg      = int(args.segment)
    ext      = args.ext
    host     = args.hostname
    nevents  = args.nevents

    # n.b. not the slurp convention for dsttype
    dsttype='_'.join( dstname.split('_')[-2:] )

    # TODO: allow to specify the filename
    filename = f"{dstname}-{run:08}-{seg:04}.{ext}"

    # File catalog
    fc = pyodbc.connect("DSN=FileCatalog;UID=phnxrc")
    fcc = fc.cursor()

    dataset = args.dataset

    checkfile = fcc.execute( f"select size,full_file_path from files where lfn='{filename}';" ).fetchall()
    if checkfile and replace:
        fcc.execute(f"delete from files where lfn='{filename}';")
        fcc.commit()
        

    checkdataset = fcc.execute( f"select size from datasets where filename='{filename}' and dataset='{dataset}';" ).fetchall()
    if checkdataset and replace:
        fcc.execute(f"delete from datasets where  filename='{filename}' and dataset='{dataset}';" )
        fcc.commit()

    # Calculate md5 checksum
    md5 = md5sum( f"{filename}")#  #sh.md5sum( f"{args.path}/{filename}").split()[0]
    #sz  = int( sh.stat( '--printf=%s', f"{args.path}/{filename}" ) )
    sz  = int( os.path.getsize(f"{filename}") ) 

    # Insert into files
    insert=f"""
    insert into files (lfn,full_host_name,full_file_path,time,size,md5) 
    values ('{filename}','{host}','{args.path}/{filename}','now',{sz},'{md5}');
    """
    fcc.execute(insert)
    fcc.commit()

    # Insert into datasets
    insert=f"""
    insert into datasets (filename,runnumber,segment,size,dataset,dsttype,events)
    values ('{filename}',{run},{seg},{sz},'{dataset}','{dsttype}',{args.nevents})
    """
    fcc.execute(insert)
    fcc.commit()

#_______________________________________________________________________________________________________
@subcommand([
    argument( "filename", help="Name of the file to be staged out"),
    argument( "outdir",   help="Output directory" ),
    argument( "--retries", help="Number of retries before silent failure", type=int, default=1 ),
    argument( "--hostname", help="host name of the filesystem", default="lustre", choices=["lustre","gpfs"] ),
    argument( "--nevents",  help="Number of events produced",dest="nevents",type=int,default=0),
    argument( "--dataset", help="sets the name of the dataset", default="test" ),
    #argument( "--add-to-files",    dest="add_to_files", help="Adds to the file catalog", default=True, action="store_true"),
    #argument( "--no-add-to-files", dest="add_to_files", help="Do not add to the file catalog", action="store_false"),
    #argument( "--add-to-datasets",    dest="add_to_datasets", help="Adds to the file catalog", default=True, action="store_true"),
    #argument( "--no-add-to-datasets", dest="add_to_datasets", help="Do not add to the file catalog", action="store_false"),    
])
def stageout(args):
    """
    Stages the given file out to the specified 
    """
    md5true  = md5sum( args.filename ) # md5 of the file we are staging out

    # Stage the file out to the target directory.
    print("Copy back file")
    shutil.copy2( f"{args.filename}", f"{args.outdir}" )
    md5check = md5sum( f"{args.outdir}/{args.filename}" )

    print("Checksum before and after")
    print(md5true)
    print(md5check)

    # Unlikely to have failed w/out shutil throwing an error
    if md5true==md5check:

        # Copy succeeded.  Connect to file catalog and add to it
        fc = pyodbc.connect("DSN=FileCatalogWrite;UID=phnxrc")
        fcc = fc.cursor()
        
        # TODO: switch to an update mode rather than a delete / replace mode.
        timestamp= args.timestamp
        run      = int(args.run)
        seg      = int(args.segment)
        host     = args.hostname
        nevents  = args.nevents

        # n.b. not the slurp convention for dsttype
        dstname  = args.dstname
        dsttype='_'.join( dstname.split('_')[-2:] )
        
        sz  = int( os.path.getsize(f"{args.filename}") ) #int( sh.stat( '--printf=%s', f"{args.filename}" ) )
        md5=md5check

        # Strip off any leading path 
        filename=args.filename.split('/')[-1]

        

        # Insert into files primary key: (lfn,full_host_name,full_file_path)
        print("Insert into files")
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
        print(insert)
        fcc.execute(insert)
        fcc.commit()



        # Insert into datasets primary key: (filename,dataset)
        print("Insert into datasets")
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
        print(insert)
        fcc.execute(insert)
        fcc.commit()

        # and remove the file
        print("Cleanup file")        
        os.remove( f"{filename}")


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
            
    finished=parser.parse_args( ["-d", args.dstname, "-r", args.run, "-s", args.segment, state, "-e","%s"%result.exit_code ] ); 
    finished.func(finished)

#_______________________________________________________________________________________________________
@subcommand([
    argument( "--qafile", help="Read the given QA file and save as a jsonb entry in the production quality table" )
])
def quality(args):
    tablename = args.table             # the production_status table
    dstname   = args.dstname
    run       = int( args.run )
    segment   = int( args.segment )
    id_       = getLatestId( tablename, dstname, run, segment )    # the corresponding production status entry
    qastring  = None
    with open( args.qafile, 'r') as qafile:
        qastring = str( json.load( qafile ) )

    # Make sure to replace the single quotes with double
    qastring = qastring.replace("'",'"')

    qaentry=f"""
    INSERT INTO production_quality (stat_id,dstname,run,segment,qual) values
      ( {id_},'{dstname}',{run},{segment},'{qastring}' );   
    """

    # File catalog
    statusdbc.execute(qaentry)
    statusdbc.commit()    


def main():

    args=parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)        

if __name__ == '__main__':
    main()
