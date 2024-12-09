#!/usr/bin/env python

import pyodbc
import htcondor
import classad
import re
import uuid
import os
import pathlib
import pprint
import sh
import argparse
import datetime
import time
import itertools
from  glob import glob
import math
import platform
import random

from slurptables import SPhnxProductionSetup
from slurptables import SPhnxProductionStatus
from slurptables import SPhnxInvalidRunList
from slurptables import sphnx_production_status_table_def

from dataclasses import dataclass, asdict, field

from simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

import logging

# This is the maximum number of DST names / types that will be in production at any one time
MAXDSTNAMES = 100

# List of states which block the job
blocking = ["submitting","submitted","started","running","evicted","failed","finished"]
#blocking = []
args     = None
userargs = None

__frozen__ = True
__rules__  = []

# File Catalog (and file catalog cursor)
# TODO: exception handling... if we can't connect, retry at some randomized point in the future.
# ... and set a limit on the number of retries before we bomb out ...
#fc = pyodbc.connect("DSN=FileCatalog")
#fcc = fc.cursor()

def printDbInfo( cnxn, title ):
    name=cnxn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv=cnxn.getinfo(pyodbc.SQL_SERVER_NAME)
    print(f"Connected {name} from {serv} as {title}")

try:
    statusdbr_ = pyodbc.connect("DSN=ProductionStatus")    
    statusdbr = statusdbr_.cursor()
    printDbInfo( statusdbr_, "Production Status Table [reads]" )

except pyodbc.InterfaceError:
    for s in [ 10*random.random(), 20*random.random(), 30*random.random() ]:
        print(f"Could not connect to DB... retry in {s}s")
        time.sleep(s)
        try:
            statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
            statusdbr = statusdbr_.cursor()
        except:
            exit(0)
except pyodbc.Error as e:
    print(e)
    exit(0)


try:
    statusdbw_ = pyodbc.connect("DSN=ProductionStatusWrite")
    statusdbw = statusdbw_.cursor()
    printDbInfo( statusdbw_, "Production Status Table [writes]" )

except (pyodbc.InterfaceError) as e:
    for s in [ 10*random.random(), 20*random.random(), 30*random.random() ]:
        print(f"Could not connect to DB... retry in {s}s")
        time.sleep(s)
        try:
            statusdbw_ = pyodbc.connect("DSN=ProductionStatusWrite")
            statusdbw = statusdbw_.cursor()
            break
        except:
            pass
    else:
        exit(0) # no break in for loop
except pyodbc.Error as e:
    print(e)
    exit(0)

fcro  = pyodbc.connect("DSN=FileCatalog;READONLY=True")
fccro = fcro.cursor()
printDbInfo( fcro, "File Catalog [reads]" )

try:
    daqdb = pyodbc.connect("DSN=daq;UID=phnxrc;READONLY=True");
    daqc = daqdb.cursor()
    printDbInfo( daqdb, "DAQ database [reads]" )

except:
    daqdb = None
    daqc = None



rawdr_ = pyodbc.connect("DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True")
rawdr  = rawdr_.cursor()
printDbInfo( rawdr_, "RAW database [reads]" )


#print(f"ProductionStatus [RO]: timeout {statusdbr_.timeout}s")
#print(f"ProductionStatus [Wr]: timeout {statusdbw_.timeout}s")
#print(f"FileCatalog [RO]:      timeout {fcro.timeout}s")
#print(f"DaqDB [RO]:            timeout {daqdb.timeout}s")

cursors = { 
    'daq':rawdr,
    'fc':fccro,
    'fccro':fccro,
    'daqdb':rawdr,
    'filecatalog': fccro,
    'status' : statusdbr,
    'raw':rawdr,
    'rawdr':rawdr,
}

cnxn_string_map = {
    'daq'         : 'DSN=daq;UID=phnxrc;READONLY=True',
    'daqdb'       : 'DSN=daq;UID=phnxrc;READONLY=True',
    'fc'          : 'DSN=FileCatalog;READONLY=True',
    'fccro'       : 'DSN=FileCatalog;READONLY=True',
    'filecatalog' : 'DSN=FileCatalog;READONLY=True',
    'status'      : 'DSN=ProductionStatus',
    'statusw'     : 'DSN=ProductionStatusWrite',
    'raw'         : 'DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True',
    'rawdr'       : 'DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True',
}

def dbQuery( cnxn_string, query, ntries=10 ):

    print(f"dbQuery {cnxn_string}")

    # Some guard rails
    assert( 'delete' not in query.lower() )    
    assert( 'insert' not in query.lower() )    
    assert( 'update' not in query.lower() )    
    assert( 'select'     in query.lower() )

    lastException = None
    
    # Attempt to connect up to ntries
    for itry in range(0,ntries):
        try:
            conn = pyodbc.connect( cnxn_string )

            printDbInfo( conn, f"Connected {cnxn_string}" )
            curs = conn.cursor()
            curs.execute( query )
            return curs
                
        except Exception as E:
            lastException = E
            delay = (itry + 1 ) * random.random()
            time.sleep(delay)

    print(lastException)
    exit(0)
            

    

verbose=0

#
# Format strings for run and segment numbers.  n.b. that the "rungroup" which defines the logfile and output file directory structure
# hardcodes "08d" as the run format...  
#
RUNFMT = "%08i"
SEGFMT = "%05i"
DSTFMT = "%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"

@dataclass
class SPhnxCondorJob:
    """
    Condor submission job template.
    """
    universe:              str = "vanilla"
    executable:            str = "$(script)"    
    arguments:             str = "$(nevents) $(run) $(seg) $(lfn) $(indir) $(dst) $(outdir) $(buildarg) $(tag) $(ClusterId) $(ProcId)"
    batch_name:            str = "$(name)_$(build)_$(tag)"
    #output:                str = f"$(name)_$(build)_$(tag)-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT}).stdout"
    #error:                 str = f"$(name)_$(build)_$(tag)-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT}).stderr"
    output:                str = None 
    error:                 str = None
    log:                   str = f"$(condor)/$(name)_$(build)_$(tag)-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT}).condor"
#   periodichold: 	   str = "(NumJobStarts>=1 && JobStatus == 1) || (NumJobStarts>=2 && JobStatus == 2)"
    periodichold: 	   str = "(NumJobStarts>=1 && JobStatus == 1)"
    priority:              str = "1958"
    job_lease_duration:    str = "3600"
    requirements:          str = '(CPU_Type == "mdc2")';    
    request_cpus:          str = "1"
    request_memory:        str = "$(mem)"
    should_transfer_files: str = "YES"
    output_destination:    str = "file://./output/"
    #output_destination:    str = "file:////sphenix/data/data02/sphnxpro/condorlog/$$($(run)/100)00"
    when_to_transfer_output: str = "ON_EXIT"
    request_disk:          str = None    
    initialdir:            str = None
    accounting_group:      str = None
    accounting_group_user: str = None
#   transfer_output_files: str = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).out,$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).err"
    transfer_output_files: str = '""'
    transfer_output_remaps: str = None
    
    transfer_input_files:  str = None
    user_job_wrapper:      str = None
    max_retries:           str = None # No default...
    request_xferslots:     str = None

    transferout:           str = "false"
    transfererr:           str = "false"

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

    def __post_init__(self):

        if args:
            object.__setattr__( self, 'batch_name', args.batch_name )

@dataclass( frozen= __frozen__ )
class SPhnxRule:
    name:              str            # Name of the rule
    script:            str            # Production script
    build:             str            # Build tag
    tag:               str            # Database tag
    files:             str  = None    # Input files query
    filesdb:           str  = None    # Input files DB to query
    runlist:           str  = None    # Input run list query from daq
    direct:            str  = None    # Direct path to input files (supercedes filecatalog)
    job:               SPhnxCondorJob = SPhnxCondorJob()
    resubmit:          bool = False   # Set true if job should overwrite existing job
    buildarg:          str  = ""      # The build tag passed as an argument (leaves the "." in place).
    payload:           str = "";      # Payload directory (condor transfers inputs from)
    limit:    int = 0                 # maximum number of matches to return 0=all
    runname:           str = None     # eg run2pp, extracted from name or ...

    def __eq__(self, that ):
        return self.name == that.name
    
    def __post_init__(self):
        # Verify the existence of the production script
        #    ... no guarentee that the script is actually at this default path ...
        #    ... it could be sitting in the intialdir of the job ...
        path_ = ""
        if self.job.initialdir:
            path_ = self.job.initialdir + "/"
        #assert( pathlib.Path( path_ + self.script ).exists() )

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)        

        if self.runname==None:
            object.__setattr__(self, 'runname', self.name.split('_')[-1])

        # Add to the global list of rules
        __rules__.append(self)

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }        
        

@dataclass( frozen = __frozen__ )
class SPhnxMatch:
    name:     str = None;        # Name of the matching rule
    script:   str = None;        # The run script
    lfn:      str = None;        # Logical filename that matches
    dst:      str = None;        # Transformed output
    run:      str = None;        # Run #
    seg:      str = None;        # Seg #
    build:    str = None;        # Build
    tag:      str = None;        # DB tag
    mem:      str = None;        # Required memory
    disk:     str = None;        # Required disk space
    payload:  str = None;        # Payload directory (condor transfers inputs from)
    #manifest: list[str] = field( default_factory=list );  # List of files in the payload directory
    stdout:   str = None; 
    stderr:   str = None; 
    condor:   str = None;
    buildarg: str = None;
    inputs:   str = None;
    ranges:   str = None;
    rungroup: str = None;
    firstevent: str = None;
    lastevent: str = None;
    runs_last_event: str = None;
    neventsper : str = None
    #intputfile: str = None;
    #outputfile: str = None;


    def __eq__( self, that ):
        return self.run==that.run and self.seg==that.seg

    def __post_init__(self):

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)                
        run = int(self.run)
        object.__setattr__(self, 'rungroup', f'{100*math.floor(run/100):08d}_{100*math.ceil((run+1)/100):08d}')

        #sldir = "/tmp/slurp/%i"%( math.trunc(run/100)*100 )
        #if self.condor == None: object.__setattr__(self, 'condor', sldir )
        #sldir = "/sphenix/data/data02/sphnxpro/condorlogs/%i"%( math.trunc(run/100)*100 )            
        #if self.stdout == None: object.__setattr__(self, 'stdout', sldir )
        #if self.stderr == None: object.__setattr__(self, 'stderr', sldir )

#    def __post_init__(self):
#        if self.condor == None:
#            a = int(self.run)
#            self.condor = "/tmp/slurp/%i"%( math.trunc(a/100)*100 )

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }


def table_exists( tablename ):
    """
    """ 
    result = False
    if statusdbr.tables( table=tablename.lower(), tableType='TABLE' ).fetchone():
        result = True
    return result



def fetch_production_status( setup, runmn=0, runmx=-1, update=True, dstname=" " ):
    """
    Given a production setup, returns the production status table....
    """
    result = [] # of SPhnxProductionStatus

    name = "PRODUCTION_STATUS"
    
    if table_exists( name ):

        query = f"select * from {name} where true"
        if ( runmn>runmx ): 
            query = query + f" and run>={runmn}"
        else              : 
            query = query + f" and run>={runmn} and run<={runmx}"

        #if dstname is not None:
        #    query = query + f" and dstfile like '{dstname}%'"

        query=query+";"

        dbresult = dbQuery( cnxn_string_map['statusw'], query )

        # Transform the list of tuples from the db query to a list of prouction status dataclass objects
        result = [ SPhnxProductionStatus( *db ) for db in dbresult if dstname in db.dstfile ]

    elif update==True: # note: we should never reach this state ...  tables ought to exist already

        create = sphnx_production_status_table_def( setup.name, setup.build, setup.dbtag )

        statusdbw.execute(create) # 
        statusdbw.commit()
        

    return result

def fetch_invalid_run_entry( dstname, run, seg ):
    query = f"""
    select 
    ,   id
    ,   dstname
    ,   first_run
    ,   last_run
    ,   first_segment
    ,   last_segment
        expires_at at time zone 'utc' as expires 
        from invalid_run_list
    where 
        (dstname='{dstname}' or dstname='all' or dstname='ALL' ) and first_run<={run} and ( last_run>={run} or last_run=-1 ) and first_segment<={segment} and last_segment>={segment};       
    """


    return [ 
        SPhnxInvalidRunList(*db) 
        for db in 
               statusdbr.execute( query ).fetchall() 
    ]


#def getLatestId( tablename, dstname, run, seg ):  # limited to status db
#    query=f"""
#    select id from {tablename} where dstname='{dstname}' and run={run} and segment={seg} order by id desc limit 1;
#    """
#    result = statusdbw.execute(query).fetchone()[0]
#    return result

def getLatestId( tablename, dstname, run, seg ):

    cache="cups.cache"
    
    # We are limiting to the list of all productions for a given run,segment pair.

    result  = 0
    query=f"""
    select id,dstname from {tablename} where run={run} and segment={seg} order by id desc limit {MAXDSTNAMES};
    """
    # Find the most recent ID with the given dstname

    for r in dbQuery( cnxn_string_map[ 'statusw' ], query ):
        if r.dstname == dstname:
            result = r.id
            break

    # Widen the search if needed...
    if result==0:
        query=f"""
        select id,dstname from {tablename} where run={run} and segment={seg} order by id desc limit {MAXDSTNAMES*10};
        """
        for r in dbQuery( cnxn_string_map[ 'statusw' ], query ):
            if r.dstname == dstname:
                result = r.id
                break

    if result==0:
        print(f"Warning: could not find {dstname} with run={run} seg={seg}... this may not end well.")

    return result

def update_production_status( matching, setup, condor, state ):

    # Condor map contains a dictionary keyed on the "output" field of the job description.
    # The map contains the cluster ID, the process ID, the arguments, and the output log.
    # (This is the condor.stdout log...)
    condor_map = {}
    for ad in condor:
        clusterId = ad['ClusterId']
        procId    = ad['ProcId']
        out       = ad['Out'].split('/')[-1]   # discard anything that looks like a filepath
        ulog      = ad['UserLog'].split('/')[-1] 
        key       = ulog.split('.')[0].lower()  # lowercase b/c referenced by file basename

        condor_map[key]= { 'ClusterId':clusterId, 'ProcId':procId, 'Out':out, 'UserLog':ulog }

    name = sphenix_dstname( setup.name, setup.build, setup.dbtag )

    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])

        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )

        try:
            cluster = condor_map[ key.lower() ][ 'ClusterId' ]
            process = condor_map[ key.lower() ][ 'ProcId'    ]
        except KeyError:
            ERROR("Key Error getting cluster and/or process number from the class ads map.")
            ERROR(f"  key={key}")
            ERROR("Assuming this is an issue with condor, setting cluster=0, process=0 and trying to continue...")
            cluster=0
            process=0
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag
        dstfile=( dstname + '-' + RUNFMT + '-' + SEGFMT ) % (run,segment)

        # 1s time resolution
        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        id_ = getLatestId( 'production_status', dstname, run, segment )

        update=f"""
        update  production_status
        set     status='{state}',{state}='{timestamp}',cluster={cluster},process={process}
        where id={id_}
        """
        
        statusdbw.execute(update)

    statusdbw.commit()

def insert_production_status( matching, setup, condor=[], state='submitting' ):

    # Condor map contains a dictionary keyed on the "output" field of the job description.
    # The map contains the cluster ID, the process ID, the arguments, and the output log.
    # (This is the condor.stdout log...)
    condor_map = {}
    for ad in condor:
        clusterId = ad['ClusterId']
        procId    = ad['ProcId']
        out       = ad['Out'].split('/')[-1]   # discard anything that looks like a filepath
        ulog      = ad['UserLog'].split('/')[-1] 
        key       = ulog.split('.')[0].lower()  # lowercase b/c referenced by file basename

        condor_map[key]= { 'ClusterId':clusterId, 'ProcId':procId, 'Out':out, 'UserLog':ulog }

    # replace with sphenix_dstname( setup.name, setup.build, setup.dbtag )
    name = sphenix_dstname( setup.name, setup.build, setup.dbtag )

    values = []

    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])
        dstfileinput = m['lfn'].split('.')[0]

        # If the match contains a list of inputs... we will set it in the production status...
        if m['inputs']:
            dstfileinput=m['inputs']
        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag
        dstfile=( dstname + '-' + RUNFMT + '-' + SEGFMT ) % (run,segment)
        
        prod_id = setup.id
        try:
            cluster = condor_map[ key.lower() ][ 'ClusterId' ]
            process = condor_map[ key.lower() ][ 'ProcId'    ]
        except KeyError:
            cluster = 0
            process = 0

        status  = state        

        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        # TODO: Handle conflict
        node=platform.node().split('.')[0]

        insert=f"""
        insert into production_status
               (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents, submission_host )

        values ('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'{dstfileinput}',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0, '{node}' )
        """

        values.append( f"('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'{dstfileinput}',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0, '{node}' )" )
        
    insvals = ','.join(values)

    insert = f"""
    insert into production_status
           (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents, submission_host )
    values 
           {insvals}

    returning id
    """
    statusdbw.execute(insert)
    # commit is deferred until the update succeeds

    result=[ int(x.id) for x in statusdbw.fetchall() ]

    return result
    

        


def submit( rule, maxjobs, **kwargs ):

    # Will return cluster ID
    result = 0

    actions = [ "dump" ]

    dump = kwargs.get( "dump", False )
    if dump:
        del kwargs["dump"];

    if args:
        kwargs['resubmit'] = args.resubmit

    # Build list of LFNs which match the input
    matching, setup, runlist = matches( rule, kwargs )

    nmatches = len(matching)
    if len(matching)==0:
        return result

    #
    # Resubmit is only a manual operation.  Existing files must be removed or the DB query adjusted to avoid
    # stomping on previous output files.
    #
    if kwargs.get('resubmit',False):
        reply = None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Warning: resubmit option may overwrite previous production.  Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result
 
    #
    # An unclean setup is also cause for manual intervention.  It will hold up any data production.
    #    (but we will allow override with the batch flag)
    #
    if not ( setup.is_clean and setup.is_current ) and args.batch==False:
        if setup.is_clean  ==False: WARN("Uncommitted local changes detected")
        if setup.is_current==False: WARN("Local repo needs to be updated")

        reply=None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result        


    INFO("Get the job dictionary")
    jobd = rule.job.dict()

    # Append $(cupsid) as the last argument
    jobd['arguments'] = jobd['arguments'] + ' $(cupsid)'

    INFO("Passing job to htcondor.Submit")
    submit_job = htcondor.Submit( jobd )
    if verbose>0:
        INFO(submit_job)
        if verbose>10:
            for m in matching:
                pprint.pprint(m)

    dispatched_runs = []


    #
    # At this point in the code, matching jobs are storred in the array 'matching'.
    # All jobs in this array are ripe for submission.  If maxjobs is defined, this
    # is the point where we can truncate the matches...
    #
    if maxjobs:
        INFO(f"Truncating the number of jobs to maxjobs={maxjobs}")        
        matching = matching[:int(maxjobs)]


    if dump==False:
        if verbose==-10:
            INFO(submit_job)
        
        schedd = htcondor.Schedd()    

        runtypes = {}
        # Strip out unused $(...) condor macros
        INFO("Converting matches to list of dictionaries for schedd...")
        mymatching = []
        for m in iter(matching):
            d = {}

            # TODO:  This should be accessed from the run table / daqdb
            runtype='none'
            d['runtype']='unset'
            d['runname']=rule.runname

            # massage the inputs from space to comma separated
            if m.get('inputs',None): 
                m['inputs']= ','.join( m['inputs'].split() )
                if '/physics/' in m['inputs']: # physics can appear twice by mistake...
                    runtype = 'physics'
                    d['runtype']=runtype
                if '/beam/' in m['inputs']: # beam supercedes...
                    runtype = 'beam'
                    d['runtype']=runtype
                if '/cosmics/' in m['inputs']: # beam supercedes...
                    runtype = 'cosmics'
                    d['runtype']=runtype
                
            runtypes[runtype]=1 # register the runtype for directory creation below

            if m.get('ranges',None):
                m['ranges']= ','.join( m['ranges'].split() )

            for k,v in m.items():

                if k in ['outdir','logdir','histdir','condor']:
                    m[k] = v.format( **locals() )

                if k in str(submit_job):
                    d[k] = m[k]
               
                if args.dbinput: 
                    d['inputs']= 'dbinput'            
                    d['ranges']= 'dbranges'


            mymatching.append(d)        
            dispatched_runs.append( (d['run'],d['seg']) )
                
        run_submit_loop=30
        schedd_query = None
        
        # Insert jobs into the production status table and add the ID to the dictionary
        INFO("... insert")
        cupsids = insert_production_status( matching, setup, [], state="submitting" ) 
        for i,m in zip(cupsids,mymatching):
            m['cupsid']=str(i)

        
        INFO("Preparing to submitting the jobs to condor")
        try:

            INFO("... creating directories if they do not exist")
            for outname in [ 'outdir', 'logdir', 'condor', 'histdir' ]:

                outdir=kwargs.get(outname,None)
                if outdir==None: continue
                outdir = outdir.replace('file:/','')
                outdir = outdir.replace('//','/')

                outdir = outdir.replace( '$(rungroup)', '{rungroup}')
                outdir = outdir.replace( '$(build)',    '{rule.build}' )
                outdir = outdir.replace( '$(tag)',      '{rule.tag}' )
                outdir = outdir.replace( '$(name)',     '{rule.name}' )
                outdir = outdir.replace( '$(runname)',  '{rule.runname}' )
                                 
                outdir = f'f"{outdir}"'

                rungroups = {}
                for run in runlist:
                    mnrun = 100 * ( math.floor(run/100) )
                    mxrun = mnrun+100
                    rungroup=f'{mnrun:08d}_{mxrun:08d}'                
                    for runtype in runtypes.keys():  # runtype is a possible KW in the yaml file that can be substituted
                        pathlib.Path( eval(outdir) ).mkdir( parents=True, exist_ok=True )            

            # submits the job to condor
            INFO("... submitting to condor")

            pprint.pprint(mymatching)

            submit_result = schedd.submit(submit_job, itemdata=iter(mymatching))  # submit one job for each item in the itemdata
            # commits the insert done above
            statusdbw.commit()

        except:
            # if condor did not accept the jobs, rollback to the previous state and 
            statusdbw.rollback()
            raise
            
        INFO("Getting back the cluster and process IDs")
        schedd_query = schedd.query(
            constraint=f"ClusterId == {submit_result.cluster()}",
            projection=["ClusterId", "ProcId", "Out", "UserLog", "Args" ]
        )

        # Update DB IFF we have a valid submission
        INFO("Insert and update the production_status")
        if ( schedd_query ):
            

            INFO("... result")
            result = submit_result.cluster()            

            INFO("... update")
            update_production_status( matching, setup, schedd_query, state="submitted" )


    else:
        order=["script","name","nevents","run","seg","lfn","indir","dst","outdir","buildarg","tag","stdout","stderr","condor","mem"]           
        with open( "submit.job", "w" ) as f:
            f.write( str(submit_job) )
            line = "queue ";
            for k in order:
                line += str(k) + ", "
            line += "from submit.in\n"
            f.write(line)
            
        with open( "submit.in", "w" ) as f:
            for m in matching:
                line = []
                for k in order:                    
                    line.append( str(m[k]) )
                line = ','.join(line)                
                f.write(line+"\n")

    return dispatched_runs

def fetch_production_setup( name, build, dbtag, repo, dir_, hash_ ):
    """
    Fetches the production setup from the database for the given (name,build,dbtag,hash).
    If it doesn't exist in the DB it is created.  Queries the git repository to verify 
    that the local repo is clean and up to date with the remote.  Returns production setup
    object.
    """

    result = None # SPhnxProductionSetup

    query="""
    select id,hash from production_setup 
           where name='%s'  and 
                 build='%s' and 
                 dbtag='%s' and 
                 hash='%s'
                 limit 1;
    """%( name, build, dbtag, hash_ )
    
    #array = list( statusdbw.execute( query ).fetchall() )
    array = [ x for x in dbQuery( cnxn_string_map['statusw'], query ) ]
    assert( len(array)<2 )

    if   len(array)==0:
        insert="""
        insert into production_setup(name,build,dbtag,repo,dir,hash)
               values('%s','%s','%s','%s','%s','%s');
        """%(name,build,dbtag,repo,dir_,hash_)

        statusdbw.execute( insert )
        statusdbw.commit()

        result = fetch_production_setup(name, build, dbtag, repo, dir_, hash_)

    elif len(array)==1:


        # Check to see if the payload has any local modifications
        is_clean = len( sh.git("-c","color.status=no","status","-uno","--short",_cwd=dir_).strip().split('\n') ) == 0;

        # git show origin/main --format=%h -s
        remote_hash = sh.git("show","origin/main","--format=%h","-s").strip()
        is_current = (hash_ == remote_hash)

        id_ = int( array[0][0] )
        old_hash = str( array[0][1] )
        is_current = ( hash_ == old_hash and remote_hash == old_hash )

        # We reach this point in the code under two conditions... 1) the production_setup
        # was found in the DB, or 2) this is a recursive call after we just created
        # the setup.  So...
        #
        # We can return the setup based on the ID provided in the DB.
        # We can return a setup based on the arguments passed to the function b/c
        # 1) If it did not exist in the DB already, it was just created
        # 2) OR it exists... the repo and local directories do not matter... but if the hash
        #    has changed it is a problem...  
        # Should issue a warning before submitting in case clean or current is violated...
        result=SPhnxProductionSetup( id_, name, build, dbtag, repo, dir_, hash_, is_clean, is_current )

    return result

        
def sphenix_dstname( dsttype, build, dbtag ):
    result = "%s_%s_%s"%( dsttype, build.replace(".",""), dbtag )
    return result

def sphenix_base_filename( dsttype, build, dbtag, run, segment ):
    #result = "%s-%08i-%04i" %( sphenix_dstname(dsttype, build, dbtag), int(run), int(segment) )
    result = ("%s-" + RUNFMT + "-" + SEGFMT) % ( sphenix_dstname(dsttype, build, dbtag), int(run), int(segment) )
    return result
    

def matches( rule, kwargs={} ):
    """
    
    Apply rule... extract files from DB according to the specified query
    and build the matches.  Return list of matches.  Return the production
    setup from the DB.
    
    """
    global args

    result = []

    name      = kwargs.get('name',      rule.name)
    build     = kwargs.get('build',     rule.build)      # TODO... correct handling from submit.  build=ana.xyz --> build=anaxyz buildarg=ana.xyz
    buildarg  = kwargs.get('buildarg',  rule.buildarg)
    tag       = kwargs.get('tag',       rule.tag)
    script    = kwargs.get('script',    rule.script)
    resubmit  = kwargs.get('resubmit',  rule.resubmit)
    payload   = kwargs.get('payload',   rule.payload)
    update    = kwargs.get('update',    True ) # update the DB

    outputs = []

    # Build list of possible outputs from filelist query... (requires run,sequence as 2nd and 3rd
    # elements in the query result)
    fc_result  = []

    rl_result = None
    rl_map    = None

    lfn_lists  = {}  # LFN lists per run requested in the input query
    pfn_lists  = {}  # PFN lists per run existing on disk
    rng_lists  = {}  # LFN:firstevent:lastevent

    runMin=999999
    runMax=0
    INFO("Building candidate inputs")
    if rule.files:
        curs      = cursors[ rule.filesdb ]

        inputquery = dbQuery( cnxn_string_map[ rule.filesdb ], rule.files )

        outputs = [] # WARNING: len(outputs) and len(fc_result) must be equal

        INFO(f"... {len(fc_result)} inputs")
        for f in inputquery:
            fc_result.append(f) # cache the query
            run     = f.runnumber
            segment = f.segment

            outputs.append( DSTFMT %(name,build,tag,int(run),int(segment)) )

            if run>runMax: runMax=run
            if run<runMin: runMin=run
            if lfn_lists.get(run,None) == None:
                lfn_lists[ f"'{run}-{segment}'" ] = f.files.split()
                rng_lists[ f"'{run}-{segment}'" ] = getattr( f, 'fileranges', '' ).split()
            else:
                # If we hit this result, then the db query has resulted in two rows with identical
                # run numbers.  Violating the implicit submission schema.
                ERROR(f"Run number {run}-{segment} reached twice in this query...")
                ERROR(rule.files)
                exit(1)
    
    if len(lfn_lists)==0: return [], None, []  # Early exit if nothing to be done
    
    # Build dictionary of DSTs existing in the datasets table of the file catalog.  For every DST that is in this list,
    # we know that we do not have to produce it if it appears w/in the outputs list.
    dsttype="%s_%s_%s"%(name,build,tag)  # dsttype aka name above
    
    exists = {}
    INFO("Building list of existing outputs")
    chkq = f"select filename,runnumber,segment from datasets where runnumber>={runMin} and runnumber<={runMax} and filename like'"+dsttype+"%';"
    for check in dbQuery( cnxn_string_map['fccro'], chkq ):
        exists[ check.filename ] = ( check.runnumber, check.segment)  # key=filename, value=(run,seg)
    INFO(f"... {len(exists.keys())} existing outputs")



    # IF we are on a direct (disk) lookup for PFN lists, we will build a map of
    # LFN to PFN here...
    lfn2pfn = {}
    if rule.direct:
        INFO("Building lfn2pfn map from filesystem")
        lfn2pfn = { pfn.split("/")[-1] : pfn for pfn in glob(rule.direct+'/*') }
        INFO("done")

    else:
        INFO("Building lfn2pfn map from filecatalog")
        fcquery=f"""

        with lfnlist as (
   
            select filename from datasets where runnumber>={runMin} and runnumber<={runMax} and dataset='{build}_{tag}'

        )

        select lfn,full_file_path as pfn from 

            lfnlist join files

        on lfnlist.filename=files.lfn;        
        """
        lfn2pfn = { r.lfn : r.pfn for r in dbQuery( cnxn_string_map['fccro'],fcquery ) }

                    
    # Build lists of PFNs available for each run
    INFO("Building PFN lists")
    for runseg,lfns in lfn_lists.items():

        runnumber, segment = runseg.strip("'").split('-')        
        output = DSTFMT %(name,build,tag,int(runnumber),int(segment))
        
        lfns_ = [ f"'{x}'" for x in lfns ]
        list_of_lfns = ','.join(lfns_)

        # Add a new entry in the pfn_lists lookup table
        if pfn_lists.get(runseg,None)==None:
            pfn_lists[runseg]=[]

        # Build list of PFNs via direct lookup and append the results
        try:
            if rule.direct:

                pfn_lists[runseg] = [lfn2pfn[lfn] for lfn in lfns] 

                # Build list of PFNs via filecatalog lookup if direct path has not been specified
            if rule.direct==None:            

                pfn_lists[runseg] = [lfn2pfn[lfn] for lfn in lfns]

        except KeyError:
            print( "No PFN for all LFNs in the input query.")
            print(f"direct_path: {str(rule.direct)}")
            print( "    ... if it is None, you should specify the directory paths where input files can be found")
            print( "        in the input query.")
            print( "    ... if it specifies one or more directories, then your list is incomplete, or there are missing input files.")
            raise KeyError

    INFO(f"... {len(pfn_lists.keys())} pfn lists")

    # 
    # The production setup will be unique based on (1) the specified analysis build, (2) the specified DB tag,
    # and (3) the hash of the local github repository where the payload scripts/macros are found.
    #
    repo_dir  = payload #'/'.join(payload.split('/')[1:]) 
    repo_hash = sh.git('rev-parse','--short','HEAD',_cwd=payload).rstrip()
    repo_url  = sh.git('config','--get','remote.origin.url',_cwd=payload ).rstrip()  # TODO: fix hardcoded directory

    INFO("Fetching production setup")
    setup = fetch_production_setup( name, buildarg, tag, repo_url, repo_dir, repo_hash )
    
    #
    # Returns the production status table from the database
    #
    if runMin>runMax:
        runMax=999999
        runMin=0

    INFO("Fetching production status")
    prod_status = fetch_production_status ( setup, runMin, runMax, update, sphenix_dstname(setup.name,setup.build,setup.dbtag))  # between run min and run max inclusive

    #
    # Map the production status table onto the output filename.  We use this map later on to determine whether
    # the proposed candidate output DST in the outputs list is currently being produced by a condor job, or
    # has failed and needs expert attention.
    #
    prod_status_map = {}
    INFO("Building production status map")    
    for stat in prod_status:
        # replace with sphenix_base_filename( setup.name, setup.build, setup.dbtag, stat.run, stat.segment )
        file_basename = sphenix_base_filename( setup.name, setup.build, setup.dbtag, stat.run, stat.segment )        # Not even sure how this was working???  This is the filename of the proposed job
        fbn = stat.dstfile
        prod_status_map[fbn] = stat.status  # supposed to be the map of the jobs which are in the production database to the filename of that job


    #
    # Build the list of matches.  We iterate over the fc_result zipped with the set of proposed outputs
    # which derives from it.  Keep a list of all runs we are about to submit.
    #
    list_of_runs = []
    INFO("Building matches")

    assert( len(fc_result)==len(outputs) ) 

    for (fc,dst) in zip(fc_result,outputs): # fcc.execute( rule.files ).fetchall():        

        lfn = fc.source
        run = fc.runnumber
        seg = fc.segment
        firstevent = getattr(fc,'firstevent',None)
        lastevent  = getattr(fc,'lastevent',None)
        runs_last_event = getattr(fc,'runs_last_event',None)
        if firstevent: firstevent=str(firstevent)
        if lastevent: lastevent=str(lastevent)
        if runs_last_event: runs_last_event=str(runs_last_event)

        neventsper = getattr(fc,'neventsper',None)
                
        #
        # Get the production status from the proposed output name
        #
        # TODO: Shouldn't we replace all suffixes here?
        #
        x    = dst.replace(".root","").strip()
        stat = prod_status_map.get( x, None )

        #
        # There is a master list of states which result in a DST producion job being blocked.  By default
        # this is (or ought to be) the total list of job states.  Jobs can end up failed, so there exist
        # options to ignore the a blocking state... which will remove it from the blocking list.
        #
        if stat in blocking:
            if args.batch==False:           WARN("%s is blocked by production status=%s, skipping."%( dst, stat ))
            continue
        
        #
        # Next we check to see if the job has alread been produced (i.e. it is registered w/in the file catalog).
        # If it exists, we will not reproduce the DST.  Unless the resubmit option overrides.
        #
        test=exists.get( dst, None )
        if test and not resubmit:
            if args.batch==False:           WARN("%s has already been produced, skipping."%dst)
            continue

        #
        # Check consistentcy between the LFN list (from input query) and PFN list (from file catalog query) 
        # for the current run.  Verify that the two lists are consistent.
        #
        num_lfn = len( lfn_lists[f"'{run}-{seg}'"] )
        num_pfn = len( pfn_lists[f"'{run}-{seg}'"] )
        sanity = True
        pfn_check = [ x.split('/')[-1] for x in pfn_lists[f"'{run}-{seg}'"] ]
        for x in pfn_check:
            if x not in lfn_lists[f"'{run}-{seg}'"]:
                sanity = False
                break

        # TODO: Add MD5 check

        #
        # If there are more LFNs requested than exist on disk, OR if the lfn list does
        # not match the pfn list, then reject.
        #
        if num_lfn > num_pfn or sanity==False:
            WARN(f"LFN list and PFN list are different.  Skipping this run {run} {seg}")
            WARN( f"{num_lfn} {num_pfn} {sanity}" )
            for i in itertools.zip_longest( lfn_lists[f"'{run}-{seg}'"], pfn_lists[f"'{run}-{seg}'"] ):
                print(i)
            continue


        inputs_ = pfn_lists[f"'{run}-{seg}'"]
        ranges_ = rng_lists[f"'{run}-{seg}'"]
        

        #
        # If the DST has been produced (and we make it to this point) we issue a warning that
        # it will be overwritten.
        #
        if test and resubmit:
            WARN("%s exists and will be overwritten"%dst)

        #
        #
        #
        if True:

            if verbose>10:
                INFO (lfn, run, seg, dst, "\n");

            myinputs = None
            myranges = None

            if inputs_:
                myinputs = ' '.join(inputs_) ### ??????

            # Direct lookup used in event builder jobs and implies we should obtain our
            # inputs from the database
            #
            #if inputs_ and rule.direct:
            #    myinputs = "dbinputs"
            #



            if ranges_:
                myranges = ' '.join(ranges_)

            
            # 
            # Build the rule-match data structure and immediately convert it to a dictionary.
            #    

            match = SPhnxMatch(
                name,                   # name of the DST, e.g. DST_CALO
                script,                 # script which will be run on the worker node
                lfn,                    # lfn of the input file
                dst,                    # name of the DST output file
                str(run),               # run number
                str(seg),               # segment number
                buildarg,               # sPHENIX software build (preserve the "." when building the match)
                tag,                    # database tag.
                "4096MB",               # default memory requirement
                "10GB",                 # default disk space requirement
                payload,                # payload directory
                inputs=myinputs,        # space-separated list of input files
                ranges=myranges,        # space-separated list of input files with first and last event separated by :
                firstevent=firstevent,
                lastevent=lastevent,
                runs_last_event=runs_last_event,
                neventsper=neventsper,
                )

            match = match.dict()

            #
            # Add / override with kwargs.  This is where (for instance) the memory and disk requirements
            # can be adjusted.
            #
            for k,v in kwargs.items():
                match[k]=str(v)              # coerce any ints to string
            
            result.append(match)

            if int(run) not in list_of_runs: list_of_runs.append(run)

            #
            # Terminate the loop if we exceed the maximum number of matches
            #
            if rule.limit and len(result)>= rule.limit:
                break    

    INFO(f"Matched {len(result)} jobs to the rule")

    return result, setup, list_of_runs

#__________________________________________________________________________________________________
#
arg_parser = argparse.ArgumentParser()    
arg_parser.add_argument( "--batch", default=False, action="store_true",help="Batch mode...")
arg_parser.add_argument( '-u', '--unblock-state', nargs='*', dest='unblock',  choices=["submitting","submitted","started","running","evicted","failed","finished"] )
arg_parser.add_argument( '-r', '--resubmit', dest='resubmit', default=False, action='store_true', 
                         help='Existing filecatalog entry does not block a job')

arg_parser.add_argument( "--dbinput", default=True, action="store_true",help="Passes input filelist through the production status db rather than the argument list of the production script." )
arg_parser.add_argument( "--no-dbinput", dest="dbinput", action="store_false",help="Unsets dbinput flag." )

arg_parser.add_argument( "--batch-name", dest="batch_name", default="$(name)_$(build)_$(tag)" )

def warn_options( args, userargs ):
    if args.dbinput==False:
        WARN("Option --no-dbinput is deprecated, and will be retired in the future.  All workflows should become db aware.")

def parse_command_line():
    global blocking
    global args
    global userargs

    args, userargs = arg_parser.parse_known_args()
    #blocking_ = ["submitting","submitted","started","running","evicted","failed","finished"]

    warn_options( args, userargs )

    if args.unblock:
        blocking = [ b for b in blocking if b not in args.unblock ]

    return args, userargs

        

        
        
