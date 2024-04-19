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

from slurptables import SPhnxProductionSetup
from slurptables import SPhnxProductionStatus
from slurptables import sphnx_production_status_table_def

from dataclasses import dataclass, asdict, field

from simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

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

statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
statusdbr = statusdbr_.cursor()

statusdbw_ = pyodbc.connect("DSN=ProductionStatusWrite")
statusdbw = statusdbw_.cursor()

fcro  = pyodbc.connect("DSN=FileCatalog;READONLY=True")
fccro = fcro.cursor()

daqdb = pyodbc.connect("DSN=daq;UID=phnxrc;READONLY=True");
daqc = daqdb.cursor()

cursors = { 
    'daq':daqc,
    'fc':fccro,
    'daqdb':daqc,
    'filecatalog': fccro,
    'status' : statusdbr
}

verbose=0

@dataclass
class SPhnxCondorJob:
    """
    Condor submission job template.
    """
    universe:              str = "vanilla"
    executable:            str = "$(script)"    
    arguments:             str = "$(nevents) $(run) $(seg) $(lfn) $(indir) $(dst) $(outdir) $(buildarg) $(tag) $(ClusterId) $(ProcId)"
    batch_name:            str = "$(name)_$(build)_$(tag)"
    output:                str = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).stdout"
    error:                 str = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).stderr"
    log:                   str = "$(condor)/$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).condor"
    periodichold: 	   str = "(NumJobStarts>=1 && JobStatus == 1)"
    priority:              str = "1958"
    job_lease_duration:    str = "3600"
    requirements:          str = '(CPU_Type == "mdc2")';    
    request_cpus:          str = "1"
    request_memory:        str = "$(mem)"
    should_transfer_files: str = "YES"
    output_destination:    str = "file://./output/"
    #output_destination:    str = "file:////sphenix/data/data02/sphnxpro/condorlog/$$($(run)/100)00"
    when_to_transfer_output: str = "ON_EXIT_OR_EVICT"
    request_disk:          str = None    
    initialdir:            str = None
    accounting_group:      str = None
    accounting_group_user: str = None
#   transfer_output_files: str = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).out,$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).err"
    transfer_output_files: str = None
    transfer_output_remaps: str = None
    
    transfer_input_files:  str = None
    user_job_wrapper:      str = None
    max_retries:           str = "0"  # default to no retries

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v }

    def __post_init__(self):
        pass

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



def fetch_production_status( setup, runmn=0, runmx=-1, update=True ):
    """
    Given a production setup, returns the production status table....
    """
    result = [] # of SPhnxProductionStatus

    name = "PRODUCTION_STATUS"
    
    if table_exists( name ):

        query = f"select * from {name} where prod_id={setup.id}"
        if ( runmn>runmx ): query = query + f" and run>={runmn};"
        else              : query = query + f" and run>={runmn} and run<={runmx};"

        dbresult = statusdbr.execute( query ).fetchall();

        # Transform the list of tuples from the db query to a list of prouction status dataclass objects
        result = [ SPhnxProductionStatus( *db ) for db in dbresult ]

    elif update==True: # note: we should never reach this state ...  tables ought to exist already

        create = sphnx_production_status_table_def( setup.name, setup.build, setup.dbtag )

        statusdbw.execute(create) # 
        statusdbw.commit()
        

    return result

#
# NAMING CONVENTION DEPENDENCE... will also need the iteration on this?
#
def getLatestId( tablename, dstname, run, seg ):  # limited to status db
    query=f"""
    select id from {tablename} where dstname='{dstname}' and run={run} and segment={seg} order by id desc limit 1;
    """
    result = statusdbw.execute(query).fetchone()[0]
    return result

def update_production_status( matching, setup, condor, state ):

    # 
    # NAMING CONVENTION DEPENDENCE...
    # 
    name = sphenix_dstname( setup.name, setup.build, setup.dbtag )

    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])

        #
        # NAMING CONVENTION DEPENDENCE
        #
        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag
        dstfile=dstname+'-%08i-%04i'%(run,segment)

        # 1s time resolution
        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        id_ = getLatestId( 'production_status', dstname, run, segment )


        #
        # NAMING CONVENTION DEPENDENCE
        #
        update=f"""
        update  production_status
        set     status='{state}',{state}='{timestamp}'
        where   dstname='{dstname}' and run={run} and segment={segment} and id={id_}
        """
        statusdbw.execute(update)
        statusdbw.commit()

def insert_production_status( matching, setup, condor, state ):

    condor_map = {}
    for ad in condor:
        clusterId = ad['ClusterId']
        procId    = ad['ProcId']
        out       = ad['Out'].split('/')[-1]   # discard anything that looks like a filepath
        args      = ad['Args']

        #
        # NAMING CONVENTION DEPENDENCE
        #
        key       = out.split('.')[0].lower()  # lowercase b/c referenced by file basename

        condor_map[key]= { 'ClusterId':clusterId, 'ProcId':procId, 'Out':out, 'Args':args }


# select * from status_dst_calor_auau23_ana387_2023p003;
# id | run | segment | nsegments | inputs | prod_id | cluster | process | status | flags | exit_code 
#----+-----+---------+-----------+--------+---------+---------+---------+--------+-------+-----------

    # replace with sphenix_dstname( setup.name, setup.build, setup.dbtag )
    name = sphenix_dstname( setup.name, setup.build, setup.dbtag )

    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])
        dstfileinput = m['lfn'].split('.')[0]

        #
        # NAMING CONVENTION DEPENDENCE
        #
        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag

        #
        # NAMING CONVENTION DEPENDENCE
        #
        dstfile=dstname+'-%08i-%04i'%(run,segment)
        
        prod_id = setup.id
        try:
            cluster = condor_map[ key.lower() ][ 'ClusterId' ]
            process = condor_map[ key.lower() ][ 'ProcId'    ]
        except KeyError:
            ERROR("Key Error getting cluster and/or process number from the class ads map.")
            ERROR(f"  key={key}")
            pprint.pprint( condor_map )
            ERROR("Assuming this is an issue with condor, setting cluster=0, process=0 and trying to continue...")
            cluster = 0
            process = 0

        status  = state        

        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        # TODO: Handle conflict

        insert=f"""
        insert into production_status
               (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents )
        values ('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'{dstfileinput}',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0 )
        """

        statusdbw.execute(insert)
        statusdbw.commit()

        


def submit( rule, **kwargs ):

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

    if len(matching)==0:
        WARN("No input files match the specifed rule.")
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


    jobd = rule.job.dict()


    for outname in [ 'outdir', 'logdir', 'condor']:

        outdir=kwargs.get(outname,None)
        if outdir==None: continue
        outdir = outdir.replace('file:/','')
        outdir = outdir.replace('//','/')

        outdir = outdir.replace( '$(rungroup)', '{rungroup}')
        outdir = f'f"{outdir}"'
        for run in runlist:
            mnrun = 100 * ( math.floor(run/100) )
            mxrun = mnrun+100
            rungroup=f'{mnrun:08d}_{mxrun:08d}'
            pathlib.Path( eval(outdir) ).mkdir( parents=True, exist_ok=True )            

    #pprint.pprint(jobd)
        # This is calling for a regex... 
#$$        outdir = outdir.replace( "$$([", "{math.floorOPAR" ) # start of condor expr becomes start of python format expression
#$$$       outdir = outdir.replace( "])",   "CPAR:06d}" ) # end of condor expr ...
#$$        outdir = outdir.replace( "])",   "CPAR}" ) # end of condor expr ...
#$$        outdir = outdir.replace( "$(", "" )    # condor macro "run" becomes local variable run.... hork.
#$$        outdir = outdir.replace( ")",  "" )
#$$        outdir = outdir.replace( "OPAR", "(" )
#$$        outdir = outdir.replace( "CPAR", ")" )

#$$        outdir = f'f"{outdir}"'
#$$        for run in runlist:
#$$            pathlib.Path( eval(outdir) ).mkdir( parents=True, exist_ok=True )            
    
    submit_job = htcondor.Submit( jobd )

    if verbose>0:
        INFO(submit_job)
        for m in matching:
            pprint.pprint(m)

    if dump==False:
        if verbose==-10:
            INFO(submit_job)
        
        schedd = htcondor.Schedd()    

        # Strip out unused $(...) condor macros
        mymatching = []
        for m in iter(matching):
            d = {}
            # massage the inputs from space to comma separated
            if m.get('inputs',None): 
                m['inputs']= ','.join( m['inputs'].split() )
            if m.get('ranges',None):
                m['ranges']= ','.join( m['ranges'].split() )
            for k,v in m.items():
                if k in str(submit_job):
                    d[k] = v
            mymatching.append(d)        


        
        
        run_submit_loop=30
        schedd_query = None
        for run_submit_loop in [120,180,300,600]:
            try:
                submit_result = schedd.submit(submit_job, itemdata=iter(mymatching))  # submit one job for each item in the itemdata

                schedd_query = schedd.query(
                    constraint=f"ClusterId == {submit_result.cluster()}",
                    projection=["ClusterId", "ProcId", "Out", "Args" ]
                )
                break # success... break past the else clause
            
            except htcondor.HTCondorIOError:

                WARN(f"Could not submit jobs to condor.  Retry in {run_submit_loop} seconds")
                time.sleep( run_submit_loop )

        else:
            # Executes after final iteration
            ERROR(f"ERROR: could not submit jobs to condor after several retries")
            
            
 
        # Update DB IFF we have a valid submission
        if ( schedd_query ):

            insert_production_status( matching, setup, schedd_query, state="submitting" ) 

            result = submit_result.cluster()

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

    return result                

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
    
    array = list( statusdbw.execute( query ).fetchall() )
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
    result = "%s-%08i-%04i" %( sphenix_dstname(dsttype, build, dbtag), int(run), int(segment) )
    return result

def sphenix_base_filename_( name, build, dbtag, fcres, fccols ):
    result = f"{name}_{build}_{dbtag}-{unique_job_key(fcres,fccols)}"
    return result


def unique_job_key( f, cols ):
    key=""
    if "runnumber" in cols:        key+=f"{f.runnumber:08d}"
    if "lastrun"   in cols:        key+=f"-{f.lastrun:08d}"
    if "segment"   in cols:        key+=f"-{f.segment:04d}"
    if "iteration" in cols:        key+=f"-{f.iteration:02d}"
    return key
    

def matches( rule, kwargs={} ):
    """
    
    Apply rule... extract files from DB according to the specified query
    and build the matches.  Return list of matches.  Return the production
    setup from the DB.
    
    """
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
    fc_columns = []
    fc_map     = None


    rl_result = None
    rl_map    = None

    lfn_lists  = {}  # LFN lists per run requested in the input query
    pfn_lists  = {}  # PFN lists per run existing on disk
    rng_lists  = {}  # LFN:firstevent:lastevent

    if rule.files:
        curs       = cursors[ rule.filesdb ]
        fc_result  = list( curs.execute( rule.files ).fetchall() )
        fc_columns = [ c[0] for c in curs.description ] 
        for f in fc_result:
            run     = f.runnumber
            segment = f.segment
            key     = unique_job_key(f,fc_columns)
            if lfn_lists.get(key,None) == None:   # NOTE:  Possible (unlikely) breaking change
                lfn_lists[ key ] = f.files.split()
                rng_lists[ key ] = getattr( f, 'fileranges', '' ).split()
            else:
                # If we hit this result, then the db query has resulted in two rows with identical
                # run numbers.  Violating the implicit submission schema.
                ERROR(f"Run number reached twice in this query...")
                ERROR(f"{key}")
                ERROR(rule.files)
                exit(1)
            
        # TODO: This is probably defunct... remove it?
        fc_map = { f.runnumber : f for f in fc_result }


    # Build lists of PFNs available for each run
    for ujobid,lfns in lfn_lists.items():

        lfns_ = [ f"'{lfn}'" for lfn in lfns ]
        list_of_lfns = ','.join(lfns_)

        # Add a new entry in the pfn_lists lookup table
        if pfn_lists.get(ujobid,None)==None:
            pfn_lists[ujobid]=[]

        # Build list of PFNs via direct lookup and append the results
        if rule.direct:
            for direct in glob(rule.direct):
                for p in [ direct+'/'+f for f in lfns if os.path.isfile(os.path.join(direct, f)) ]:
                    pfn_lists[ ujobid ].append( p )
            

        # Build list of PFNs via filecatalog lookup if direct path has not been specified
        if rule.direct==None:
            pfnquery=f"""
            select full_file_path,md5 from files where lfn in ( {list_of_lfns} );
            """        
            for pfnresult in fccro.execute( pfnquery ):
                pfn_lists[ ujobid ].append( pfnresult.full_file_path )




    #
    # Build the list of output files for the transformation 
    #
#   outputs = [
#       f"{name}_{build}_{tag}-{unique_job_key(fcres,fc_columns)}.root" for fcres in fc_result
#   ]
    outputs = [
        f"{sphenix_base_filename_(name,build,tag,fcres,fc_columns)}.root" for fcres in fc_result
    ]


    #
    # Build dictionary of DSTs existing in the datasets table of the file catalog.  For every DST that is in this list,
    # we know that we do not have to produce it if it appears w/in the outputs list.
    #
    # TODO: This is potentially a big, long query.  Limit query to the existing set of proposed output files or the 
    # list of runs...
    dsttype="%s_%s_%s"%(name,build,tag)  # dsttype aka name above

    exists = {}
    for check in fccro.execute("select filename,runnumber,segment from datasets where filename like '"+dsttype+"%';"):
        # pprint.pprint(check)        
        # Exists is a map between the filename and an arbitrary tuple
        exists[ check.filename ] = True # ( check.runnumber, check.segment)  # key=filename, value=(run,seg)


    # 
    # The production setup will be unique based on (1) the specified analysis build, (2) the specified DB tag,
    # and (3) the hash of the local github repository where the payload scripts/macros are found.
    #
    repo_dir  = payload #'/'.join(payload.split('/')[1:]) 
    repo_hash = sh.git('rev-parse','--short','HEAD',_cwd=payload).rstrip()
    repo_url  = sh.git('config','--get','remote.origin.url',_cwd=payload ).rstrip()  # TODO: fix hardcoded directory

    setup = fetch_production_setup( name, buildarg, tag, repo_url, repo_dir, repo_hash )
    
    #
    # Returns the production status table from the database
    #
    prod_status = fetch_production_status ( setup, 0, -1, update )  # between run min and run max inclusive


    #
    # Map the production status table onto the output filename.  We use this map later on to determine whether
    # the proposed candidate output DST in the outputs list is currently being produced by a condor job, or
    # has failed and needs expert attention.
    #
    prod_status_map = {}
    for stat in prod_status:
        # replace with sphenix_base_filename( setup.name, setup.build, setup.dbtag, stat.run, stat.segment )
        file_basename = sphenix_base_filename( setup.name, setup.build, setup.dbtag, stat.run, stat.segment )        
        prod_status_map[file_basename] = stat.status


    #
    # Build the list of matches.  We iterate over the fc_result zipped with the set of proposed outputs
    # which derives from it.  Keep a list of all runs we are about to submit.
    #
    list_of_runs = []
    for (f,dst) in zip(fc_result,outputs): # fcc.execute( rule.files ).fetchall():        
        (lfn,run,seg,*fc_rest) = f

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
        key = unique_job_key( f, fc_columns )
        num_lfn = len( lfn_lists[key] )
        num_pfn = len( pfn_lists[key] )
        sanity = True
        pfn_check = [ x.split('/')[-1] for x in pfn_lists[key] ]
        for x in pfn_check:
            if x not in lfn_lists[key]:
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
            for i in itertools.zip_longest( lfn_lists[key], pfn_lists[key] ):
                print(i)
            continue

        inputs_ = pfn_lists[key]
        ranges_ = rng_lists[key]
        
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

            myinputs = None
            myranges = None
            if inputs_:
                myinputs = ' '.join(inputs_) ### ??????

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

    return result, setup, list_of_runs

#__________________________________________________________________________________________________
#
arg_parser = argparse.ArgumentParser()    
arg_parser.add_argument( "--batch", default=False, action="store_true",help="Batch mode...")
arg_parser.add_argument( '-u', '--unblock-state', nargs='*', dest='unblock',  choices=["submitting","submitted","started","running","evicted","failed","finished"] )
arg_parser.add_argument( '-r', '--resubmit', dest='resubmit', default=False, action='store_true', 
                         help='Existing filecatalog entry does not block a job')

def parse_command_line():
    global blocking
    global args
    global userargs

    args, userargs = arg_parser.parse_known_args()

    if args.unblock:
        blocking = [ b for b in blocking if b not in args.unblock ]

    return args, userargs

        

        
        
