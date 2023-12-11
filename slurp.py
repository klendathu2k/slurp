#!/usr/bin/env python

import pyodbc
import htcondor
import classad
import re
import uuid
import os
import pathlib
import pprint
import math
import sh
import argparse
import datetime

from slurptables import SPhnxProductionSetup
from slurptables import SPhnxProductionStatus
from slurptables import sphnx_production_status_table_def

from dataclasses import dataclass, asdict, field

# List of states which block the job
blocking = ["submitting","submitted","started","running","evicted","failed","finished"]
args = None

verbose = 0

__frozen__ = True
__rules__  = []

# File Catalog (and file catalog cursor)
# TODO: exception handling... if we can't connect, retry at some randomized point in the future.
# ... and set a limit on the number of retries before we bomb out ...
fc = pyodbc.connect("DSN=FileCatalog")
fcc = fc.cursor()

# FileCatalog Cache
fc_cache = {}


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
    periodichold: 	       str = "(NumJobStarts>=1 && JobStatus == 1)"
    priority:              str = "53"
    job_lease_duration:    str = "3600"
    requirements:          str = '(CPU_Type == "mdc2")\n';    
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
    transfer_output_files: str = None
    transfer_input_files:  str = None

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v }

    def __post_init__(self):
        pass

@dataclass( frozen= __frozen__ )
class SPhnxRule:
    name:              str          # Name of the rule
    files:             str          # FileCatalog DB query
    script:            str          # Production script
    build:             str          # Build tag
    tag:               str          # Database tag
    job:               SPhnxCondorJob = SPhnxCondorJob()
    resubmit:          bool = False # Set true if job should overwrite existing job
    buildarg:          str  = ""    # The build tag passed as an argument (leaves the "." in place).
    payload:  str = "";      # Payload directory (condor transfers inputs from)
    #manifest: list[str] = None;      # List of files in the payload directory
    limit:    int = 0 # maximum number of matches to return 0=all

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

    

    def __eq__( self, that ):
        return self.run==that.run and self.seg==that.seg

    def __post_init__(self):

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)        
        
        run = int(self.run)
        sldir = "/tmp/slurp/%i"%( math.trunc(run/100)*100 )
        if self.condor == None: object.__setattr__(self, 'condor', sldir )
        sldir = "/sphenix/data/data02/sphnxpro/condorlogs/%i"%( math.trunc(run/100)*100 )            
        if self.stdout == None: object.__setattr__(self, 'stdout', sldir )
        if self.stderr == None: object.__setattr__(self, 'stderr', sldir )

#    def __post_init__(self):
#        if self.condor == None:
#            a = int(self.run)
#            self.condor = "/tmp/slurp/%i"%( math.trunc(a/100)*100 )

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }


def Xtable_exists( tablename ):
    """
    Returns true if the named table exists
    """    
    query = """
    select exists ( 
         select 1 from information_schema.tables where table_name='%s'
    );
    """%tablename

    result = bool( fcc.execute( query ).fetchone()[0] )
    fcc.execute( "select exists ( select 1 from information_schema.tables where table_name='production_setup' )" ).fetchone()

    return result

def table_exists( tablename ):
    """
    """ 
    result = False
    if fcc.tables( table=tablename.lower(), tableType='TABLE' ).fetchone():
        result = True
    return result



def fetch_production_status( setup, runmn=0, runmx=-1 ):
    """
    Given a production setup, returns the production status table....
    """
    result = [] # of SPhnxProductionStatus

    #name = "STATUS_%s"% sphenix_dstname( setup.name, setup.build, setup.dbtag )
    name = "PRODUCTION_STATUS"
    
    if table_exists( name ):

        query = "select * from %s"%name
        if ( runmn>runmx ): query = query + " where run>=%i;"            %(runmn)
        else              : query = query + " where run>=%i and run<=%i;"%(runmn,runmx)

        dbresult = fcc.execute( query ).fetchall();

        # Transform the list of tuples from the db query to a list of prouction status dataclass objects
        result = [ SPhnxProductionStatus( *db ) for db in dbresult ]

    else:

        create = sphnx_production_status_table_def( setup.name, setup.build, setup.dbtag )
        # replace with ???... down one level... sphenix_dstname( setup.name, setup.build, setup.dbtag )


        fcc.execute(create) # 
        fcc.commit()
        

    return result

def update_production_status( matching, setup, condor, state ):

    name = sphenix_dstname( setup.name, setup.build, setup.dbtag )

    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])

        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag
        dstfile=dstname+'-%08i-%04i'%(run,segment)

        # 1s time resolution
        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        update=f"""
        update  production_status
        set     status='{state}',{state}='{timestamp}'
        where   dstname='{dstname}' and run={run} and segment={segment}
        """
        fcc.execute(update)
        fcc.commit()

def insert_production_status( matching, setup, condor, state ):

    condor_map = {}
    for ad in condor:
        clusterId = ad['ClusterId']
        procId = ad['ProcId']
        out    = ad['Out']
        args   = ad['Args']
        key    = out.split('.')[0].lower()  # lowercase b/c referenced by file basename
        condor_map[key]= { 'ClusterId':clusterId, 'ProcId':procId, 'Out':out, 'Args':args }


# select * from status_dst_calor_auau23_ana387_2023p003;
# id | run | segment | nsegments | inputs | prod_id | cluster | process | status | flags | exit_code 
#----+-----+---------+-----------+--------+---------+---------+---------+--------+-------+-----------

    # replace with sphenix_dstname( setup.name, setup.build, setup.dbtag )
    name = sphenix_dstname( setup.name, setup.build, setup.dbtag )

    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])

        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag
        dstfile=dstname+'-%08i-%04i'%(run,segment)
        
        prod_id = setup.id
        cluster = condor_map[ key.lower() ][ 'ClusterId' ]
        process = condor_map[ key.lower() ][ 'ProcId'    ]
        status  = state        

        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        # Consider deleting the entry here...

        insert=f"""
        insert into production_status
               (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents )
        values ('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0 )
        """

        fcc.execute(insert)
        fcc.commit()

        


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
    matching, setup = matches( rule, kwargs )

    if len(matching)==0:
        print("Warning: no input files match the specifed rule.  Done.")
        return result

    # Build "list" of paths which need to be created before submitting
    mkpaths = {}
    for m in matching:        
        mkpaths[ m["condor"] ] = 1;
        mkpaths[ m["stdout"] ] = 2;
        mkpaths[ m["stderr"] ] = 3;

    for (p,v) in mkpaths.items():
        if not os.path.exists(p): os.makedirs( p )


    if kwargs.get('resubmit',False):
        reply = None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Warning: resubmit option may overwrite previous production.  Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result


    if not ( setup.is_clean and setup.is_current ):
        print("Warning: the macros/scripts directory is not at the same commit as its github repo and/or")
        print("         there are uncommitted local changes.")
        
        reply=None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result        

    jobd = rule.job.dict()

    submit_job = htcondor.Submit( jobd )
    if verbose>0:
        print(submit_job)
        for m in matching:
            pprint.pprint(m)

    if dump==False:

        if verbose==-10:
            print(submit_job)
        
        schedd = htcondor.Schedd()    

        submit_result = schedd.submit(submit_job, itemdata=iter(matching))  # submit one job for each item in the itemdata
 
        schedd_query = schedd.query(
            constraint=f"ClusterId == {submit_result.cluster()}",
            projection=["ClusterId", "ProcId", "Out", "Args" ]
        )

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
    
    array = list( fcc.execute( query ).fetchall() )
    assert( len(array)<2 )

    if   len(array)==0:
        insert="""
        insert into production_setup(name,build,dbtag,repo,dir,hash)
               values('%s','%s','%s','%s','%s','%s');
        """%(name,build,dbtag,repo,dir_,hash_)

        fcc.execute( insert )
        fcc.commit()

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

    outputs = []

    # Retrieve from cache if we can, otherwise go to the DB
    fc_result = fc_cache.get( rule.files, None )
    if fc_result == None:
        fc_result = list( fcc.execute( rule.files ).fetchall() )
        fc_cache[ rule.files ] = fc_result
        outputs = [ "%s_%s_%s-%08i-%04i.root"%(name,build,tag,int(x[1]),int(x[2])) for x in fc_result ]

    # Build dictionary of existing dsts
    dsttype="%s_%s_%s"%(name,build,tag)  # dsttype aka name above
    fc_check = list( fcc.execute("select filename,runnumber,segment from datasets where dsttype like '"+dsttype+"';").fetchall() )
    exists = {}
    for check in fc_check:
        exists[ check[0] ] = ( check[1], check[2] )  # key=filename, value=(run,seg)
    

    # Get the production setup for this submission
    repo_dir  = payload #'/'.join(payload.split('/')[1:]) 
    repo_hash = sh.git('rev-parse','--short','HEAD',_cwd=payload).rstrip()
    repo_url  = sh.git('config','--get','remote.origin.url',_cwd="MDC2/submit/rawdata/caloreco/rundir/").rstrip()

    setup = fetch_production_setup( name, buildarg, tag, repo_url, repo_dir, repo_hash )
    
    prod_status = fetch_production_status ( setup, 0, -1 )  # between run min and run max inclusive

    prod_status_map = {}
    for stat in prod_status:
        # replace with sphenix_base_filename( setup.name, setup.build, setup.dbtag, stat.run, stat.segment )
        file_basename = sphenix_base_filename( setup.name, setup.build, setup.dbtag, stat.run, stat.segment )        
        prod_status_map[file_basename] = stat.status

    # Build the list of matches    
    for ((lfn,run,seg),dst) in zip(fc_result,outputs): # fcc.execute( rule.files ).fetchall():
        
        x = dst.replace(".root","").strip()
        stat = prod_status_map.get( x, None )

        if stat in blocking:
           print("Warning: %s is blocked by production status=%s, skipping."%( dst, stat ))
           continue
        
        test=exists.get( dst, None )
        if test and not resubmit:
            print("Warning: %s has already been produced, skipping."%dst)
            continue

        if test and resubmit:
            print("Warning: %s exists and will be overwritten"%dst)

        if True:
            if verbose>10:
                print (lfn, run, seg, dst, "\n");

            match = SPhnxMatch(
                name,
                script,
                lfn,
                dst,
                str(run),
                str(seg),
                buildarg,   # preserve the "." when building the match
                tag,
                "4096MB",
                "10GB",
                payload
                )

            match = match.dict()

            # Add / override with kwargs
            for k,v in kwargs.items():
                match[k]=str(v)              # coerce any ints to string
            
            result.append(match)

            # Terminate the loop if we exceed the maximum number of matches
            if rule.limit and len(result)>= rule.limit:
                break


                
                

    return result, setup



#__________________________________________________________________________________________________
#
arg_parser = argparse.ArgumentParser()    
arg_parser.add_argument( '-u', '--unblock-state', nargs='*', dest='unblock',  choices=blocking )
arg_parser.add_argument( '-r', '--resubmit', dest='resubmit', default=False, action='store_true', 
                         help='Existing filecatalog entry does not block a job')

def parse_command_line():
    global blocking
    global args

    args = arg_parser.parse_args()

    if args.unblock:
        blocking = [ b for b in blocking if b not in args.unblock ]

    return args

        

        
        
