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
import time

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
fc = pyodbc.connect("DSN=FileCatalog")
fcc = fc.cursor()

fcro  = pyodbc.connect("DSN=FileCatalog;READONLY=True")
fccro = fc.cursor()

daqdb = pyodbc.connect("DSN=daq;UID=phnxrc;SERVER=sphnxdaqdbreplica.sdcc.bnl.gov;READONLY=True");
daqc = daqdb.cursor()

cursors = { 
    'daq':daqc,
    'fc':fccro,
    'daqdb':daqc,
    'filecatalog': fccro
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

    

    def __eq__( self, that ):
        return self.run==that.run and self.seg==that.seg

    def __post_init__(self):

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)                
        run = int(self.run)

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
    if fccro.tables( table=tablename.lower(), tableType='TABLE' ).fetchone():
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

        dbresult = fccro.execute( query ).fetchall();

        # Transform the list of tuples from the db query to a list of prouction status dataclass objects
        result = [ SPhnxProductionStatus( *db ) for db in dbresult ]

    elif update==True:

        create = sphnx_production_status_table_def( setup.name, setup.build, setup.dbtag )

        fccro.execute(create) # 
        fccro.commit()
        

    return result

def getLatestId( tablename, dstname, run, seg ):
    query=f"""
    select id from {tablename} where dstname='{dstname}' and run={run} and segment={seg} order by id desc limit 1;
    """
    result = fccro.execute(query).fetchone()[0]
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

        id_ = getLatestId( 'production_status', dstname, run, segment )

        update=f"""
        update  production_status
        set     status='{state}',{state}='{timestamp}'
        where   dstname='{dstname}' and run={run} and segment={segment} and id={id_}
        """
        fcc.execute(update)
        fcc.commit()

def insert_production_status( matching, setup, condor, state ):

    condor_map = {}
    for ad in condor:
        clusterId = ad['ClusterId']
        procId    = ad['ProcId']
        out       = ad['Out']
        args      = ad['Args']
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

        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment )
        
        dsttype=setup.name
        dstname=setup.name+'_'+setup.build.replace(".","")+'_'+setup.dbtag
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

        # Consider deleting the entry here...

        insert=f"""
        insert into production_status
               (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents )
        values ('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'{dstfileinput}',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0 )
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
        WARN("No input files match the specifed rule.")
        return result

    # Build "list" of paths which need to be created before submitting
    #$$$mkpaths = {}
    #$$$for m in matching:        
    #$$$    mkpaths[ m["condor"] ] = 1;
    #$$$    mkpaths[ m["stdout"] ] = 2;
    #$$$    mkpaths[ m["stderr"] ] = 3;

    #$$$for (p,v) in mkpaths.items():
    #$$$    if not os.path.exists(p): os.makedirs( p )

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
            if m.get('inputs',None): m['inputs']= ','.join( m['inputs'].split() )
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
    
    array = list( fccro.execute( query ).fetchall() )
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
    update    = kwargs.get('update',    True ) # update the DB

    outputs = []

    # Build list of possible outputs from filelist query... (requires run,sequence as 2nd and 3rd
    # elements in the query result)
    fc_result = []
    fc_map    = None

    rl_result = None
    rl_map    = None

    if rule.files:
        curs      = cursors[ rule.filesdb ]
        fc_result = list( curs.execute( rule.files ).fetchall() )
        fc_map = { f.runnumber : f for f in fc_result }
        for fc in fc_result:
            print( fc )

    if rule.runlist:
        rl_result = list( daqc.execute( rule.runlist ).fetchall() )
        rl_map = { r.runnumber : r for r in rl_result }

    runlistfromfc = """
    select 
            'filecatalog/files' as source,
            runnumber,
            segment,
            string_agg( distinct lfn ) as files
    from files
    where ...
    """

    #
    # Build the list of output files for the transformation from the run and segment number in the filecatalog query.
    #
    outputs = [ "%s_%s_%s-%08i-%04i.root"%(name,build,tag,int(x[1]),int(x[2])) for x in fc_result ]

    #
    # Build dictionary of DSTs existing in the datasets table of the file catalog.  For every DST that is in this list,
    # we know that we do not have to produce it if it appears w/in the outputs list.
    #
    # TODO: This is potentially a big, long query.  Limit query to the existing set of proposed output files or the 
    # list of runs...
    dsttype="%s_%s_%s"%(name,build,tag)  # dsttype aka name above
    exists = {}
    for check in fcc.execute("select filename,runnumber,segment from datasets where filename like '"+dsttype+"%';"):
        exists[ check.filename ] = ( check.runnumber, check.segment)  # key=filename, value=(run,seg)


    # 
    # The production setup will be unique based on (1) the specified analysis build, (2) the specified DB tag,
    # and (3) the hash of the local github repository where the payload scripts/macros are found.
    #
    repo_dir  = payload #'/'.join(payload.split('/')[1:]) 
    repo_hash = sh.git('rev-parse','--short','HEAD',_cwd=payload).rstrip()
    repo_url  = sh.git('config','--get','remote.origin.url',_cwd="MDC2/submit/rawdata/caloreco/rundir/").rstrip()  # TODO: fix hardcoded directory

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
    # which derives from it.
    #
    for ((lfn,run,seg,*fc_rest),dst) in zip(fc_result,outputs): # fcc.execute( rule.files ).fetchall():
        
        #
        # Get the production status from the proposed output name
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
        # Runlist query from the daq was specified.  This requires that all files transferred to SDCC
        # show up in the datasets catalog.  
        #
        # Consistecy between the input query (filecatalog) and the runlist query ( daqdb ) is required.
        # If the daqdb indicates that the set of files have been transferred, but one or more files 
        # is missing in the filecatalog, we skip submission of this DST production.  If the filecatalog
        # has a file in it that the daqdb is not aware of... we issue a warning.  
        #
        # A filelist is built from the resulting physical file locations and provided to condor via the
        # $(inputs) variable, which may be passed into the payload script.
        #
        inputs_ = None
        if fc_map and rl_map:
            (fdum, frun, fseg, ffiles, *frest) = fc_map[run]
            (rdum, rrun, rseg, rfiles, *rrest) = rl_map[run]
            ffiles=ffiles.split()
            rfiles=rfiles.split()
            test =  set(ffiles) ^ set(rfiles)
            skip = False
            # Loop over the difference between the sets of files
            for f in test:
                if f in rfiles: 
                    if args.batch==False: INFO (f"{f} has been transferred to SDCC but is not in the filecatalog, skipping")
                    skip = True
                if f in ffiles: 
                    INFO (f"{f} in filecatalog missing in the daq filelist.") # accepting for now but will reject in production
            if skip: 
                continue

            lfns = ffiles # list of LFNs from the filecatalog            
            lfnpar = ','.join( '?' * len(lfns) )

            #
            # We have a list of input LFNs that we now transform into their corresponding physical file locations.
            # The resulting inputs_ list is passed down to the job as the last argument of the user script.
            #
            query=f"""
            select full_file_path 
                   from files
            where
                   lfn in ( {lfnpar} )            
            """
            inputs_ = []
            for f in fccro.execute ( query, lfns ).fetchall():
                inputs_.append(f[0])            

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
            if inputs_:
                myinputs = ' '.join(inputs_)
            
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
                inputs=myinputs         # comma-separated list of input files
                )

            match = match.dict()

            #
            # Add / override with kwargs.  This is where (for instance) the memory and disk requirements
            # can be adjusted.
            #
            for k,v in kwargs.items():
                match[k]=str(v)              # coerce any ints to string
            
            result.append(match)

            #
            # Terminate the loop if we exceed the maximum number of matches
            #
            if rule.limit and len(result)>= rule.limit:
                break                                

    return result, setup

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
    #blocking_ = ["submitting","submitted","started","running","evicted","failed","finished"]

    if args.unblock:
        blocking = [ b for b in blocking if b not in args.unblock ]

    return args, userargs

        

        
        
