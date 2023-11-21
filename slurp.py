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

from dataclasses import dataclass, asdict
from typing import Callable

verbose = 0

__frozen__ = True
__rules__  = []

# File Catalog (and file catalog cursor)
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
    arguments:             str = "$(nevents) $(run) $(seg) $(lfn) $(indir) $(dst) $(outdir) $(buildarg) $(tag)"
    batch_name:            str = "$(name)_$(build)_$(tag)"
    output:                str = "$(stdout)/$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).stdout"
    error:                 str = "$(stderr)/$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).stderr"
    log:                   str = "$(condor)/$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).condor"
    periodichold: 	       str = "(NumJobStarts>=1 && JobStatus == 1)"
    priority:              str = "53"
    job_lease_duration:    str = "3600"
    requirements:          str = '(CPU_Type == "mdc2")\n';    
    request_cpus:          str = "1"
    request_memory:        str = "$(mem)"
    request_disk:          str = None    
    initialdir:            str = None
    accounting_group:      str = None
    accounting_group_user: str = None

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v }

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

    def __eq__(self, that ):
        return self.name == that.name
    
    def __post_init__(self):
        # Verify the existence of the production script
        #    ... no guarentee that the script is actually at this default path ...
        #    ... it could be sitting in the intialdir of the job ...
        path_ = ""
        if self.job.initialdir:
            path_ = self.job.initialdir + "/"
        assert( pathlib.Path( path_ + self.script ).exists() )

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)        

        # Add to the global list of rules
        __rules__.append(self)

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v }        
        

@dataclass( frozen = __frozen__ )
class SPhnxMatch:
    name:   str = None;      # Name of the matching rule
    script: str = None;      # The run script
    lfn:    str = None;      # Logical filename that matches
    dst:    str = None;      # Transformed output
    run:    str = None;      # Run #
    seg:    str = None;      # Seg #
    build:  str = None;      # Build
    tag:    str = None;      # DB tag
    mem:    str = None;      # Required memory
    disk:   str = None;      # Required disk space
    stdout: str = None; 
    stderr: str = None; 
    condor: str = None;
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
        return { k: str(v) for k, v in asdict(self).items() if v }


def submit( rule, **kwargs ):

    # Will return cluster ID
    result = 0

    actions = [ "dump" ]

    dump = kwargs.get( "dump", False )
    if dump:
        del kwargs["dump"];

    # Build list of LFNs which match the input
    matching = matches( rule, kwargs )
    if len(matching)==0:
        print("Warning: no input files match the specifed rule.  Done.")
        return result

    if kwargs.get('resubmit',False):
        reply = None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Warning: resubmit option may overwrite previous production.  Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result

    job = rule.job.dict()

    submit_job = htcondor.Submit( rule.job.dict() )
    if verbose>0:
        print(submit_job)
        for m in matching:
            pprint.pprint(m)

    if dump==False:

        if verbose==-10:
            print(submit_job)
        
        schedd = htcondor.Schedd()        
        submit_result = schedd.submit(submit_job, itemdata=iter(matching))  # submit one job for each item in the itemdata


        schedd.query(
            constraint=f"ClusterId == {submit_result.cluster()}",
            projection=["ClusterId", "ProcId", "Out", "Args" ]
            )

        result = submit_result.cluster()
        

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

def matches( rule, kwargs={} ):
    """
    
    Apply rule... extract files from DB according to the specified query
    and build the matches.
    
    """
    result = []

    name      = kwargs.get('name',      rule.name)
    build     = kwargs.get('build',     rule.build)      # TODO... correct handling from submit.  build=ana.xyz --> build=anaxyz buildarg=ana.xyz
    buildarg  = kwargs.get('buildarg',  rule.buildarg)
    tag       = kwargs.get('tag',       rule.tag)
    script    = kwargs.get('script',    rule.script)
    resubmit  = kwargs.get('resubmit',  rule.resubmit)

    outputs = []

    # Retrieve from cache if we can, otherwise go to the DB
    fc_result = fc_cache.get( rule.files, None )
    if fc_result == None:
        fc_result = list( fcc.execute( rule.files ).fetchall() )
        fc_cache[ rule.files ] = fc_result
        outputs = [ "%s_%s_%s-%08i-%04i.root"%(name,build,tag,int(x[1]),int(x[2])) for x in fc_result ]

    # Build dictionary of existing dsts
    dsttype="%s_%s_%s"%(name,build,tag)
    fc_check = list( fcc.execute("select filename,runnumber,segment from datasets where dsttype like '"+dsttype+"';").fetchall() )
    exists = {}
    for check in fc_check:
        exists[ check[0] ] = ( check[1], check[2] )  # key=filename, value=(run,seg)
    

    # Query the sched for all running jobs.  
    schedd = htcondor.Schedd()
    query  = schedd.query( projection=["Out","ClusterId","ProcId"] )
    stdout = {}
    for q in query:
        x = os.path.basename( q['Out'] )
        stdout[x]=( q['ClusterId'], q['ProcId'] )

    
    for ((lfn,run,seg),dst) in zip(fc_result,outputs): # fcc.execute( rule.files ).fetchall():

        x = dst.replace(".root",".stdout").rstrip()
        test=stdout.get( dst.replace(".root",".stdout"), None )
        if test:
            print("Warning: %s is already being produced by %s.%s, skipping."%( dst, str(test[0]), (test[1]) ))
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
                build,
                tag,
                "4096MB",
                "10GB",
                stdout=".",
                stderr=".",
                condor="/tmp/slurp/rn%i"%(math.trunc(int(run)/100)*100)
                ).dict()

            # Add / override with kwargs
            for k,v in kwargs.items():
                match[k]=str(v)              # coerce any ints to string
            
            result.append(match)

    return result




        
        
