import pprint
import pytest

from slurp import SPhnxRule  as Rule
from slurp import SPhnxMatch as Match
from slurp import SPhnxCondorJob as Job
from slurp import matches
from slurp import submit
import slurp

import re

pytest.rules = []

def test_define_rules():
    r = Rule( name              = "DST_CALOR",
          files             = "select * from files where lfn like 'beam-%'",   # anything which matches the query
          script            = "ProdFlow/RHIC2023/CALOR/scripts/dst_calor.sh",  # will submit to condor via this script
          build             = "ana.385",                                       # run against this build
          tag               = "2003p003",                                      # and this database tag
          job               = Job()                                            # condor job description
          )

    pprint.pprint(r)
    
def Xtest_define_condor_job():
    job1 = Job()
    job2 = Job( executable="hello_world.sh",
                arguments="howdy sup how you doin? $(build)",
                output="hello.log",
                error="hello.err",
                log="hello.condor",
                request_cpus="1",
                request_memory="1024MB",
                request_disk="1024MB" )

                

    
def Xtest_simple_job_no_macros():
    job2 = Job( executable="hello_world.sh",
                arguments="howdy sup how you doin? $(build)",
                output="hello.log",
                error="hello.err",
                log="hello.condor",
                request_cpus="1",
                request_memory="1024MB",
                request_disk="1024MB" )

    DST_CALOR_query = """
    select filename,runnumber,segment from datasets
        where dsttype = 'beam'
          and filename like 'beam-%'
          and runnumber > 0
        order by runnumber,segment
        limit 1
    """

    DST_CALOR_rule = Rule( name              = "DST_CALOR_auau23",
                           files             = DST_CALOR_query,
                           script            = "hello_world.sh",
                           build             = "ana.385", 
                           tag               = "2003p003",
                           job               = job2)
    slurp.verbose=11
    submit (DST_CALOR_rule, nevents="0" )
    
    
def Xtest_apply_rules_and_submit():

    indir ="/sphenix/lustre01/sphnxpro/commissioning/aligned_2Gprdf/"
    #outdir="/sphenix/lustre01/sphnxpro/slurp/"
    outdir="/dev/null/"

    # Query to apply to the DST_CALOR rule.
    # filename, runnumber and segment need to be returned.  (could parse out of lfn if need be)    
    DST_CALOR_query = """
    select filename,runnumber,segment from datasets
        where dsttype = 'beam'
          and filename like 'beam-%'
          and runnumber > 21199
        order by runnumber,segment
        limit 20
    """

    # note... can generalize this through a function... Query( "select ..." ), LocalFiles( [ ... ] ), etc...

    # note... should probably inject the physical filename at the point of query as well

    job=Job( executable="hello_world.sh" )
    

    # DST_CALOR rule
    DST_CALOR_rule = Rule( name              = "DST_CALOR_auau23",
                           files             = DST_CALOR_query,
                           script            = "hello_world.sh",
                           build             = "ana385",         # intentional typo
                           tag               = "2003p003",
                           job               = job)

    slurp.verbose=111
    #submit (DST_CALOR_rule, nevents=0, build="ana385",indir=indir, outdir=outdir  )  # any condor macro can be overridden w/ a keyword argument to submit.
    submit (DST_CALOR_rule, nevents=0, build="ana385",indir=indir, outdir=outdir, dump=False )  # any condor macro can be overridden w/ a keyword argument to submit.    
