#!/usr/bin/env python
import pprint
import pytest

from slurp import SPhnxRule  as Rule
from slurp import SPhnxMatch as Match
from slurp import SPhnxCondorJob as Job
from slurp import matches
from slurp import submit
import slurp

def main():
    
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
        limit 5
    """

    # note... can generalize this through a function... Query( "select ..." ), LocalFiles( [ ... ] ), etc...

    # note... should probably inject the physical filename at the point of query as well

    job=Job( executable="hello_world.sh",
             initialdir="MDC2/submit/rawdata/caloreco/rundir/",
             output_destination="file:///sphenix/u/jwebb2/work/2023/slurp/output/",
             transfer_output_files="hello_world.$(ClusterId).$(ProcId).log"
             )
    

    # DST_CALOR rule
    DST_CALOR_rule = Rule( name              = "DST_CALOR_auau23",
                           files             = DST_CALOR_query,
                           script            = "hello_world.sh",
                           build             = "ana.385",      
                           tag               = "2003p003",
                           job               = job)

    slurp.verbose=0


    result = submit (DST_CALOR_rule, nevents=0, indir=indir, outdir=outdir, dump=False )  # any condor macro can be overridden w/ a keyword argument to submit.

    print(result)

    



if __name__ == '__main__':
    main()
