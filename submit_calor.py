#!/usr/bin/env python

import slurp

from slurp import SPhnxRule  as Rule
from slurp import SPhnxMatch as Match
from slurp import SPhnxCondorJob as Job
from slurp import matches
from slurp import submit

import pprint

# parse command line options
slurp.parse_command_line()

indir  = "/sphenix/lustre01/sphnxpro/commissioning/aligned_2Gprdf/"
#outdir = "/sphenix/lustre01/sphnxpro/slurp/"
outdir = "/sphenix/lustre01/sphnxpro/slurp/$$([$(run)/100])00"
logdir = "file:///sphenix/data/data02/sphnxpro/condorlogs/$$([$(run)/100])00"
condor = logdir.replace("file://","") 

DST_CALOR_query = """
select filename,runnumber,segment from datasets
   where dsttype = 'beam'
   and filename like 'beam-%'
   and runnumber = 22026
   order by runnumber,segment
   limit 10
"""

job=Job(
    executable            = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/run_caloreco.sh",
    output_destination    = logdir,
    transfer_output_files = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).out,$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).err",
    transfer_input_files  = "$(payload)"
    )


# DST_CALOR rule
DST_CALOR_rule = Rule( name              = "DST_CALOR_auau23",
                       files             = DST_CALOR_query,
                       script            = "run_caloreco.sh",
                       build             = "ana.387",        
                       tag               = "2023p003",
                       payload           = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/",
                       job               = job)

submit(DST_CALOR_rule, nevents=100, indir=indir, outdir=outdir, dump=False, resubmit=True, condor=condor ) 


#kw={ 'nevents':10, 'indir':indir, 'outdir':outdir, 'condor':condor, 'resubmit':True }
#matching = matches( DST_CALOR_rule, kw )
#for m in matching:
#    pprint.pprint(m)
