#!/usr/bin/env python

import slurp

from slurp import SPhnxRule  as Rule
from slurp import SPhnxMatch as Match
from slurp import SPhnxCondorJob as Job
from slurp import matches
from slurp import submit

from slurp import arg_parser

import pprint

# Extend the command line arguments
arg_parser.add_argument( '-n', '--nevents', default=0, dest='nevents', help='Number of events to process.  0=all.', type=int)
arg_parser.add_argument( '--rule', help="Submit against specified rule", default="DST_CALOR" )

# parse command line options
args = slurp.parse_command_line()

indir  = "/sphenix/lustre01/sphnxpro/commissioning/aligned_2Gprdf/"
outdir = "/sphenix/lustre01/sphnxpro/slurp/$$([$(run)/100])00"
logdir = "file:///sphenix/data/data02/sphnxpro/condorlogs/$$([$(run)/100])00"
condor = logdir.replace("file://","") 

if args.rule == "DST_CALOR":

    DST_CALOR_query = """
    select filename,runnumber,segment from datasets
    where dsttype = 'beam'
    and filename like 'beam-%'
    and runnumber > 0
    and runnumber = 22026
    order by runnumber,segment
    """

    job=Job(
        executable            = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/run_caloreco.sh",
        output_destination    = logdir,
        transfer_output_files = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).out,$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).err",
        transfer_input_files  = "$(payload),cups.py"
    )


    # DST_CALOR rule
    DST_CALOR_rule = Rule( name              = "DST_CALOR_auau23",
                           files             = DST_CALOR_query,
                           script            = "run_caloreco.sh",
                           build             = "ana.387",        
                           tag               = "2023p003",
                           payload           = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/",
                           job               = job)

    submit(DST_CALOR_rule, nevents=args.nevents, indir=indir, outdir=outdir, dump=False, resubmit=True, condor=condor ) 

