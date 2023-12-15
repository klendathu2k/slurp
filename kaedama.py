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
arg_parser.add_argument( '--rule', help="Submit against specified rule", default="DST_EVENT" )
arg_parser.add_argument( '--limit', help="Maximum number of jobs to submit", default=0, type=int )
arg_parser.add_argument( '--submit',help="Job will be submitted", dest="submit", default="True", action="store_true")
arg_parser.add_argument( '--no-submit', help="Job will not be submitted... print things", dest="submit", action="store_false")

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
        transfer_input_files  = "$(payload),cups.py",
    )


    # DST_CALOR rule
    DST_CALOR_rule = Rule( name              = "DST_CALOR_auau23",
                           files             = DST_CALOR_query,
                           script            = "run_caloreco.sh",
                           build             = "ana.387",        
                           tag               = "2023p003",
                           payload           = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/",
                           job               = job,
                           limit             = args.limit
    )

    submit(DST_CALOR_rule, nevents=args.nevents, indir=indir, outdir=outdir, dump=False, resubmit=True, condor=condor ) 


elif args.rule == 'DST_EVENT':

    DST_EVENT_query = """
    select DISTINCT ON (runnumber) filename,runnumber,segment from datasets 
    where ( filename like 'beam_seb%' or filename like 'beam_East%' or filename like 'beam_West%' or filename like 'beam_LL1%' or filename like 'GL1_beam%') 
    and runnumber = 22026
    order by runnumber,segment
    """

    file_lists = ','.join("""
    eventcombine/lists/hcaleast_$(run).list
    eventcombine/lists/hcalwest_$(run).list
    eventcombine/lists/ll1_$(run).list
    eventcombine/lists/mbd_$(run).list
    eventcombine/lists/seb00_$(run).list
    eventcombine/lists/seb01_$(run).list
    eventcombine/lists/seb02_$(run).list
    eventcombine/lists/seb03_$(run).list
    eventcombine/lists/seb04_$(run).list
    eventcombine/lists/seb05_$(run).list
    eventcombine/lists/seb06_$(run).list
    eventcombine/lists/seb07_$(run).list
    eventcombine/lists/zdc_$(run).list""".replace(' ','').split('\n'))

    #cups_cmd = f"-d DST_EVENT_auau23 -r $(run) -s $(seg) execute $(script) $(nevents) $(name)_$(build)_$(tag) $(run) $(ClusterId) $(ProcId)"

    logbase = "$(name)_$(build)_$(tag)-$INT(run,%08d)-0000"
    outbase = "$(name)_$(build)_$(tag)"
    script_cmd = f"$(nevents) {outbase} {logbase} {outdir} $(run) $(ClusterId) $(ProcId) $(build) $(tag)"

    Xjob=Job(
        executable            = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/hello_world.sh",
        arguments             =  script_cmd,
        output_destination    = f"{logdir}",
        #transfer_output_files = f"{logbase}.out,{logbase}.err",
        #transfer_output_files = "hello_world.sh",
        transfer_input_files  =  "$(payload),cups.py,"+file_lists,
        output                = f'{logbase}.condor.stdout',
        error                 = f'{logbase}.condor.stderr',
        log                   = f'$(condor)/{logbase}.condor',
    )

    job=Job(
        executable            = "/sphenix/u/sphnxpro/slurp/eventcombine/run.csh",
        arguments             =  script_cmd,
        output_destination    = f"{logdir}",
        transfer_input_files  =  "$(payload),cups.py,"+file_lists,
        output                = f'{logbase}.condor.stdout',
        error                 = f'{logbase}.condor.stderr',
        log                   = f'$(condor)/{logbase}.condor',
    )

    if args.submit:
        DST_EVENT_rule = Rule( name   = "DST_EVENT_auau23",
                               files  = DST_EVENT_query,
                               script = "run.csh",
                               build  = "ana.393",
                               tag    = "2023p009",
                               payload = "/sphenix/u/sphnxpro/slurp/eventcombine/",
                               job    = job,
                               limit  = args.limit )

        submit(DST_EVENT_rule, nevents=args.nevents, indir=indir, outdir=outdir, dump=False, resubmit=True, condor=condor ) 

    else:
        pprint.pprint(job)
