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
arg_parser.add_argument( '--runs', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=['26022'] )
arg_parser.add_argument( '--segments', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[] )

def main():

    # parse command line options
    args = slurp.parse_command_line()

    run_condition = ""
    if len(args.runs)==1:
        run_condition = f"and runnumber={args.runs[0]}"
    elif len(args.runs)==2:
        run_condition = f"and runnumber>={args.runs[0]} and runnumber<={args.runs[1]}"
    elif len(args.runs)==3:
        run_condition = "and runnumber in ( %s )" % ','.join( args.runs )

    seg_condition = ""
    if len(args.segments)==1:
        seg_condition = f"and segment={args.segments[0]}"
    elif len(args.segments)==2:
        seg_condition = f"and segment>={args.segments[0]} and segment<={args.segments[1]}"
    elif len(args.segments)==3:
        seg_condition = "and segment in ( %s )" % ','.join( args.segments )

    limit_condition=""
    if args.limit>0:
        limit_condition = f"limit {args.limit}"
        




    indir  = "/sphenix/lustre01/sphnxpro/commissioning/aligned_2Gprdf/"
    outdir = "/sphenix/lustre01/sphnxpro/slurp/$$([$(run)/100])00"
    logdir = "file:///sphenix/data/data02/sphnxpro/condorlogs/$$([$(run)/100])00"
    condor = logdir.replace("file://","") 

    if args.rule == 'INFO':
        print( "INDIR:   ", indir )
        print( "OUTDIR:  ", outdir )
        print( "LOGDIR:  ", logdir )

    elif args.rule == "DST_CALOR":

#                      filename                       | runnumber | segment |    size     | dataset |     dsttype     | events 
#-----------------------------------------------------+-----------+---------+-------------+---------+-----------------+--------
# DST_EVENT_auau23_ana393_2023p009-00022026-0008.prdf |     22026 |       8 | 20986167296 | mdc2    | ana393_2023p009 |      0


        DST_CALOR_query = f"""
        select filename,runnumber,segment from datasets where
        filename like 'DST_EVENT_auau23_ana393_2023p009-%'
        {run_condition} {seg_condition}
        order by runnumber,segment
        {limit_condition}
        """

        job=Job(
            executable            = "/sphenix/u/sphnxpro/slurp/MDC2/submit/rawdata/caloreco/rundir/run_caloreco.sh",
            output_destination    = logdir,
            transfer_output_files = "$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).out,$(name)_$(build)_$(tag)-$INT(run,%08d)-$INT(seg,%04d).err",
            transfer_input_files  = "$(payload),cups.py",
            accounting_group      = "group_sphenix.mdc2",
            accounting_group_user = "sphnxpro",
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

        submit (DST_CALOR_rule, nevents=args.nevents, indir=indir, outdir=outdir, dump=False, resubmit=True, condor=condor, mem="2048MB", disk="2GB" ) 


    elif args.rule == "DST_CALOR.old":

        DST_CALOR_query = f"""
        select filename,runnumber,segment from datasets
        where dsttype = 'beam'
        and filename like 'beam-%'
        {run_condition} {seg_condition}
        order by runnumber,segment
        {limit_condition}
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

        DST_EVENT_query = f"""
        select DISTINCT ON (runnumber) filename,runnumber,segment from datasets 
        where ( filename like 'beam_seb%' or filename like 'beam_East%' or filename like 'beam_West%' or filename like 'beam_LL1%' or filename like 'GL1_beam%') 
        {run_condition} {seg_condition}
        order by runnumber,segment
        {limit_condition}
        """

        print(DST_EVENT_query)

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

        job=Job(
            executable            = "/sphenix/u/sphnxpro/slurp/eventcombine/run.sh",
            user_job_wrapper      = "init.sh",
            arguments             =  script_cmd,
            output_destination    = f"{logdir}",
            transfer_input_files  =  "$(payload),cups.py,init.sh,pull.py,"+file_lists,
            output                = f'{logbase}.condor.stdout',
            error                 = f'{logbase}.condor.stderr',
            log                   = f'$(condor)/{logbase}.condor',
            accounting_group      = "group_sphenix.mdc2",
            accounting_group_user = "sphnxpro",
        )

        if args.submit:
            DST_EVENT_rule = Rule( name   = "DST_EVENT_auau23",
                                   files  = DST_EVENT_query,
                                   script = "run.sh",
                                   build  = "ana.393",
                                   tag    = "2023p009",
                                   payload = "/sphenix/u/sphnxpro/slurp/eventcombine/",
                                   job    = job,
                                   limit  = args.limit )

            submit(DST_EVENT_rule, nevents=args.nevents, indir=indir, outdir=outdir, dump=False, resubmit=True, condor=condor ) 

        else:
            pprint.pprint(job)


if __name__ == '__main__': main()
