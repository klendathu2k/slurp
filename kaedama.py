#!/usr/bin/env python

import slurp
import yaml

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
arg_parser.add_argument( '--config',help="Specifies the yaml configuration file")

def main():

    # parse command line options
    args = slurp.parse_command_line()

    config={}
    with open(args.config,"r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

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

    #_______________________________________________________________________________________________
    #___________________________________________________________________________________DST_CALOR___
    elif args.rule == "DST_CALOR":

        # Reduce configuration to this rule
        config = config[ args.rule ]

        # Input query specifies the source of the input files
        input_query= config['input_query'].format(**locals())
        params     = config['params']
        filesystem = config['filesystem']
        job_       = config['job']

        logbase = params['logbase']
        outbase = params['outbase']

        jobkw = {}
        for k,v in job_.items():
            jobkw[k] = v.format( **locals() )
        
        # And now we can create the job definition thusly
        job = Job( **jobkw )

        # DST_CALOR rule
        DST_CALOR_rule = Rule( name              = params['name'],
                               files             = input_query,
                               script            = params['script'],
                               build             = params['build'],
                               tag               = params['dbtag'],
                               payload           = params['payload'],
                               job               = job,
                               limit             = args.limit
        )

        submit (DST_CALOR_rule, 
                nevents=args.nevents, 
                indir=filesystem['indir'], 
                outdir=filesystem['outdir'], 
                dump=False, 
                resubmit=True, 
                condor=filesystem['condor'], 
                mem="2048MB", 
                disk="2GB" 
        ) 

    #_______________________________________________________________________________________________
    #___________________________________________________________________________________DST_EVENT___

    elif args.rule == 'DST_EVENT':

        # Get configuration for this rule
        config     = config['DST_EVENT']

        # Input query specifies the source of the input files
        input_query= config['input_query'].format(**locals())
        params     = config['params']
        filesystem = config['filesystem']
        job_       = config['job']

        file_lists = ','.join( params['file_lists'] )

        logbase = params['logbase']
        outbase = params['outbase']
        
        # Need to apply string formatting to all values in the job_ dictionary
        jobkw = {}
        for k,v in job_.items():
            jobkw[k] = v.format( **locals() )
        
        # And now we can create the job definition thusly
        job = Job( **jobkw )
        
        if args.submit:
            DST_EVENT_rule = Rule( name    = params['name']    ,
                                   files   = input_query       ,
                                   script  = params['script']  ,
                                   build   = params['build']   ,
                                   tag     = params['dbtag']   ,
                                   payload = params['payload'] ,
                                   job    = job                ,
                                   limit  = args.limit         
            )

            submit(DST_EVENT_rule, 
                   nevents = args.nevents, 
                   indir   = filesystem['indir'], 
                   outdir  = filesystem['outdir'], 
                   dump    = False, 
                   resubmit= True, 
                   condor  = filesystem['condor'] 
            ) 

        else:
            pprint.pprint(job)


if __name__ == '__main__': main()
