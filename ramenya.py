#!/usr/bin/env python
import sh
from time import sleep
from contextlib import redirect_stdout
import sys
import argparse


kaedama  = sh.Command("kaedama.py")
condor_q = sh.Command("condor_q")
psql     = sh.Command("psql")

arg_parser = argparse.ArgumentParser()    
arg_parser.add_argument( '--runs', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[] )
arg_parser.add_argument( '--rules', nargs='+', default="['all']" )
arg_parser.add_argument( '--delay', help="Delay between loop executions",default=600)
arg_parser.add_argument( '--submit', help="Submit jobs to condor",default=True,action="store_true")
arg_parser.add_argument( '--no-submit', help="No submission, just print the summary information",action="store_false",dest="submit")
arg_parser.add_argument( '--outputs',help="Information printed at each loop",nargs='+', default=['started'] )
arg_parser.add_argument( '--once',help="Break out of the loop after one iteration",default=False,action="store_true")
arg_parser.add_argument( '--config',help="Specifies the configuration file used by kaedama to submit workflows", default='sphenix_auau23.yaml')

args = arg_parser.parse_args()

def makeRunCondition( runs ):
    result = ""
    if len(runs)==1 and int(runs[0])>0:
        result = f" and run={runs[0]} ";
    if len(runs)==2:
        result = f" and run>={runs[0]} and run<={runs[1]}";
    if len(runs)==3:
        result = f" and runnumber in ({','.join(runs)})"
    return result

def main():

    while (True):

        if args.submit:

            if 'all' in args.rules or 'DST_EVENT' in args.rules:
                print("Running the DST_EVENT rule")
                if len(args.runs)==1:
                    kaedama( runs=args.runs[0], rule="DST_EVENT", config=args.config, batch=True, _out=sys.stdout )
                elif len(args.runs)==2:
                    kaedama( "--runs", args.runs[0], args.runs[1], rule="DST_EVENT", config=args.config, batch=True, _out=sys.stdout )
                elif len(args.runs)>2:
                    for r in args.runs:
                        kaedama( "--runs", r, rule="DST_EVENT", config=args.config, batch=True, _out=sys.stdout )

            if 'all' in args.rules or 'DST_CALOR' in args.rules:
                print("Running the DST_CALOR rule")
                if len(args.runs)==1:
                    kaedama( runs=args.runs[0], rule="DST_CALOR", config=args.config, batch=True, _out=sys.stdout )
                elif len(args.runs)==2:
                    kaedama( "--runs", args.runs[0], args.runs[1], rule="DST_CALOR", config=args.config, batch=True, _out=sys.stdout )
                elif len(args.runs)>2:
                    for r in args.runs:
                        kaedama( "--runs", r, rule="DST_CALOR", config=args.config, batch=True, _out=sys.stdout )


        # Build conditions for output queries
        conditions = ""
        conditions += makeRunCondition( args.runs )


        if 'condorq' in args.outputs:
            condor_q("-batch","sphnxpro",_out=sys.stdout)        


        if 'pending' in args.outputs:
            print("Summary of jobs which have not reached staus='started'")
            print("------------------------------------------------------")
            psqlquery=f"""
            select dsttype,
               count(run)                        as num_jobs           ,
               avg(age(submitted,submitting))    as avg_time_to_submit ,
               min(age(submitted,submitting))    as min_time_to_submit ,
               max(age(submitted,submitting))    as max_time_to_submit
       
            from   production_status 
            where  status<='started'  {conditions}
            group by dsttype
            order by dsttype desc
            ;
            """
            psql(dbname="FileCatalog",command=psqlquery,_out=sys.stdout)

        if 'started' in args.outputs:
            print("Summary of jobs which have reached staus='started'")
            print("--------------------------------------------------")
            psqlquery=f"""
            select dsttype,
               run,
               count(run)                      as num_jobs,
               avg(age(started,submitting))    as avg_time_to_start,
               count( case status when 'submitted' then 1 else null end )
                                               as num_submitted,
               count( case status when 'running' then 1 else null end )
                                               as num_running,
               count( case status when 'finished' then 1 else null end )
                                               as num_finished,
               count( case status when 'failed' then 1 else null end )
                                               as num_failed,
               avg(age(ended,started))         as avg_job_duration,
               min(age(ended,started))         as min_job_duration,
               max(age(ended,started))         as max_job_duration,
               sum(nevents)                    as sum_events
       
            from   production_status 
            where  status>='started' {conditions}
            group by dsttype,run
            order by dsttype,run desc
               ;
            """
            psql(dbname="FileCatalog",command=psqlquery,_out=sys.stdout)

        if 'clusters' in args.outputs:
            print("Summary of jobs which have reached staus='started'")
            print("--------------------------------------------------")
            psqlquery=f"""
            select dsttype,cluster,
               count(run)                      as num_jobs,
               avg(age(started,submitting))    as avg_time_to_start,
               count( case status when 'submitted' then 1 else null end )
                                               as num_submitted,
               count( case status when 'running' then 1 else null end )
                                               as num_running,
               count( case status when 'finished' then 1 else null end )
                                               as num_finished,
               count( case status when 'failed' then 1 else null end )
                                               as num_failed,
               avg(age(ended,started))         as avg_job_duration,
               min(age(ended,started))         as min_job_duration,
               max(age(ended,started))         as max_job_duration,
               sum(nevents)                    as sum_events
       
            from   production_status 
            where  status>='started'  {conditions}
            group by dsttype,cluster
            order by dsttype desc
               ;
            """
            psql(dbname="FileCatalog",command=psqlquery,_out=sys.stdout)


        if 'everything' in args.outputs:
            psql(dbname="FileCatalog", 
                 command="select dsttype,run,segment,cluster,process,status,nevents,started,running,ended,exit_code from production_status order by id;", _out=sys.stdout);
 
        if args.once:
            break

        sleep(int(args.delay))

if __name__ == '__main__':
    main()


