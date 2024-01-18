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
arg_parser.add_argument( '--runs', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=['26022'] )
arg_parser.add_argument( '--rules', nargs='+', default="['all']" )
arg_parser.add_argument( '--delay', help="Delay between loop executions",default=600)
arg_parser.add_argument( '--submit', help="Submit jobs to condor",default=True,action="store_true")
arg_parser.add_argument( '--no-submit', help="No submission, just print the summary information",action="store_false",dest="submit")

args = arg_parser.parse_args()



def main():
    first=True
    while (True):

        if args.submit:

            if 'all' in args.rules or 'DST_EVENT' in args.rules:
                print("Running the DST_EVENT rule")
                if len(args.runs)==1:
                    kaedama( runs=args.runs[0], rule="DST_EVENT", config="sphenix_auau23.yaml", batch=True, _out=sys.stdout )
                elif len(args.runs)==2:
                    kaedama( "--runs", args.runs[0], args.runs[1], rule="DST_EVENT", config="sphenix_auau23.yaml", batch=True, _out=sys.stdout )
                elif len(args.runs)>2:
                    for r in args.runs:
                        kaedama( "--runs", r, rule="DST_EVENT", config="sphenix_auau23.yaml", batch=True, _out=sys.stdout )

            if 'all' in args.rules or 'DST_CALOR' in args.rules:
                print("Running the DST_CALOR rule")
                if len(args.runs)==1:
                    kaedama( runs=args.runs[0], rule="DST_CALOR", config="sphenix_auau23.yaml", batch=True, _out=sys.stdout )
                elif len(args.runs)==2:
                    kaedama( "--runs", args.runs[0], args.runs[1], rule="DST_CALOR", config="sphenix_auau23.yaml", batch=True, _out=sys.stdout )
                elif len(args.runs)>2:
                    for r in args.runs:
                        kaedama( "--runs", r, rule="DST_CALOR", config="sphenix_auau23.yaml", batch=True, _out=sys.stdout )

                
        condor_q("-batch","sphnxpro",_out=sys.stdout)        


        print("Summary of jobs which have not reached staus='started'")
        print("------------------------------------------------------")
        psqlquery="""
        select dsttype,
               count(run)                        as num_jobs           ,
               avg(age(submitted,submitting))    as avg_time_to_submit ,
               min(age(submitted,submitting))    as min_time_to_submit ,
               max(age(submitted,submitting))    as max_time_to_submit
       
        from   production_status 
        where  status<='started' 
        group by dsttype
        order by dsttype desc
               ;
        """
        psql(dbname="FileCatalog",command=psqlquery,_out=sys.stdout)


        print("Summary of jobs which have reached staus='started'")
        print("--------------------------------------------------")
        psqlquery="""
        select dsttype,
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
        where  status>='started' 
        group by dsttype
        order by dsttype desc
               ;
        """
        psql(dbname="FileCatalog",command=psqlquery,_out=sys.stdout)


        psql(dbname="FileCatalog", 
             command="select dsttype,run,segment,cluster,process,status,nevents,started,running,ended,exit_code from production_status order by id;", _out=sys.stdout);

 


        #print("Pausing loop for 2min")
        sleep(int(args.delay))

if __name__ == '__main__':
    main()


