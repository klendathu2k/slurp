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
arg_parser.add_argument( '--delay', help="Delay between loop executions",default=600)
args = arg_parser.parse_args()



def main():
    first=True
    while (True):
        print("Running the DST_EVENT rule")

        if len(args.runs)==1:
            kaedama( runs=args.runs[0], rule="DST_EVENT", batch=True, _out=sys.stdout )
        elif len(args.runs)==2:
            kaedama( "--runs", args.runs[0], args.runs[1], rule="DST_EVENT", batch=True, _out=sys.stdout )
        elif len(args.runs)>2:
            for r in args.runs:
                kaedama( "--runs", r, rule="DST_EVENT", batch=True, _out=sys.stdout )


        print("Running the DST_CALOR rule")

        if len(args.runs)==1:
            kaedama( runs=args.runs[0], rule="DST_CALOR", batch=True, _out=sys.stdout )
        elif len(args.runs)==2:
            kaedama( "--runs", args.runs[0], args.runs[1], rule="DST_CALOR", batch=True, _out=sys.stdout )
        elif len(args.runs)>2:
            for r in args.runs:
                kaedama( "--runs", r, rule="DST_CALOR", batch=True, _out=sys.stdout )


        condor_q("-batch","sphnxpro",_out=sys.stdout)        


        #psql(dbname="FileCatalog", 
        #     command="select dsttype,run,segment,cluster,process,status,started,running,ended,exit_code from production_status order by id desc limit 20;", _out=sys.stdout);


        #print("Pausing loop for 2min")
        sleep(args.delay)

if __name__ == '__main__':
    main()


