#!/usr/bin/env python
import sh
from time import sleep
from contextlib import redirect_stdout
import sys

kaedama  = sh.Command("kaedama.py")
condor_q = sh.Command("condor_q")
psql     = sh.Command("psql")


def main():
    first=True
    while (True):
        print("Running the DST_EVENT rule")

        kaedama( "--runs", "22026", "22027", 
                 rule="DST_EVENT", 
                 batch=True, 
                 _out=sys.stdout )


        print("Running the DST_CALOR rule")
        kaedama( 
            "--runs", "22026", "22027",
            rule="DST_CALOR", batch=True )
        condor_q("-batch","sphnxpro",_out=sys.stdout)        


        psql(dbname="FileCatalog", 
             command="select dsttype,run,segment,cluster,process,status,started,running,ended,exit_code from production_status where run<=22027 and run>=22026 order by id;", _out=sys.stdout);


        print("Pausing loop for 2min")
        sleep(120)

if __name__ == '__main__':
    main()


