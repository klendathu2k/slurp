#!/usr/bin/env python
import sh
from time import sleep
from contextlib import redirect_stdout
import sys

kaedama  = sh.Command("kaedama.py")
condor_q = sh.Command("condor_q")
psql     = sh.Command("psql")


def main():
    while (True):
        print("Running the DST_EVENT rule")
        kaedama( rule="DST_EVENT", batch=True, _out=sys.stdout )
        print("Running the DST_CALOR rule")
        kaedama( rule="DST_CALOR", batch=True )
        condor_q("-batch","sphnxpro",_out=sys.stdout)        
        # b'  id  |     dsttype      |             dstname              |                    dstfile                     |  run  | segment | nsegments | inputs | prod_id | cluster | process | status  |     submitting      |      submitted      |       started       |       running       | ended | flags | exit_code | nevents \n'
        psql(dbname="FileCatalog", command="select dsttype,run,segment,cluster,process,status,started,running,ended,exit_code from production_status order by id;", _out=sys.stdout);
        print("Pausing loop for 2min")
        sleep(120)

if __name__ == '__main__':
    main()


