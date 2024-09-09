#!/bin/env python
import plotext as plt
import htcondor
import classad
import pprint
import datetime

def main():

    schedd = htcondor.Schedd()
    condor_job_status_map = {                          
        0:"Total",
        1:"Idle",
        2:"Running",
        3:"Removing",
        4:"Completed",
        5:"Held",
        6:"Transferring Output",
        7:"Suspended", 
    }

    colors=[ 
        "blue",
        "green+",
        "grey",
        "grey+",
        "red+",
        "green",
        "red"        
    ]

    try:
        condor_query = schedd.query(
            projection=["JobBatchName","JobStatus"]
        )                                                                                                                                                                                                  
    except htcondor.HTCondorIOError: 
        print("... could not query condor.  skipping report ...")
        return

    total_jobs       = {}
    idle_jobs        = {}
    running_jobs     = {}
    removing_jobs    = {}
    completed_jobs   = {}
    held_jobs        = {}
    transfering_jobs = {}
    suspended_jobs   = {}

    jobarray = [
        total_jobs       ,
        idle_jobs        ,
        running_jobs     ,
        removing_jobs    ,
        completed_jobs   ,
        held_jobs        ,
        transfering_jobs ,
        suspended_jobs   ,
    ]


    # Get the complete list of names
    jobnames = {}
    for q in condor_query:        
        name   = q['JobBatchName']
        jobnames[ name ] = 1
    jobnames = list( jobnames.keys() )

    # Initialize the jobarray
    for status in range(0,8):
        for name in jobnames:
            jobarray[status][name] = 0
                
    for q in condor_query:        

        status = int( q['JobStatus'] )
        name   = q['JobBatchName']

        jobarray[0][ name ] = jobarray[0][ name ] + 1
        jobarray[status][ name ] = jobarray[status][ name ] + 1


    statearray = []
    for state in range(1,8):
        statearray.append( [ jobarray[state][name] for name in jobnames ] )

    statelabels = [ condor_job_status_map[i] for i in range(1,8) ]
    
    plt.simple_stacked_bar( jobnames, statearray, labels=statelabels, width=200, title=f"Current production status {datetime.datetime.now()}", colors=colors )
    plt.title("Job states per type")
    plt.show()    

if __name__ == '__main__':    
    main()



