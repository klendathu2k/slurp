#!/usr/bin/env python

import pyodbc

# Connect to the DAQ databse
daqdb = pyodbc.connect("DSN=daq;UID=phnxro;READONLY=True");

# Get the cursor
cursor  = daqdb.cursor()

def example():

    #
    # Express an SQL query.  We will select all columns from the 'filelist'
    # table and only return rows where the run number is between 39000 and 39010
    # inclusive.
    #
    query_text="""
    select * from filelist where runnumber>=39000 and runnumber<=39010
    """

    # Get the results of the databse query
    results = cursor.execute( query_text )

    # Print the name of each row
    print("Print the column headers")
    print("-------------------------------------------------------------------------------------------------")
    for columnInfo in cursor.description:
        name     = columnInfo[0]
        datatype = columnInfo[1]
        print(f"Column {name} is a {datatype}")

    # Now iterate over all the rows in the query and just print them
    print("Print the rows returned from the query")
    print("-------------------------------------------------------------------------------------------------")
    for row in cursor.fetchall():

        print(row) # The row can be printed by itself...

    # We make another query... this time limiting ourseleves to any
    # file that contains the letters "seb".  The "%" before and after "seb" is a wildcard that
    # will match anything.
    query_text="""
    select * from filelist where runnumber>=39000 and runnumber<=39010 and filename like '%seb%'
    """    

    # Get the results of the databse query
    results = cursor.execute( query_text )

    # Iterate again and illustrate how to access the values...    
    for row in cursor.fetchall():
        runnumber  = row.runnumber
        hostname   = row.hostname
        filename   = row.filename
        sequence   = row.sequence
        firstevent = row.firstevent
        lastevent  = row.lastevent
        events     = row.events
        md5sum     = row.md5sum
        ctime      = row.ctime
        mtime      = row.mtime
        status     = row.status
        size       = row.size
        to_hpss    = row.transferred_to_hpss
        to_sdcc    = row.transferred_to_sdcc
        bytes_hpss = row.transferred_bytes_hpss
        bytes_sdcc = row.transferred_bytes_sdcc

        # ... and print them out again
        print( runnumber,hostname,filename,sequence,firstevent,lastevent,events,md5sum,ctime,mtime,status,size,to_hpss,to_sdcc,bytes_hpss,bytes_sdcc )




if __name__=='__main__':
    
    example()
