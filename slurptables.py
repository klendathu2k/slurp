#!/usr/bin/env python

import pyodbc
import htcondor
import classad
import re
import uuid
import os
import pathlib
import pprint
import math

from dataclasses import dataclass, asdict, field

#____________________________________________________________________________
def sphnx_production_setup_table_def():
    result="""
CREATE TABLE if not exists PRODUCTION_SETUP 
(
       id       serial           unique ,    -- unique identifier
       name     varchar(32)      not null,   -- dsttype eg DST_CALO_auau1
       build    varchar(8)       not null,   -- software build eg ana.387
       dbtag    varchar(8)       not null,   -- database tag eg 2023p003
       repo     text             not null,   -- git repo used
       dir      text             not null,   -- directory relative to repo
       hash     varchar(8)       not null,   -- hash for the production setup
       primary key (name,build,dbtag,hash)
)
"""
    return result

@dataclass( frozen=True )
class SPhnxProductionSetup:
    id :        int
    name :      str
    build :     str
    dbtag :     str
    repo:       str
    dir_:       str
    hash_:      str
#   fromrun:    int
#   lastrun:    int
    is_clean:   bool
    is_current: bool



#____________________________________________________________________________
def sphnx_production_status_table_state_enum_def():
    result = """
CREATE TYPE prodstate as ENUM 
(
       'submitting',       -- job is being submitted (set by slurp)
       'submitted',        -- job is submitted (set by slurp)
       'started',          -- job has started on the worker node (set by cups)
       'running',          -- job is running (set by cups periodically)
       'evicted',          -- job has recieved a bad signal (set by cups)
       'failed',           -- job ended with nonzero status code (set by cups)
       'finished'          -- job has finished (set by cups)
)
    """
    return result

def sphnx_production_status_table_def( dsttype=None, build=None, dbtag=None ):
    result="""
CREATE TABLE if not exists PRODUCTION_STATUS
(
       id        serial          unique  ,   -- unique identifier
       dsttype   varchar(16)     not null,   -- dst type eg DST_CALO
       dstname   varchar(32)     not null,   -- dst name eg DST_CALO_auau1_ana387_2023p003
       dstfile   varchar(64)     not null,   --          eg DST_CALO_auau1_ana387_2023p003-12345678-1234.root
       run       int             not null,   -- runnumber
       segment   int             not null,   -- segment number
       nsegments int             not null,   -- number of produced segments
       inputs    text            not null,   -- array of input files (possibly null)
       prod_id   int             not null,   -- production id

       cluster   int             not null,   -- condor cluster
       process   int             not null,   -- condor process

       status     prodstate       not null,   -- status

       submitting timestamp              ,   -- timestamp when job is being submitted
       submitted  timestamp              ,   -- timestamp when job is submitted
       started    timestamp              ,   -- job script has started
       running    timestamp              ,   -- payload macro is running
       ended      timestamp              ,   -- job has ended (see prodstate and/or exit code for details)      

       flags      int                     ,   -- flags
       exit_code  int                     ,   -- exit code of user script

       nevents    int                     ,   -- number of events

       submission_host varchar(16)        ,
       execution_node  varchar(32)        ,
 
       message text                       ,

       logsize         int,

       foreign key (prod_id) references PRODUCTION_SETUP (id) ,

       primary key (id,run,segment,prod_id)         

);    
"""   
    return result


@dataclass( frozen=True )
class SPhnxProductionStatus:
    id:        int
    dsttype:   str
    dstname:   str
    dstfile:   str
    run:       int
    segment:   int
    nsegments: int 
    inputs:    list[str]
    prod_id:   int
    cluster:   int
    process:   int
    status:    str
    submitting: str
    submitted: str
    started: str
    running: str
    ended: str
    flags:     str 
    exit_code: int 
    nevents : int
    message : str
    submission_host: str
    execution_node: str
    logsize: int


def sphnx_production_quality_table_def():
    return """
    CREATE TABLE if not exists PRODUCTION_QUALITY (
       id        serial          unique  ,   -- unique identifier
       dstname   varchar(32)     not null,   -- dst name eg DST_CALO_auau1_ana387_2023p003
       run       int             not null,   -- runnumber
       segment   int             not null,   -- segment number
       stat_id   int             not null,   -- corresponding entry in the status table
       qual      jsonb           not null,   -- quality

       foreign key (stat_id) references PRODUCTION_STATUS (id) ,
       primary key (id, dstname, run, segment )

    );
    """


#_____________________________________
def sphnx_production_dataset():

    return """
    CREATE TABLE if not exists DATASET_STATUS (
       id        serial      unique  
    ,  dstname   varchar(63) not null 
    ,  run       int         not null
    ,  lastrun   int         default 0
    ,  revision  int         default 0      -- incremented each time the dataset is resubmitted
    ,  nsegments int         default 0      -- total number of segments (files) produced in this dataset
    ,  parent    text        default 'DAQ'  -- parent dataset (upgrade? comma separated list of datasets?)
    ,  created   timestamp                  -- timestamp when the dataset was created (job submitted by kaedama)
    ,  updated   timestamp                  -- timestamp for the last update (file added to the dataset)
    ,  finalized timestamp                  -- timestamp when the dataset is finalized (event builder ends / downstream job produces last segment)
    ,  status    text                       -- arbitrary status string
    ,  blame     text                       -- who updated the status
    ,  primary key(id,dstname,run)
    );
    """

@dataclass( frozen=True )
class SPhnxDatasetStatus:
    id: int
    dstname: str
    run: int
    lastrun: int
    revision: int
    nsegments: int
    parent: str
    submitted: str
    updated: str
    finalized: str
    status: str
    blame: str


#_____________________________________
def sphnx_invalid_run_list():
    
    return """
    CREATE TABLE if not exists INVALID_RUN_LIST (
       id            serial      unique  
    ,  dstname       varchar(32) not null 
    ,  first_run     int         not null
    ,  last_run      int         not null default    -1
    ,  first_segment int         not null default     0
    ,  last_segment  int         not null default 99999
    ,  created_at    timestamp   not null default (now()                 at time zone 'utc')
    ,  expires_at    timestamp   not null default ('2161-10-11 00:00:00' at time zone 'utc')
    ,  primary key(id,dstname,run )
    );
    """

@dataclass( frozen=True )
class SPhnxInvalidRunList:
    id: int
    dstname: str
    run: int
    first_segment: int
    last_segment: int
    created_at: str
    expires_at: str
    
