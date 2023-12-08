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
       int      fromrun                  ,   -- if set first run covered by the production setup
       int      lastrun                  ,   -- if set last run covered by the production setup
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
    is_clean:   bool
    is_current: bool



#____________________________________________________________________________
def sphnx_production_status_table_state_enum_def():
    result = """
CREATE TYPE prodstate as ENUM 
(
       'submitted',        -- job is submitted (set by slurp)
       'started',          -- job has started on the worker node (set by cups)
       'running',          -- job is running (set by cups periodically)
       'evicted',          -- job has recieved a bad signal (set by cups)
       'failed',           -- job ended with nonzero status code (set by cups)
       'finished'          -- job has finished (set by cups)
)
    """
    return result

def sphnx_production_status_table_def( dsttype, build, dbtag ):
    result="""
CREATE TABLE if not exists STATUS_%s_%s_%s 
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

       foreign key (prod_id) references PRODUCTION_SETUP (id) ,

       primary key (id,run,segment,prod_id)         

);    
"""%(dsttype,build.replace(".",""),dbtag)
    print(result)
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
    
