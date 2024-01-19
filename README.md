sPHENIX Lightweight Utilities for Realtime Production (SLURP)
-------------------------------------------------------------

# Introduction

SLURP provides utilities for the submission and management of
production jobs via condor, backed by a file catalog and
production status (database tables) used to determine which
datasets have been produced under what conditions, and track
the location of the analysis artifacts.

Support for quality assesment tables a work in progress.

## User Facing Components

- kaedama.py -- used for dispatching jobs for one or more runs
  based on a single specified rule.  Typical usage:

  Submit a single run
  $ kaedama.py --runs 22026 --rule DST_EVENT --config sphenix_auau23.yaml

  Submit a range of runs (two runs always interpreted as a range).
  $ kaedama.py --runs 22000 22100 --rule DST_EVENT --config sphenix_auau23.yaml

  Submit a list of runs.  
  $ kaedama.py --runs 22026 22027 22033 ...  --rule DST_EVENT --config sphenix_auau23.yaml

  Submit a list of 2 runs by providing 0 as an invalid 3rd run.
  $ kaedama.py --runs 22026 22027 0  --rule DST_EVENT --config sphenix_auau23.yaml

- ramenya.py -- runs a loop over multiple runs and/or rules, and/or prints statistics
  about the currently running set of production jobs.  Typical usage:

  Run a dispatch loop over runs in the specified range (same syntax as kaedama for
  single run and/or list of runs).  10min pause between each iteration of the loop.
  $ ramenya.py --runs 22000 23000 --rules all --delay 600

  Run dispatch loops over single rules with different delays, and a seperate monitoring
  process.
  $ ramenya.py --runs 22000 23000 --rules DST_EVENT --delay 300 --outputs none &
  $ ramenya.py --runs 22000 23000 --rules DST_CALOR --delay 600 --outputs none &
  $ ramenya.py --runs 22000 23000 --rules none      --delay  60 --outputs pending started clusters

- A yaml-based job definition file.  e.g. sphenix_auau23.yaml

  - See the job definition file for a... hopefully reasonably well... annotated
    example.

## Job Instrumentation

Two python scripts are provided which are meant to instrument the production scripts.
These python scripts (cups.py and pull.py) provide run-time updates of the status
of production jobs and may be used to insert output files into the file catalog.

- cups.py is the Common User Production Script.  It can be operated in two modes.  A
  wrapper mode (still work in progress) which runs the user's production script, and
  handles the database signalling automaticaly, and a tagging mode (supported) which
  allows the user to signal the production status table when a job has reach various
  states.  

  For example, running a calorimeter production one would define a dataset name like
  the following... (it should follow the naming convention of the output files)...

  DST_CALOR_auau23_${build}_${dbtag}

  Insert the following line near the top of the production script to signal that the
  job has reached the "started" stage.
  
  ./cups.py -r ${runnumber} -s 0 -d ${datasetname} started  

  The following line would be added just before running the production macro (or macros).
  It signals that the job is currently running.

  ./cups.py -r ${runnumber} -s 0 -d ${datasetname} running

  Following the end of the macro, you should capture the status flag as

  macro_status=$?

  and signal to the status table the completion state of the job

  ./cups.py -r ${runnumber} -s 0 -d ${datasetname} exitcode -e ${macro_status}


- pull.py is a helper script which can be used to insert new files into the file
  catalog in the event builder jobs, and to pass back accumulated event totals to
  the production status table...

  root -q -b Fun4All_EventBuilder.C(...) | pull.py

  root -q -b Fun4All_CaloChain.C(...) | pull.py

## Expectations for user scripts

  - The stdout and stderr of the user scripts should be captured in logfiles which
    follow the naming convention estabilsed in the "transfer_output_files" line of
    the job description in sphenix_auau23.yaml.

  - The user is generally responsible for staging files out to the filesystem and
    registering them with the file catalog.  The exception is the event builder
    where (1) the output should be written directly to the lustre filesystem and
    (2) pull.py should be used to register the output files.  You may need to change
    the verbosity level in the fun4all macro for file registration to work.

  - Number of events should be emitted at the end of the job for "pull.py" to pick up.
    The output is of the form "<output file>.root, <nevents>"


