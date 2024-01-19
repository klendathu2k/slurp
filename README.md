sPHENIX Lightweight Utilities for Realtime Production (SLURP)
-------------------------------------------------------------

SLURP provides utilities for the submission and management of
production jobs via condor.  It consists of three user-facing
components.

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




