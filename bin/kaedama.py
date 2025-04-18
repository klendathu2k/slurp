#!/usr/bin/env python 

import cProfile
#import slurp
import yaml
import datetime
import pathlib

from slurp import SPhnxRule  as Rule
from slurp import SPhnxMatch as Match
from slurp import SPhnxCondorJob as Job
from slurp import matches
from slurp import submit 

import argparse
from slurp import arg_parser
from slurp import parse_command_line 

from slurp import RUNFMT as RUNFMT_
from slurp import SEGFMT as SEGFMT_

from slurp import PRODUCTION_MODE
from slurp import get_production_cursor

import sh
import sys
import re
import os

import platform

#from slurp import cursors

import logging
from logging.handlers import RotatingFileHandler

import pprint

# Extend the command line arguments
arg_parser.add_argument( '-n', '--nevents', default=0, dest='nevents', help='Number of events to process.  0=all.', type=int)
arg_parser.add_argument( '--rule', help="Submit against specified rule", default="DST_EVENT" )
arg_parser.add_argument( '--limit', help="Maximum number of jobs to submit", default=0, type=int )
arg_parser.add_argument( '--submit',help="Job will be submitted", dest="submit", default="True", action="store_true")
arg_parser.add_argument( '--no-submit', help="Job will not be submitted... print things", dest="submit", action="store_false")
arg_parser.add_argument( '--runs', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list.  If cursor is provided as the first argument, start from the last run wholly or partially submitted.  The second argument is interpreted as a window, i.e. submit runs cursor to cursor+N inclusive.", default=['cursor'] )
arg_parser.add_argument( '--runlist', default=None, help="Flat text file containing list of runs to process, separated by whitespace / newlines." )
arg_parser.add_argument( '--segments', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[] )
arg_parser.add_argument( '--config',help="Specifies the yaml configuration file")
arg_parser.add_argument( '--docstring',default=None,help="Appends a documentation string to the log entry")
arg_parser.add_argument( '--experiment-mode',dest="mode",help="Specifies the experiment mode (commissioing or physics) for direct lookup of input files.",default="physics")

arg_parser.add_argument( '--test-mode',dest="test_mode",default=False,help="Sets testing mode, which will mangle DST names and directory paths.",action="store_true")
arg_parser.add_argument( '--mangle-dstname',dest='mangle_dstname',help="Replaces 'DST' with the specified name.", default=None )
arg_parser.add_argument( '--mangle-dirpath',dest='mangle_dirpath',help="Inserts string after sphnxpro/ (or tmp/) in the directory structure", default=None, type=int )
arg_parser.add_argument( '--maxjobs',dest="maxjobs",help="Maximum number of jobs to pass to condor", default=None )
arg_parser.add_argument( '--print-query',dest='printquery',help="Print the query after parameter substitution and exit", action="store_true", default=False )

arg_parser.add_argument( '--append-to-rsync', dest='append2rsync', default=None,help="Appends the argument to the list of rsync files to copy to the worker node" )

arg_parser.add_argument( '--logdir', dest='logdir', default=None, help="Directory for kaedama logging (defaults under /tmp)" )

arg_parser.add_argument( '--advance-cursor', dest='advance_cursor', default=False, action="store_true", help="Advances the production run cursor to the last run submitted (sticks to last running job)")
arg_parser.add_argument( '--ratchet-cursor', dest='ratchet_cursor', default=False, action="store_true", help="Advances the production run cursor to the last run submitted")
arg_parser.add_argument( '--set-cursor', dest='set_cursor', default=None, help="Sets the production run cursor to the provided value.")

arg_parser.add_argument( '--dbtag', dest='dbtag', default=None, help=argparse.SUPPRESS ) # System option.  If provided by ramenya it will override the DB option specified in the yaml file.
arg_parser.add_argument( '--input-dataset', dest='dataset', default=None, help=argparse.SUPPRESS ) # System option.  If provided by ramenya it will override the dataset option specified in the yaml file.

arg_parser.add_argument( '--mem', dest='mem', default=None, help=argparse.SUPPRESS ) # System option.  If provided by ramenya it will override the parameter specified in the yaml file.

arg_parser.add_argument( '--mask', nargs='*', dest='mask', default=[], help=argparse.SUPPRESS ) # System option.  Manipulates the param['required'] list if it is provided.  Removes the specified system from the list.

#
# Specifies the default directory layout for files.  Note that "production" will be replaced with "production-testbed" for the
# testbed setups.
#
_default_filesystem = {
        'outdir'   :           "/sphenix/lustre01/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/dst"
    ,   'logdir'   : "file:///sphenix/data/data02/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/log"
    ,   'histdir'  :        "/sphenix/data/data02/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/hist"
    ,   'calibdir' :        "/sphenix/data/data02/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/calib"    
    ,   'condor'   :                                 "/tmp/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/log"    
}

def sanity_checks( params, inputq ):
    result = True

    #
    # The following rules establish the sPHENIX naming convention.  In stageout.sh we
    # extract the dst type, name, run, etc... based on the positions of fields separated
    # by the underscores and/or dashes.
    #
    # These stageout.sh scripts are reasonably general, but limited in the way that they
    # are extracting information from the filename.  The assume that the dst type, build,
    # etc... can be extracted from fixed positions relative to the '_' in the filename.
    # (and the filename are based on the params.name checked here...)
    #
    # We ought to be able to extend this for new workflows, so long as we generalize the 
    # stageout script.  Once we generalize said script, we should be able to bring that
    # script into the old workflows... and then not worry.
    #

    # Name should be of the form DST_NAME_runXauau
    #if re.match( "[A-Z][A-Z][A-Z]_([A-Z]+_)+[a-z0-9]+", params['name'] ) == None:
    #    logging.warn( f'params.name {params["name"]} does not respect the sPHENIX convention:  DST_NAME_run<N>species' )
    #    result = False

    # Build and dbtag should not contain a "_"
    if re.match("_",params['build']):
        logging.error( f'params.build {params["build"]} cannot contain an underscore' )
        result = False
    if re.match("_",params['build_name']):
        logging.error( f'params.build_name {params["build_name"]} cannot contain an underscore' )
        result = False
    if re.match("_",params['dbtag']):
        logging.error( f'params.dbtag {params["dbtag"]} cannot contain an underscore' )
        result = False

    build = params['build']
    rev   = params.get( 'version', None )

    if rev is None:
        params['version']=0
        rev = 0

    assert ( rev >= 0 )
    
    if rev==0 and build != 'new':
        logging.error( f'production version must be nonzero for fixed builds' )
        result = False

    if rev!=0 and build == 'new':
        logging.error( 'production version must be zero for new build' )
        result = False
    
    return result


#def dbconsistency():
#    try:
#        idw = slurp.statusdbw.execute( "select id from production_status order by id desc limit 1" ).fetchone().id
#        idr = slurp.statusdbw.execute( "select id from production_status order by id desc limit 1" ).fetchone().id
#    except:
#        logging.warn( "Read and write instance of status db are out of sync / or could not connect to one or both." )
#    return (idr,idw)

def checkRequiredParams( params ):

    fatal=False
    if params.get( 'rsync', None )==None:
        logging.error("Specify rsync: <payload files> in the params block.")
        fatal=True

    if fatal:
        logging.error("YAML rule is not properly defined.  Correct above errors and try again.")
        exit(1)
        
    

def main():

    # parse command line options
    args, userargs = parse_command_line()

    mycwd = pathlib.Path(".")
    if 'testbed' in str(mycwd.absolute()).lower() or pathlib.Path(".slurp/testbed").is_file():
        args.test_mode = True
        logging.info("Running in testbed mode.")

    if args.test_mode:
        args.mangle_dirpath = 'production-testbed'
        

    if args.test_mode:
        mylogdir=f"/tmp/testbed/kaedama/{args.rule}"; #{str(datetime.datetime.today().date())}.log",
    else:
        mylogdir=f"/tmp/kaedama/kaedama/{args.rule}"; #{str(datetime.datetime.today().date())}.log",
    pathlib.Path(mylogdir).mkdir( parents=True, exist_ok=True )

    if args.logdir:
        mylogdir=args.logdir

    RotFileHandler = RotatingFileHandler(
    #    filename='kaedama.log', 
        filename=f"{mylogdir}/{str(datetime.datetime.today().date())}.log",
        mode='a',
        maxBytes=25*1024*1024,
    #   maxBytes=5*1024,
        backupCount=10,
        encoding=None,
        delay=0
    )

    # n.b. Adding the stream handler will echo to stdout
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            #logging.FileHandler("kaedama.log"),
            RotFileHandler,
            logging.StreamHandler()
        ]
    )


#    # require consistent database
#    (idr, idw) = dbconsistency()
#    count=0
#    while idr < idw:
#        logging.warning(f"DB inconsistency.  Master at {idw} replica at {idr}.")
#        if count==5: 
#            logging.warning("Timeout after 5 min")
#            return
#        count=count+1
#        time.sleep(60)
#        (idr, idw) = dbconsistency()        

#    logging.info(f"Executing kaedama on {platform.node()} pid {os.getpid()}")
#    logging.info(f"DB consistency.  Master at {idw} replica at {idr}.")

    config={}
    with open(args.config,"r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Could not open {args.config}")
            print(exc)

    run_condition = ""
    if len(args.runs)==1 and args.runs[0] != 'cursor':
        run_condition = f"and runnumber={args.runs[0]} "
    elif len(args.runs)==1 and args.runs[0] == 'cursor':
        run_condition = f"and runnumber>={args.runs[0]} "        

    elif len(args.runs)==2 and args.runs[0] != 'cursor':
        run_condition = f"and runnumber>={args.runs[0]} and runnumber<={args.runs[1]} "
    elif len(args.runs)==2 and args.runs[0] == 'cursor':
        run_condition = f"and runnumber>=cursor and runnumber<=cursor+{args.runs[1]} "

        
    elif len(args.runs)>=3 and args.runlist==None:
        run_condition = "and runnumber in ( %s )" % ','.join( args.runs )
    elif args.runlist:
        runs = []
        with open( args.runlist, "r" ) as rl:
            lines = [line.rstrip() for line in file]
            for line in lines:
                for run in lines.split():
                    runs.append(run)
        run_condition = "and runnumber in ( %s )" % ','.join( runs )

    seg_condition = ""
    if len(args.segments)==1:
        seg_condition = f"and segment={args.segments[0]}"
    elif len(args.segments)==2:
        seg_condition = f"and segment>={args.segments[0]} and segment<={args.segments[1]}"
    elif len(args.segments)>=3:
        seg_condition = "and segment in ( %s )" % ','.join( args.segments )

    #streamname = args.streamname
    #streamfile = args.streamfile

    RUNFMT = RUNFMT_
    SEGFMT = SEGFMT_
    PWD    = str(pathlib.Path(".").absolute())

    limit_condition=""
    if args.limit>0:
        limit_condition = f"limit {args.limit}"
        
    # Reduce configuration to this rule
    try:
        config = config[ args.rule ]
    except KeyError:
        logging.error(f"Could not locate '{args.rule}' in configuration file")
        pprint.pprint( config.keys() )
        return

    logging.info( f"Executing rule {args.rule} where ... {run_condition} {seg_condition} {limit_condition}" )

    #
    # Get  the user parameters and append additional payload if specified on the command line.
    # Also if the command line overrides a 
    #
    params          = config.get('params',None)
    if args.append2rsync:
        params['rsync'] = params['rsync']+','+args.append2rsync
    params['rsync'] = params['rsync'] + ",.slurp/"

    if args.dbtag:
        logging.warn( f"Override cdb tag in config {params['dbtag']} with {args.dbtag}" )
        params['dbtag']=args.dbtag

    if args.dataset:
        logging.warn( f"Override dataset in config {params['dataset']} with {args.dataset}" )
        params['dataset']=args.dataset        

    if args.mem:
        params['mem']=args.mem                
        logging.warn( f"Override mem in config {params['mem']} with {args.mem}" )

        

    # if the keyword 'cursor' appears, we will lookup the production cursor and replace it here
    if 'cursor' in run_condition:
        print( params['name'] )
        print( params['build_name'] )
        print( params['dbtag'] )
        print( params.get('version',None) )
        runcursor=get_production_cursor( params['name'], params['build_name'], params['dbtag'], params['version'] )
        run_condition=run_condition.replace('cursor',str(runcursor))

    if params:

        if params.get('required',None):
            params['required']=params['required'].strip()
            for mask in args.mask:
                params['required'] = params['required'].replace( mask, '' )
            params['required']=params['required'].replace('  ',' ')
            params['required']=params['required'].strip()            
            logging.info( f"Setting 'required' subsystems: {params['required']}" )                    
    

    # Input query specifies the source of the input files
    input_         = config.get('input')
    input_query    = input_.get('query','').format(**locals(),**params)
    input_query_db = input_.get('db',None)
    input_query_direct = input_.get('direct_path',None)
    if input_query_direct is not None:
        input_query_direct = input_query_direct.format( **vars( args ) )

    input_query_lfn2pfn = input_.get('lfn2pfn','lfn2pfn')        


    if args.printquery:
        print(input_query)
        return


    runlist_query = config.get('runlist_query','').format(**locals())

    if params:

        params['name']=params['name'].format( **locals() )

        if args.mangle_dstname:
            params['name']=params['name'].replace('DST',args.mangle_dstname)
            logging.info(f"DST name is mangled to {params['name']}")

        for key in ['outbase','logbase']:
            try:
                params[key]=params[key].format(**locals())
            except KeyError:
                print(key)
                print(params[key])
                pprint.pprint(locals())
                params[key]=params[key].format(**locals())

    if args.test_mode:
        print("[TESTMODE: print parameter block]")
        pprint.pprint(params)
                

    # Default filesystem.  Override with vaules specified in the workflow.
    filesystem   = _default_filesystem
    filesystem_  = config.get('filesystem',{} )
    for k,v in filesystem_.items():
        filesystem[k] = v

            
    # Mangle directory path is specified.  Production is replaced with...
    if filesystem and args.mangle_dirpath:
        for key,val in filesystem.items():
            filesystem[key]=filesystem[key].replace("production",args.mangle_dirpath)

    # Version number will default to zero
    version_number = params.get('version',0)
    if version_number is not None:
        version_number = f"v{version_number:03d}"


    # If we have a version number futher manipulate the directory structure...
    #if version_number is not None:
    #    for key,val in filesystem.items():
    #        filesystem[key]=filesystem[key].replace("{leafdir}","{leafdir}/"+f"{version_number}")        
        
        


            

    if args.test_mode:
        print("[TESTMODE: print filesystem block]")
        pprint.pprint(filesystem)

    job_          = config.get('job',None)
    presubmit     = config.get('presubmit',None)

    if args.test_mode:
        print("[TESTMODE: print job block]")
        pprint.pprint(job_)


    # Do not submit if we fail sanity check on definition file
    if sanity_checks( params, input_ ) == False:        exit(1)

    if runlist_query =='': runlist_query = None
    if input_query   =='': input_query   = None

    #__________________________________________________________________________________
    #
    # Pre Submission Phase... execute the action script on the results of the 
    # specified query to the specified database.
    #__________________________________________________________________________________
    #if presubmit:
    #    cursor=cursors[ presubmit.get('db','fcc') ]
    #    pre_query  = presubmit.get('query', '').format(**locals())
    #    result_ = [ list(x) 
    #        for x in  cursor.execute(pre_query).fetchall() 
    #    ]
    #    for result in result_:
    #        query      = ' '.join([ str(x) for x in result ])
    #        pre_action = presubmit.get('action','').format( **locals())
    #        pre_action = pre_action.split()
    #        actor=sh.Command( pre_action[0] )
    #        actor( *pre_action[1:], _out=sys.stdout )


    #__________________________________________________________________________________
    #
    # Job Submission Phase
    #__________________________________________________________________________________

    
    jobkw = {}
    job   = None
    if job_:
        assert( filesystem is not None )
        assert( params     is not None )
        assert( params.get('rsync',None) is not None), "The params block should specify rsync: <payload directories and files>"
        assert( '{PWD}'   in job_['arguments']), "The last two arguments must be {PWD} {rsync}" 
        assert( '{rsync}' in job_['arguments']), "The last two arguments must be {PWD} {rsync}" 
        
        for k,v in job_.items():
            jobkw[k] = v.format( **locals(), **filesystem, **params )



        # And now we can create the job definition thusly
        job = Job( **jobkw )


    #
    # Perform job submission IFF we have the params, input_query, filesystem
    # and job blocks
    #
        
    if args.submit and params and input_query and filesystem and job:
        dst_rule = Rule( name              = params['name'],
                         files             = input_query,
                         filesdb           = input_query_db,
                         direct            = input_query_direct,
                         lfn2pfn           = input_query_lfn2pfn,
                         runlist           = runlist_query,            # deprecated TODO
                         script            = params['script'],
                         build             = params['build'],
                         tag               = params['dbtag'],
                         payload           = params['payload'],
                         job               = job,
                         limit             = args.limit,
                         version          = version_number
                     )


        if args.test_mode:
            print("[TESTMODE: print constructe rule]")
            pprint.pprint(dst_rule)

        #
        # Extract the subset of parameters that we need to pass to submit.  Note that (most) submitkw
        # arguments will be passed down to the matches function in the kwargs dictionary.
        #
        submitkw = { kw : val for kw,val in params.items() if kw in ["mem","disk","dump", "neventsper"] }

        dispatched = submit (dst_rule, args.maxjobs, nevents=args.nevents, **submitkw, **filesystem ) 

        batch="batch"
        if args.batch==False:
            batch="user"        
        if args.docstring:
            batch=batch + " " + args.docstring

        runcount = {}
        ndisp=0
        if type(dispatched) == type([]):
            ndisp=len(dispatched)
            for tup in dispatched:
                arr = runcount.get( tup[0], [] )
                arr.append( tup[1] )


        logging.info( f"Dispatched ({batch} {args.runs}) {args.rule}: {params['name']} {ndisp} dispatched" )
        logging.info( f"  with {args}" )        
        keys = runcount.keys()
        for k in keys:
            logging.info( f"Dispatched run {k} segments: {','.join(runcount[k])}" )

            


if __name__ == '__main__': 
    #cProfile.run('main()')
    main()
