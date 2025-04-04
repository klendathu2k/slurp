#!/usr/bin/bash 

# Jobwrapper will set an alma9 container when the ana build is alma9.  Otherwise
# it runs the SL7 container.

# Create an initialization script on the worker node to get around singularity
#  ... deprecated ???
cat <<EOF > sPHENIX_INIT

#    echo "Executing sPHENIX_INIT: build ${1}"
#    source /opt/sphenix/core/bin/sphenix_setup.sh -n ${1}

    # user has supplied an odbc.ini file.  use it.
    if [ -e odbc.ini ]; then
	echo "... setting user provided odbc.ini file"
	export ODBCINI=./odbc.ini
    fi

    if [ -e sPHENIX_newcdb.json ]; then
	echo "... setting user provided conditions database config"
	export NOPAYLOADCLIENT_CONF=./sPHENIX_newcdb.json
    fi

    if [ -e sPHENIX_newcdb_test.json ]; then
	echo "... setting user provided conditions database config"
	export NOPAYLOADCLIENT_CONF=./sPHENIX_newcdb_test.json
    fi

EOF

onsighup ()  {
    ./cups.py -r 0 -s 0 -d xxx message "SIGHUP" --sighup
}
onsigint ()  {
    ./cups.py -r 0 -s 0 -d xxx message "SIGINT" --sigint
}
onsigkill () {
    ./cups.py -r 0 -s 0 -d xxx message "SIGKILL" --sigkill
}
onsigpipe () {
    ./cups.py -r 0 -s 0 -d xxx message "SIGPIPE" --sigpipe
}
onsigusr1 () {
    ./cups.py -r 0 -s 0 -d xxx message "SIGUSR1" --sigusr1
}
onsigusr2 () {
    ./cups.py -r 0 -s 0 -d xxx message "SIGUSR2" --sigusr2
}

trap onsighup SIGHUP
trap onsigint SIGINT
trap onsigkill SIGKILL
trap onsigpipe SIGPIPE
trap onsigusr1 SIGUSR1
trap onsigusr2 SIGUSR2

hostname
echo $@

# This is the container that the job will run under
container=/cvmfs/sphenix.sdcc.bnl.gov/singularity/rhic_sl7.sif
#container=/cvmfs/sphenix.sdcc.bnl.gov/singularity/sdcc_a9.sif

export userscript=$1                               # this is the executable script
export cupsid=${@: -1:1}                           # this is the ID of the job on the production system
                                                   # payload and subdir are nominally specified in the yaml file
export payload=( `echo ${@: -2:1} | tr ","  " "` ) # comma sep list --> array of files to stage in
export subdir=${@: -3:1}                           # ... relative to the submission directory

myArgs=( "$@")
shift
userArgs=( "$@" )

OS=$( hostnamectl | awk '/Operating System/{ print $3" "$4 }' )
echo "Setting up SLURP for ${OS}"

# We are relying on the sPHENIX build to be in argument #7.
# TODO:  buildarg should be one of the special arguments appended at the end of the list
source /opt/sphenix/core/bin/sphenix_setup.sh -n ${7}
export PATH=${PATH}:${HOME}/bin


if [[ $OFFLINE_MAIN =~ "*alma*" ]]; then
    echo "OFFLINE_MAIN points to an alma9 build.  Run in alma9 container";
    container=/cvmfs/sphenix.sdcc.bnl.gov/singularity/sdcc_a9.sif
fi

if [[ $OS =~ "Alma" ]]; then
   echo "Rejiggering python path"
   export PYTHONPATH=/opt/sphenix/core/lib/python3.9/site-packages
   alias python=/usr/bin/python
   alias pip=/opt/sphenix/core/bin/pip3.9
fi

echo Argument list is now completely fubared
echo $@

echo userscript: ${userscript}
echo cupsid:     ${cupsid}
echo payload:    ${payload[@]}
echo subdir:     ${subdir}

# stage in the payload files
for i in ${payload[@]}; do
    cp --verbose -r $subdir/$i . 
done

source .slurp/slurppath.sh
cp ${SLURPPATH}/cups.py .


# Test if we are in testbed mode
if [[ $subdir  =~ "*testbed*" ]]; then
    echo "Running in a testbed environment [subdir]"
    export CUPS_TESTBED_MODE=true
    touch CUPS_TESTBED_MODE
elif [[ $subdir  =~ "*Testbed*" ]]; then
    echo "Running in a testbed environment [subdir]"
    export CUPS_TESTBED_MODE=true
    touch CUPS_TESTBED_MODE
elif [[ -e ".slurp/testbed" ]]; then
    echo "Running in a testbed environment [.slurp/testbed]"
    export CUPS_TESTBED_MODE=true
    touch CUPS_TESTBED_MODE    
else
    echo "Running in a production environment"
    export CUPS_PRODUCTION_MODE=true
    touch CUPS_PRODUCTION_MODE
fi



# user has supplied an odbc.ini file.  use it.
if [ -e odbc.ini ]; then
echo "Setting user provided odbc.ini file"
export ODBCINI=./odbc.ini
fi

if [ -e sPHENIX_newcdb.json ]; then
echo "Setting user provided conditions database config"
export NOPAYLOADCLIENT_CONF=./sPHENIX_newcdb.json
fi

if [ -e sPHENIX_newcdb_test.json ]; then
echo "Setting user provided conditions database config"
export NOPAYLOADCLIENT_CONF=./sPHENIX_newcdb_test.json
fi

# Test for the existence of the user script.  Add message if it is missing.
if [ ! -f ${userscript} ]; then
    ./cups.py -r 0 -s 0 -d xxx message "User script is missing." --error 'payload-stagein-failed'
    exit 1
fi






chmod u+x ${userscript} sPHENIX_INIT

echo "Running the job in singularity: ${container}"
singularity exec -B /home -B /direct/sphenix+u -B /gpfs02 -B /sphenix/u -B /sphenix/lustre01 -B /sphenix/user  -B /sphenix/data/data02 ${container} ./${userscript} ${userArgs[@]}


exit $?
 

