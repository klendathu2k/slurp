#!/usr/bin/bash 

# Create an initialization script on the worker node to get around singularity
cat <<EOF > sPHENIX_INIT

    echo "Executing sPHENIX_INIT: build ${1}"
    source /opt/sphenix/core/bin/sphenix_setup.sh -n ${1}

    # user has supplied an odbc.ini file.  use it.
    if [ -e odbc.ini ]; then
	echo "... setting user provided odbc.ini file"
	export ODBCINI=./odbc.ini
    fi

    if [ -e sPHENIX_newcdb.json ]; then
	echo "... setting user provided conditions database config"
	export NOPAYLOADCLIENT_CONF=./sPHENIX_newcdb.json
    fi

EOF

hostname
echo $@

# This is the container that the job will run under
container=/cvmfs/sphenix.sdcc.bnl.gov/singularity/rhic_sl7.sif

export userscript=$1       # this is the executable script
export cupsid=${@: -1:1}                         # this is the ID of the job on the production system
export payload=( `echo ${@: -2:1} | tr ","  " "` ) # comma sep list --> array of files to stage in
export subdir=${@: -3:1}                         # ... relative to the submission directory



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
    cp --verbose $subdir/$i . 
done

# user has supplied an odbc.ini file.  use it.
if [ -e odbc.ini ]; then
echo "Setting user provided odbc.ini file"
export ODBCINI=./odbc.ini
fi

if [ -e sPHENIX_newcdb.json ]; then
echo "Setting user provided conditions database config"
export NOPAYLOADCLIENT_CONF=./sPHENIX_newcdb.json
fi

chmod u+x ${userscript} sPHENIX_INIT

echo "Running the job in singularity: ${container}"
singularity exec -B /home -B /direct/sphenix+u -B /gpfs02 -B /sphenix/u -B /sphenix/lustre01 -B /sphenix/user  -B /sphenix/data/data02 ${container} ./${userscript} ${userArgs[@]}

 

