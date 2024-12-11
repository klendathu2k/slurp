#!/usr/bin/bash 
{

echo $@

# Job wrapper will start with sphenix_setup level at NEW
source /opt/sphenix/core/bin/sphenix_setup.sh -n new

export userscript=$1       # this is the executable script
export cupsid=${@: -1:1}                         # this is the ID of the job on the production system
export payload=(`echo ${@: -2:1} | tr ","  " "`) # comma sep list --> array of files to stage in
export subdir=${@: -3:1}                         # ... relative to the submission directory

echo userscript: ${userscript}
echo cupsid:     ${cupsid}
echo payload:    ${payload[@]}
echo subdir:     ${subdir}

# stage in the payload files
for i in ${payload[@]}; do
    cp --verbose $subdir/$i . 
done

shift                      # everything else gets passed to the executable

chmod u+x ${userscript}

echo source ${userscript} ${@}
     source ${userscript} ${@}

} >& /sphenix/data/data02/sphnxpro/production-testbed/physics/run2pp/DST_STREAMING_EVENT_run2pp/ana444_2024p007/run_00053800_00053900/jobwrapper.${cupsid}.log

   


