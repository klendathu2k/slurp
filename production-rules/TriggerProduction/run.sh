#!/usr/bin/bash

nevents=${1}
outbase=${2}
logbase=${3}
runnumber=${4}
segment=${5}
outdir=${6}
build=${7/./}
dbtag=${8}
inputs=(`echo ${9} | tr "," " "`)  # array of input files 
ranges=(`echo ${10} | tr "," " "`)  # array of input files with ranges appended
neventsper=${11:-1000}
logdir=${12}
histdir=${13:-/dev/null}
subdir=${14}
payload=(`echo ${15} | tr ","  " "`) # array of files to be rsynced
firstevent=${16}
lastevent=${17}
lasteventinrun=${18}
#-- Must always be last + 1
export cupsid=${19}

sighandler()
{
mv ${logbase}.out ${logdir#file:/}
mv ${logbase}.err ${logdir#file:/}
}
trap sighandler SIGTERM SIGINT SIGKILL

{

export USER="$(id -u -n)"
export LOGNAME=${USER}
export HOME=/sphenix/u/${USER}

source /opt/sphenix/core/bin/sphenix_setup.sh -n ${7}

export ODBCINI=./odbc.ini

# Stagein
for i in ${payload[@]}; do
    cp --verbose ${subdir}/${i} .
done

# Set state to started
./cups.py -r ${runnumber} -s ${segment} -d ${outbase} started

echo ..............................................................................................
echo $@
echo .............................................................................................. 
echo nevents: $nevents
echo outbase: $outbase
echo logbase: $logbase
echo runnumb: $runnumber
echo segment: $segment
echo outdir:  $outdir
echo build:   $build
echo dbtag:   $dbtag
echo inputs:  ${inputs[@]}
echo nper:    $neventsper
echo logdir:  $logdir
echo histdir: $histdir
echo firstevent: $firstevent
echo lastevent: $lastevent
echo lasteventinrun: $lasteventinrun
echo cupsid: $cupsid
echo .............................................................................................. 

# Size of the inputs array
leni=${#inputs[@]}
lenf=${#ranges[@]}

# Error condition if the input files and the file ranges are not equal
if [ ${leni} -ne ${lenf} ]; then
   echo "Input files and ranges are not equal... aborting workflow"
   ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e 1 --nevents 0 
   exit 1
fi

echo "Build associative arrays holding the min and max event in each file, keyed on file"
declare -A FE
declare -A LE

for events in ${ranges[@]}; do
   e=( `echo ${events} | tr ":" " "` ) # first and last event in the file
   fname=${e[2]}
   first=${e[0]}
   last=${e[1]}

   if [[ $string == *"GL1"* ]]; then
      FE["${fname}"]=$first
      LE["${fname}"]=$last
   else
      FE["${fname}"]=$(( first - 1 ))
      LE["${fname}"]=$(( last  - 1 ))
   fi

done

echo "Filling the input file lists"
echo firstevent: $firstevent
echo lastevent: $lastevent

lastsafety=$(( lastevent+19999 ))

inputlist=""
for (( i=0; i<${leni}; i++ )); do

    f=${inputs[$i]} # file to consider
    fbase=$( basename $f )
    fe=${FE["${fbase}"]}
    le=${LE["${fbase}"]}

    e=( $fe $le )

    #
    # The file should be included in this run IF
    #
    #   The first event in the file is in the range firstevent,lastevent
    #   The last event in the file is in the range firstevent,lastevent
    #   or
    #   The file encompasses the full range of the job
    #
    #   IF the first event is < the last event in the job range, and the
    #      last event of the file is unknown, we add the file to the list
    #

    if [[ ((${e[0]} -ge $firstevent) && (${e[0]} -le $lastsafety))||  \
          ((${e[1]} -ge $firstevent) && (${e[1]} -le $lastsafety))||  \
          ((${e[0]} -le $firstevent) && (${e[1]} -ge $lastsafety))||  \
          ((${e[0]} -eq $firstevent) || (${e[0]} -eq  $lastevent))||  \
          ((${e[1]} -eq $firstevent) || (${e[1]} -eq  $lastevent))||  \
          ((${e[0]} -lt $lastsafety) && (${e[1]} -le           0))    \
       ]]; then


 
    b=$( basename $f )
    if [[ $b =~ "GL1_cosmics" ]]; then
       echo ${f} >> gl1daq.list
       echo Add ${f} ${e[@]} to gl1daq.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "GL1_physics" ]]; then
       echo ${f} >> gl1daq.list
       echo Add ${f}  ${e[@]} to gl1daq.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "GL1_beam" ]]; then
       echo ${f} >> gl1daq.list
       echo Add ${f}  ${e[@]} to gl1daq.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "GL1_calib" ]]; then
       echo ${f} >> gl1daq.list
       echo Add ${f}  ${e[@]} to gl1daq.list
       inputlist="${f} ${inputlist}"
    fi

    if [[ $b =~ seb(00|01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20) ]]; then
       nn=${BASH_REMATCH[1]}
       echo ${f} >> seb${nn}.list
       echo Add ${f}  ${e[@]} to seb${nn}.list
       inputlist="${f} ${inputlist}"
    fi

    fi

done

echo "ls -l *.list"
ls -l *.list

nlist=$( ls *.list | wc -l )
if [[ $nlist -lt 21 ]]; then
   echo "Input file lists were not properly filled.  Aborting the job"
   ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e 2 --nevents 0 --inc 
    exit 2
fi

# Register the input list and set state to running
./cups.py -r ${runnumber} -s ${segment} -d ${outbase} inputs --files ${inputlist}
./cups.py -r ${runnumber} -s ${segment} -d ${outbase} running

# Flag the creation of a new dataset in dataset_status
dstname=${logbase%%-*}
#echo ./bachi.py --blame cups created ${dstname} ${runnumber} 
#     ./bachi.py --blame cups created ${dstname} ${runnumber}

# Write local
echo root.exe -q -b Fun4All_Prdf_Combiner.C\(${nevents},${firstevent},${lastevent},\"${logbase}.root\"\)
     root.exe -q -b Fun4All_Prdf_Combiner.C\(${nevents},${firstevent},${lastevent},\"${logbase}.root\"\); status_f4a=$?

# Flag run as finished.  Increment nevents by zero
echo ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e ${status_f4a} --nevents 0 --inc 
     ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e ${status_f4a} --nevents 0 --inc 

if [ "${status_f4a}" -eq 0 ]; then

   echo   stageout.sh ${logbase}.root ${outdir}
          stageout.sh ${logbase}.root ${outdir}

#   echo   ./bachi.py --blame cups finalized ${dstname} ${runnumber}
#          ./bachi.py --blame cups finalized ${dstname} ${runnumber}

else

   echo Fun4All exited with status ${status_f4a}
   echo ... list cwd ...
   ls -la

fi

echo "script done"
} >& ${logdir#file:/}/${logbase}.out 

exit ${status_f4a}


