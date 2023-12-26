#!/usr/env bash 

# Initialize a default sPHENIX environment for the job to execute in
source /opt/sphenix/core/bin/sphenix_setup.sh -n 

echo "-----------------------------------------------------------"
echo "hello from init.sh"
echo "-----------------------------------------------------------"

exec "$@"




