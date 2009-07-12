#!/bin/bash

SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
mkdir scratch
export SCRATCH_DIRECTORY="$SANDBOX/scratch"
source _jobconfig.sh
./grid.sh $ARGS
cd $SANDBOX
rmdir scratch
