#!/bin/bash

SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
export SCRATCH_DIRECTORY="$SANDBOX"
source jobconfig.sh
./grid.sh $ARGS
