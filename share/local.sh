#!/bin/bash

SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
mkdir scratch
export GC_SCRATCH="$SANDBOX/scratch"
cat "$SANDBOX/_jobconfig.sh"
source "$SANDBOX/_jobconfig.sh"
./run.sh $ARGS
cd $SANDBOX
rmdir scratch
