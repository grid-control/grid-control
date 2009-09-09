#!/bin/bash

SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
mkdir scratch
export GC_SCRATCH="$SANDBOX/scratch"
echo "Local variables..."
echo
cat "$SANDBOX/_jobconfig.sh"
source "$SANDBOX/_jobconfig.sh"
echo
echo "==========================="
echo

./run.sh $ARGS
cd $SANDBOX
rmdir scratch
