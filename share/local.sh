#!/bin/bash

GC_SANDBOX=${GC_SANDBOX:-$1}

[ ! -f "$GC_SANDBOX/_jobconfig.sh" ] && exit 101
source "$GC_SANDBOX/_jobconfig.sh"
mv "$GC_SANDBOX/_jobconfig.sh" "$GC_SANDBOX/job_${MY_JOBID}.var"

export GC_SCRATCH="$GC_SANDBOX/scratch"
mkdir "$GC_SCRATCH"
cd $GC_SANDBOX
./run.sh ${MY_JOBID}
rmdir "$GC_SCRATCH"
