#!/bin/bash

GC_SANDBOX=${GC_SANDBOX:-$1}
GC_JOBCONF="$GC_SANDBOX/_jobconfig.sh"
[ ! -f "$GC_JOBCONF" ] && echo "$GC_JOBCONF not found" && exit 101
source "$GC_JOBCONF"
mv "$GC_JOBCONF" "$GC_SANDBOX/job_${MY_JOBID}.var"

export GC_SCRATCH="$GC_SANDBOX/scratch"
mkdir "$GC_SCRATCH"
cd $GC_SANDBOX
./run.sh ${MY_JOBID}
rmdir "$GC_SCRATCH"
