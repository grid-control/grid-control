#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

GC_SANDBOX=${GC_SANDBOX:-$1}
GC_JOBCONF="$GC_SANDBOX/_jobconfig.sh"
source "$GC_JOBCONF"
if [ ! -f "$GC_SANDBOX/job_${MY_JOBID}.var" ]; then
	[ ! -f "$GC_JOBCONF" ] && echo "$GC_JOBCONF not found" && exit 101
	cp "$GC_JOBCONF" "$GC_SANDBOX/job_${MY_JOBID}.var"
fi

# Prime job info with error - killed by batch system
(
	echo "JOBID=$MY_JOBID"
	echo "EXITCODE=107"
) > "$GC_SANDBOX/job.info"

export GC_SCRATCH="$GC_SANDBOX/scratch"
mkdir "$GC_SCRATCH"
cd $GC_SANDBOX
./run.sh ${MY_JOBID}
rmdir "$GC_SCRATCH"
