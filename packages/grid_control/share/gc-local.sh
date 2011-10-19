#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

GC_SANDBOX="${GC_SANDBOX:-$1}"
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

[ -d "/tmp" ] && export GC_LOCALSCRATCH="/tmp/${RANDOM}_${MY_JOBID}"
[ -n "$GC_LOCALSCRATCH" ] && mkdir -p "$GC_LOCALSCRATCH"
cd "$GC_SANDBOX"
if [ -n "$GC_DELAY_OUTPUT" ]; then
	./gc-run.sh ${MY_JOBID} > "$GC_LOCALSCRATCH/gc.stdout.tmp" 2> "$GC_LOCALSCRATCH/gc.stderr.tmp"
	mv "$GC_LOCALSCRATCH/gc.stdout.tmp" "$GC_DELAY_OUTPUT"
	mv "$GC_LOCALSCRATCH/gc.stderr.tmp" "$GC_DELAY_ERROR"
else
	./gc-run.sh ${MY_JOBID}
fi
[ -n "$GC_LOCALSCRATCH" ] && rmdir "$GC_LOCALSCRATCH"
