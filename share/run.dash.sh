#!/bin/bash

source $MY_LANDINGZONE/run.lib || exit 101

echo "Dashboard module starting"

checkfile "$MY_SCRATCH/_config.sh"
source "$MY_SCRATCH/_config.sh"

if [ "$DASHBOARD" == "yes" ]; then
	export REPORTID="taskId=$TASK_ID jobId=${MY_JOBID}_$GLITE_WMS_JOBID MonitorID=$TASK_ID MonitorJobID=${MY_JOBID}_$GLITE_WMS_JOBID"
	echo "Update Dashboard: $REPORTID"
	checkfile "$MY_SCRATCH/report.py"
	chmod u+x "$MY_SCRATCH/report.py"
	checkbin "$MY_SCRATCH/report.py"

	echo $MY_SCRATCH/report.py $REPORTID \
		SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
		WNname="$(hostname -f)" ExeStart="shellscript"
	$MY_SCRATCH/report.py $REPORTID \
		SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
		WNname="$(hostname -f)" ExeStart="shellscript"
fi

WALL_START="$(date +%s)"

echo "---------------------------"
echo "Executing $@ ..."
eval "$@"
CODE=$?
echo "---------------------------"

# Move output into scratch

if [ "$DASHBOARD" == "yes" ]; then
	echo "Update Dashboard: $REPORTID"
	checkbin "$MY_SCRATCH/report.py"
	echo $MY_SCRATCH/report.py $REPORTID \
		ExeEnd="shellscript" WCCPU="$[ $(date +%s) - $WALL_START ]" \
		ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE"
	$MY_SCRATCH/report.py $REPORTID \
		ExeEnd="shellscript" WCCPU="$[ $(date +%s) - $WALL_START ]" \
		ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE"
fi

exit $CODE
