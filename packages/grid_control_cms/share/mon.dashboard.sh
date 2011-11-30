#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

source $MY_LANDINGZONE/gc-run.lib || exit 101

DASH_ID=$(echo $TASK_NAME | var_replacer "" | sed "s/__/_/g;s/^_//;s/_$//")
if [ -n "$GLITE_WMS_JOBID" ]; then
	GC_WMS_ID="$GLITE_WMS_JOBID"
	GC_CE_NAME="$GLOBUS_CE"
else
	GC_WMS_ID="https://sge:/${JOB_ID}"
	GC_CE_NAME="$(hostname -f)"
fi
export REPORTID="taskId=$DASH_ID jobId=${MY_JOBID}_$GC_WMS_ID MonitorID=$DASH_ID MonitorJobID=${MY_JOBID}_$GC_WMS_ID"

case "$1" in
	"start")
		my_move "$MY_SCRATCH" "$MY_LANDINGZONE" "DashboardAPI.py Logger.py ProcInfo.py apmon.py report.py"
		export MY_DASHBOARDINFO="$MY_LANDINGZONE/Dashboard.report"

		echo "Update Dashboard: $REPORTID"
		checkfile "$MY_LANDINGZONE/report.py"
		chmod u+x "$MY_LANDINGZONE/report.py"
		checkbin "$MY_LANDINGZONE/report.py"
		echo $MY_LANDINGZONE/report.py $REPORTID \
			SyncGridJobId="$GC_WMS_ID" SyncGridName="$TASK_USER" SyncCE="$GC_CE_NAME" \
			WNname="$(hostname -f)" ExeStart="$DB_EXEC"
		$MY_LANDINGZONE/report.py $REPORTID \
			SyncGridJobId="$GC_WMS_ID" SyncGridName="$TASK_USER" SyncCE="$GC_CE_NAME" \
			WNname="$(hostname -f)" ExeStart="$DB_EXEC"
		;;
	"stop")
		echo "Update Dashboard: $REPORTID"
		checkbin "$MY_LANDINGZONE/report.py"
		[ -f "$MY_DASHBOARDINFO" ] && DASH_EXT="$(< "$MY_DASHBOARDINFO")"
		echo $MY_LANDINGZONE/report.py $REPORTID \
			ExeEnd="$DB_EXEC" WCCPU="$GC_WRAPTIME" CrabUserCpuTime="$GC_CPUTIME" CrabWrapperTime="$GC_WRAPTIME" \
			ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE" $DASH_EXT
		$MY_LANDINGZONE/report.py $REPORTID \
			ExeEnd="$DB_EXEC" WCCPU="$GC_WRAPTIME" CrabUserCpuTime="$GC_CPUTIME" CrabWrapperTime="$GC_WRAPTIME" \
			ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE" $DASH_EXT
		;;
esac
