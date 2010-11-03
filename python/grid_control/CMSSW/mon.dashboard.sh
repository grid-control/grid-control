#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

source $MY_LANDINGZONE/gc-run.lib || exit 101

case "$1" in
	"start")
		my_move "$MY_SCRATCH" "$MY_LANDINGZONE" "DashboardAPI.py Logger.py ProcInfo.py apmon.py report.py"
		DASH_ID=$(echo $TASK_NAME | var_replacer "" | sed "s/__/_/g;s/^_//;s/_$//")
		export REPORTID="taskId=$DASH_ID jobId=${MY_JOBID}_$GLITE_WMS_JOBID MonitorID=$DASH_ID MonitorJobID=${MY_JOBID}_$GLITE_WMS_JOBID"
		export MY_DASHBOARDINFO="$MY_LANDINGZONE/Dashboard.report"
		echo "Update Dashboard: $REPORTID"
		checkfile "$MY_LANDINGZONE/report.py"
		chmod u+x "$MY_LANDINGZONE/report.py"
		checkbin "$MY_LANDINGZONE/report.py"

		echo $MY_LANDINGZONE/report.py $REPORTID \
			SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
			WNname="$(hostname -f)" ExeStart="$DB_EXEC"
		$MY_LANDINGZONE/report.py $REPORTID \
			SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
			WNname="$(hostname -f)" ExeStart="$DB_EXEC"
		echo
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
		echo
		;;
esac
