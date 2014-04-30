#!/bin/bash
#-#  Copyright 2010-2013 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

source $MY_LANDINGZONE/gc-run.lib || exit 101

# Task name: derive DASH_TASK from TASK_NAME - replace "__" by "_" and remove "_" from start / end:
DASH_TASK=$(echo $TASK_NAME | var_replacer "" | sed "s/__/_/g;s/^_//;s/_$//")
export REPORTID="taskId=$DASH_TASK MonitorID=$DASH_TASK"

# JobID & CE
if [ -n "$CONDOR_WMS_DASHID" ]; then
	GC_WMS_ID="$CONDOR_WMS_DASHID"
	GC_CE_NAME="$(hostname -f)"
elif [ -n "$GLITE_WMS_JOBID" ]; then
	GC_WMS_ID="$GLITE_WMS_JOBID"
	GC_CE_NAME_GLITE="$(glite-brokerinfo getCE 2> /dev/null)"
	GC_CE_NAME="${GC_CE_NAME_GLITE:-${GLOBUS_CE:-$OSG_JOB_CONTACT}}"
else
	GC_WMS_ID="https://${GC_WMS_NAME:-LRMS}:/${JOB_ID}"
	GC_CE_NAME="$(hostname -f)"
fi
export DASH_JOBID="${MY_JOBID}_${GC_WMS_ID}"
export REPORTID="$REPORTID jobId=$DASH_JOBID MonitorJobID=$DASH_JOBID"

# Middleware info:
if [ -n "$OSG_APP" ]; then
	export REPORTID="$REPORTID GridFlavour=OSG"
elif [ -n "$GLITE_WMS_JOBID" ]; then
	export REPORTID="$REPORTID GridFlavour=LCG"
fi

# General dashboard submission
case "$1" in
	"start")
		my_move "$MY_SCRATCH" "$MY_LANDINGZONE" "DashboardAPI.py Logger.py ProcInfo.py apmon.py report.py"
		export MY_DASHBOARDINFO="$MY_LANDINGZONE/Dashboard.report"

		echo "Update Dashboard: $REPORTID"
		checkfile "$MY_LANDINGZONE/report.py"
		chmod u+x "$MY_LANDINGZONE/report.py"
		checkbin "$MY_LANDINGZONE/report.py"
		echo $MY_LANDINGZONE/report.py $REPORTID \
			SyncGridJobId="$GC_WMS_ID" SyncGridName="$GC_USERNAME" SyncCE="$GC_CE_NAME" WNHostName="$(hostname -f)" \
			WNname="$(hostname -f)" ExeStart="$DB_EXEC"
		$MY_LANDINGZONE/report.py $REPORTID \
			SyncGridJobId="$GC_WMS_ID" SyncGridName="$GC_USERNAME" SyncCE="$GC_CE_NAME" WNHostName="$(hostname -f)" \
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
