#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

export GC_DOCLEANUP="true"
source gc-run.lib || exit 101

trap abort 0 1 2 3 15
export MY_JOBID="$1"
export MY_LANDINGZONE="`pwd`"
export MY_MARKER="$MY_LANDINGZONE/RUNNING.$$"
export MY_DASHBOARDINFO="$MY_LANDINGZONE/Dashboard.report"
export MY_SCRATCH="`getscratch`"
export MY_SEED=$RANDOM$RANDOM

shift

# Print job informations
echo "JOBID=$MY_JOBID"
echo "grid-control - Version$GC_VERSION"
echo "running on: `hostname -f; uname -a;`"
[ -f /etc/redhat-release ] && cat /etc/redhat-release
echo
echo "Job $MY_JOBID started - `date`"
STARTDATE=`date +%s`

echo
echo "==========================="
echo
checkvar MY_JOBID
checkvar MY_LANDINGZONE
checkvar MY_SCRATCH
checkfile "$MY_LANDINGZONE/job_${MY_JOBID}.var"
source "$MY_LANDINGZONE/job_${MY_JOBID}.var"
export

echo
echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
checkdir "Scratch directory" "$MY_SCRATCH"

# Monitor space usage
echo $$ > $MY_MARKER
if [ -n "$(getrealdir $MY_SCRATCH | grep $(getrealdir $MY_LANDINGZONE))" ]; then
	echo "\$MY_SCRATCH is a subdirectory of \$MY_LANDINGZONE"; echo
	# Landing zone: Used space < 5Gb && Free space > 1Gb (using limits on the scratch directory)
	monitordirlimits "SCRATCH" $MY_LANDINGZONE &
else
	# Landing zone: Used space < 50Mb && Free space > 100Mb
	monitordirlimits "LANDINGZONE" "$MY_LANDINGZONE" &
	# Landing zone: Used space < 5Gb && Free space > 1Gb
	monitordirlimits "SCRATCH" "$MY_SCRATCH" &
fi

echo "==========================="
echo
checkfile "$MY_LANDINGZONE/sandbox.tar.gz"
echo "Unpacking environment"
tar xvfz "$MY_LANDINGZONE/sandbox.tar.gz" -C "$MY_SCRATCH" || fail 105

checkfile "$MY_SCRATCH/_config.sh"
source "$MY_SCRATCH/_config.sh"

echo "Prepare variable substitution"
checkfile "$MY_SCRATCH/_varmap.dat"
echo "__DATE__: Variable substitution in task __TASK_ID__: __X__" | var_replacer "SUCCESSFUL"
checkfile "$MY_SCRATCH/_replace.awk"
cat "$MY_SCRATCH/_replace.awk"

# Job timeout (for debugging)
if [ ${DOBREAK:-1} -gt 0 ]; then
(
	sleep ${DOBREAK} &&
	echo "===! Timeout after ${DOBREAK} sec !===" 1>&2 &&
	updatejobinfo 123 &&
	kill -1 $$
) &
fi
echo

# Setup dependencies
if [ -n "$GC_DEPFILES" ]; then
	echo "==========================="
	echo
	for DEPFILE in $GC_DEPFILES; do
		checkfile "$MY_SCRATCH/env.$DEPFILE.sh"
		source "$MY_SCRATCH/env.$DEPFILE.sh"
	done
fi

if [ "$DASHBOARD" == "yes" ]; then
	echo
	echo "==========================="
	echo
	my_move "$MY_SCRATCH" "$MY_LANDINGZONE" "DashboardAPI.py Logger.py ProcInfo.py apmon.py report.py"
	DASH_ID=$(echo $TASK_NAME | var_replacer "" | sed "s/__/_/g;s/^_//;s/_$//")
	export REPORTID="taskId=$DASH_ID jobId=${MY_JOBID}_$GLITE_WMS_JOBID MonitorID=$DASH_ID MonitorJobID=${MY_JOBID}_$GLITE_WMS_JOBID"
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
fi

# Select SE:
if [ -n "$SE_PATH" ]; then
	echo "Complete SE list:"
	for SE in $SE_PATH; do echo " * $SE"; done
	echo "Close SE:"
	SE_CLOSE="$(get_default_se $SE_PATH)"
	for SE in $SE_CLOSE; do echo " * $SE"; done
	[ -n "$SE_CLOSE" ] && export SE_PATH="$SE_CLOSE"
	echo "Selected SE:"
	export SE_PATH="$(get_random_se $SE_PATH)"
	echo " => $SE_PATH"
fi

# Copy files from the SE
if [ -n "$SE_INPUT_FILES" ]; then
	echo "==========================="
	echo
	url_copy "$SE_PATH" "file:///$MY_SCRATCH" "$SE_INPUT_FILES"
	echo
fi

echo "==========================="
echo
# Do variable substitutions
for SFILE in $SUBST_FILES "_config.sh"; do
	echo "Substitute variables in file $SFILE"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	var_replacer "" < "`_find $SFILE`" | tee "$SFILE.tmp"
	[ -f "$SFILE.tmp" ] && cat "$SFILE.tmp" > "`_find $SFILE`"
	[ -f "$SFILE.tmp" ] && rm "$SFILE.tmp"
done

SAVED_SE_PATH="$SE_PATH"
checkfile "$MY_SCRATCH/_config.sh"
source "$MY_SCRATCH/_config.sh"
export SE_PATH="$SAVED_SE_PATH"

echo
echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
checkdir "Scratch directory" "$MY_SCRATCH"

# Execute program
echo "==========================="
echo

cd "$MY_SCRATCH"
(cat << EOF
${MY_RUNTIME/\$@/$GC_ARGS}
EOF
) > $MY_LANDINGZONE/_runtime.sh
export MY_RUNTIME="$(var_replacer '' < "$MY_LANDINGZONE/_runtime.sh")"
checkvar MY_RUNTIME
eval "$MY_RUNTIME" &
MY_RUNID=$!
echo "Process $MY_RUNID is running..."
echo $MY_RUNID > $MY_MARKER
wait $MY_RUNID
CODE=$?
echo $$ > $MY_MARKER
zip_files "$SB_OUTPUT_FILES"
cd "$MY_LANDINGZONE"

echo "Process $MY_RUNID exit code: $CODE"
updatejobinfo $CODE
echo

echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
[ -d "$MY_SCRATCH" ] && checkdir "Scratch directory" "$MY_SCRATCH"

if [ -d "$MY_SCRATCH" -a -n "$SB_OUTPUT_FILES" ]; then
	echo "==========================="
	echo
	# Move output into landingzone
	my_move "$MY_SCRATCH" "$MY_LANDINGZONE" "$SB_OUTPUT_FILES"
	echo
fi

export LOG_MD5="$MY_LANDINGZONE/SE.log"
# Copy files to the SE
if [ $CODE -eq 0 -a -n "$SE_OUTPUT_FILES" ]; then
	echo "==========================="
	echo
	export TRANSFERLOG="$MY_SCRATCH/SE.log"
	url_copy "file:///$MY_SCRATCH" "$SE_PATH" "$SE_OUTPUT_FILES"
	(
	[ -f "$TRANSFERLOG" ] && cat "$TRANSFERLOG" | while read NAME_LOCAL NAME_DEST; do
		echo "FILE$IDX=\"$(cd "$MY_SCRATCH"; md5sum "$NAME_LOCAL")  $NAME_DEST  $SE_PATH\""
		IDX=$[IDX + 1]
	done
	) > "$LOG_MD5"
	export TRANSFERLOG=""
	echo
fi

# Emulate grid wildcard support
if [ -n "$GC_WC" ]; then
	echo "==========================="
	echo
	echo "Fake grid wildcard support"
	GC_WCFILES=$(for X in $GC_WC; do echo $X; done | sort | uniq)
	[ -n "$GC_WCFILES" ] && tar czvf "GC_WC.tar.gz" $GC_WCFILES
	echo
fi

echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
[ -d "$MY_SCRATCH" ] && checkdir "Scratch directory" "$MY_SCRATCH"

times > "$MY_LANDINGZONE/cputime"
GC_CPUTIME=`cat "$MY_LANDINGZONE/cputime" | awk '{gsub("s","m"); split($1,x,"m"); SUM+=x[1]*60+x[2]}END{printf "%.0f\n", SUM}'`
GC_WRAPTIME="$[ $(date +%s) - $STARTDATE ]"

if [ "$DASHBOARD" == "yes" ]; then
	echo "==========================="
	echo
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
fi

echo "==========================="
echo
cleanup
trap - 0 1 2 3 15
echo "Job $MY_JOBID finished - `date`"
echo "TIME=$GC_WRAPTIME" >> $MY_LANDINGZONE/job.info
[ -f "$LOG_MD5" ] && cat "$LOG_MD5" >> $MY_LANDINGZONE/job.info
cat $MY_LANDINGZONE/job.info
echo

exit $CODE
