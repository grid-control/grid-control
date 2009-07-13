#!/bin/bash
source run.lib || exit 101

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

# Job timeout (for debugging)
if [ ${DOBREAK:-1} -gt 0 ]; then
(
	sleep ${DOBREAK} &&
	echo "===! Timeout after ${DOBREAK} sec !===" 1>&2 &&
	updatejobinfo 123 &&
	kill -1 $$
) &
fi

checkvar MY_RUNTIME

echo
echo "==========================="
echo
echo "Prepare variable substitution"
checkfile "$MY_SCRATCH/_varmap.dat"
echo "__DATE__: Variable substitution __X__" | var_replacer "SUCCESSFUL"
checkfile "$MY_SCRATCH/_replace.awk"
cat "$MY_SCRATCH/_replace.awk"

# Copy files from the SE
if [ -n "$SE_INPUT_FILES" ]; then
	url_copy "$SE_PATH" "file:///$MY_SCRATCH" "$SE_INPUT_FILES"
fi

# Do variable substitutions
for SFILE in $SUBST_FILES; do
	echo "Substitute variables in file $SFILE"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	var_replacer "$SFILE" < "`_find $SFILE`" | tee "tmp.$SFILE"
	[ -f "tmp.$SFILE" ] && mv "tmp.$SFILE" "`_find $SFILE`"
done

if [ "$DASHBOARD" == "yes" ]; then
	echo
	echo "==========================="
	echo
	export REPORTID="taskId=$TASK_ID jobId=${MY_JOBID}_$GLITE_WMS_JOBID MonitorID=$TASK_ID MonitorJobID=${MY_JOBID}_$GLITE_WMS_JOBID"
	echo "Update Dashboard: $REPORTID"
	checkfile "$MY_SCRATCH/report.py"
	chmod u+x "$MY_SCRATCH/report.py"
	checkbin "$MY_SCRATCH/report.py"

	echo $MY_SCRATCH/report.py $REPORTID \
		SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
		WNname="$(hostname -f)" ExeStart="$DB_EXEC"
	$MY_SCRATCH/report.py $REPORTID \
		SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
		WNname="$(hostname -f)" ExeStart="$DB_EXEC"
fi

echo
echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
checkdir "Scratch directory" "$MY_SCRATCH"

# Execute program
echo "==========================="
echo
cd $MY_SCRATCH
eval "$MY_RUNTIME" &
MY_RUNID=$!
echo $MY_RUNID > $MY_MARKER
wait $MY_RUNID
CODE=$?
echo $$ > $MY_MARKER
cd $MY_LANDINGZONE
echo
echo "==========================="
echo
echo "Job exit code: $CODE"
updatejobinfo $CODE

echo
echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
checkdir "Scratch directory" "$MY_SCRATCH"

if [ "$DASHBOARD" == "yes" ]; then
	echo
	echo "==========================="
	echo
	echo "Update Dashboard: $REPORTID"
	checkbin "$MY_SCRATCH/report.py"
	[ -f "$MY_DASHBOARDINFO" ] && DASH_EXT="$(< "$MY_DASHBOARDINFO")"
	cat 
	echo $MY_SCRATCH/report.py $REPORTID \
		ExeEnd="$DB_EXEC" WCCPU="$[ $(date +%s) - $STARTDATE ]" \
		ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE" $DASH_EXT
	$MY_SCRATCH/report.py $REPORTID \
		ExeEnd="$DB_EXEC" WCCPU="$[ $(date +%s) - $STARTDATE ]" \
		ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE" $DASH_EXT
	echo
fi

# Copy files to the SE
if [ $CODE -eq 0 -a -n "$SE_OUTPUT_FILES" ]; then
	echo "==========================="
	echo
	echo "##MD5-SUMS -- this is a marker line used by verify.py -- do not edit."
	(cd "$MY_SCRATCH"; md5sum $SE_OUTPUT_FILES)
	url_copy "file:///$MY_SCRATCH" "$SE_PATH" "$SE_OUTPUT_FILES"
fi

echo "==========================="
echo
# Move output into landingzone
my_move "$MY_SCRATCH" "$MY_LANDINGZONE" "$SB_OUTPUT_FILES"

echo
echo "==========================="
echo
checkdir "Start directory" "$MY_LANDINGZONE"
checkdir "Scratch directory" "$MY_SCRATCH"

echo "==========================="
echo
cleanup
trap - 0 1 2 3 15
echo "Job $MY_JOBID finished - `date`"
echo "TIME=$[`date +%s` - $STARTDATE]" >> $MY_LANDINGZONE/jobinfo.txt
cat $MY_LANDINGZONE/jobinfo.txt

exit $CODE
