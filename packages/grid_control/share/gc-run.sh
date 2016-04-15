#!/bin/bash
# | Copyright 2008-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

export GC_DOCLEANUP="true"
source gc-run.lib || exit 101

set +f
gc_trap
export GC_JOB_ID="$1"
export MY_JOBID="$GC_JOB_ID" # legacy script support
export GC_LANDINGZONE="`pwd`"
export GC_MARKER="$GC_LANDINGZONE/RUNNING.$$"
export GC_FAIL_MARKER="$GC_LANDINGZONE/GCFAIL"
export GC_SCRATCH="`getscratch`"
shift

# Print job informations
echo "JOBID=$GC_JOB_ID"
echo "grid-control - Version $GC_VERSION"
echo "running on: `hostname -f 2>&1; uname -a;`"
lsb_release -a 2> /dev/null
echo
echo "Job $GC_JOB_ID started - `date`"
export STARTDATE=`date +%s`
timestamp "WRAPPER" "START"

echo
echo "==========================="
echo
checkvar GC_JOB_ID
checkvar GC_LANDINGZONE
checkvar GC_SCRATCH
export | display_short

echo
echo "==========================="
echo
checkdir "Start directory" "$GC_LANDINGZONE"
checkdir "Scratch directory" "$GC_SCRATCH"

echo "==========================="
echo
timestamp "DEPLOYMENT" "START"
echo "==========================="
checkfile "$GC_LANDINGZONE/gc-sandbox.tar.gz"

echo "Unpacking basic job configuration"
tar xvfz "$GC_LANDINGZONE/gc-sandbox.tar.gz" -C "$GC_SCRATCH" _config.sh || fail 105
checkfile "$GC_SCRATCH/_config.sh"
source "$GC_SCRATCH/_config.sh"

# Monitor space usage
echo $$ > $GC_MARKER
if [ -n "$(getrealdir $GC_SCRATCH | grep $(getrealdir $GC_LANDINGZONE))" ]; then
	echo "\$GC_SCRATCH is a subdirectory of \$GC_LANDINGZONE"; echo
	# Landing zone: Used space < 5Gb && Free space > 1Gb (using limits on the scratch directory)
	monitordirlimits "SCRATCH" $GC_LANDINGZONE &
else
	# Landing zone: Used space < 50Mb && Free space > 100Mb
	monitordirlimits "LANDINGZONE" "$GC_LANDINGZONE" &
	# Landing zone: Used space < 5Gb && Free space > 1Gb
	monitordirlimits "SCRATCH" "$GC_SCRATCH" &
fi

echo "Unpacking environment"
tar xvfz "$GC_LANDINGZONE/gc-sandbox.tar.gz" -C "$GC_SCRATCH" || fail 105
checkfile "$GC_LANDINGZONE/job_${GC_JOB_ID}.var"
cat "$GC_LANDINGZONE/job_${GC_JOB_ID}.var" >> "$GC_SCRATCH/_config.sh"
source "$GC_SCRATCH/_config.sh"

echo "Prepare variable substitution"
checkfile "$GC_SCRATCH/_varmap.dat"
echo "@DATE@: Variable substitution in task @GC_TASK_ID@: @X@" | var_replacer "SUCCESSFUL"
checkfile "$GC_SCRATCH/_replace.awk"
cat "$GC_SCRATCH/_replace.awk" | display_short

# Job timeout (for debugging)
if [ ${GC_JOBTIMEOUT:-1} -gt 0 ]; then
(
	sleep ${GC_JOBTIMEOUT} &&
	echo "===! Timeout after ${GC_JOBTIMEOUT} sec !===" 1>&2 &&
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
		checkfile "$GC_SCRATCH/env.$DEPFILE.sh"
		source "$GC_SCRATCH/env.$DEPFILE.sh"
	done
	echo
fi

# Notify monitoring about job start
if [ -n "$GC_MONITORING" ]; then
	echo "==========================="
	echo
	my_move "$GC_SCRATCH" "$GC_LANDINGZONE" "$GC_MONITORING"
	echo
	for MON_APP in $GC_MONITORING; do
		checkfile "$GC_LANDINGZONE/$MON_APP"
		source "$GC_LANDINGZONE/$MON_APP" "start"
	done
	echo
fi
echo "==========================="
timestamp "DEPLOYMENT" "DONE"
echo

echo
timestamp "SE_IN" "START"
# Select SE:
if [ -n "$SE_INPUT_PATH" -o -n "$SE_OUTPUT_PATH" ]; then
	echo "==========================="
	echo
	echo "Complete SE list:";
	for SE in $SE_INPUT_PATH; do echo " < $SE"; done
	for SE in $SE_OUTPUT_PATH; do echo " > $SE"; done
	echo "Close SE:"
	for SE in $(get_default_se "$SE_INPUT_PATH"); do echo " < $SE"; done | sort | uniq
	for SE in $(get_default_se "$SE_OUTPUT_PATH"); do echo " > $SE"; done | sort | uniq
	echo "Selected SE:"
	export SE_INPUT_PATH="$(get_best_se "SE_INPUT_PATH")"
	export SE_OUTPUT_PATH="$(get_best_se "SE_OUTPUT_PATH")"
	echo " * $SE_INPUT_PATH => ... => $SE_OUTPUT_PATH * "
	echo
fi

# Copy files from the SE
if [ -n "$SE_INPUT_FILES" ]; then
	echo "==========================="
	echo
	url_copy "$SE_INPUT_PATH" "file:///$GC_SCRATCH" "$SE_INPUT_FILES"
	echo
fi

echo "==========================="
timestamp "SE_IN" "DONE"
echo
# Do variable substitutions
for SFILE in $SUBST_FILES "_config.sh"; do
	echo "Substitute variables in file $SFILE"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	var_replacer "" < "`_find $SFILE`" | tee "$SFILE.tmp"
	[ -f "$SFILE.tmp" ] && cat "$SFILE.tmp" > "`_find $SFILE`"
	[ -f "$SFILE.tmp" ] && rm "$SFILE.tmp"
done

SAVED_SE_INPUT_PATH="$SE_INPUT_PATH"
SAVED_SE_OUTPUT_PATH="$SE_OUTPUT_PATH"
checkfile "$GC_SCRATCH/_config.sh"
source "$GC_SCRATCH/_config.sh"
export SE_INPUT_PATH="$SAVED_SE_INPUT_PATH"
export SE_OUTPUT_PATH="$SAVED_SE_OUTPUT_PATH"

echo
echo "==========================="
echo
checkdir "Start directory" "$GC_LANDINGZONE"
checkdir "Scratch directory" "$GC_SCRATCH"

# Execute program
echo "==========================="
echo

timestamp "EXECUTION" "START"
echo "==========================="
cd "$GC_SCRATCH"
echo "${GC_RUNTIME/\$@/$GC_ARGS}" > $GC_LANDINGZONE/_runtime.sh
export GC_RUNTIME="$(var_replacer '' < "$GC_LANDINGZONE/_runtime.sh")"
checkvar GC_RUNTIME
eval "$GC_RUNTIME" &
GC_PROCESS_ID=$!
echo "Process $GC_PROCESS_ID is running..."
echo $GC_PROCESS_ID > $GC_MARKER
wait $GC_PROCESS_ID
GC_PROCESS_CODE=$?
echo $$ > $GC_MARKER
zip_files "$SB_OUTPUT_FILES"
cd "$GC_LANDINGZONE"
echo "==========================="
timestamp "EXECUTION" "DONE"

echo "==========================="
echo
checkdir "Start directory" "$GC_LANDINGZONE"
[ -d "$GC_SCRATCH" ] && checkdir "Scratch directory" "$GC_SCRATCH"

if [ -d "$GC_SCRATCH" -a -n "$SB_OUTPUT_FILES" ]; then
	echo "==========================="
	echo
	# Move output into landingzone
	my_move "$GC_SCRATCH" "$GC_LANDINGZONE" "$SB_OUTPUT_FILES"
	echo
fi

timestamp "SE_OUT" "START"
export LOG_MD5="$GC_LANDINGZONE/SE.log"
# Copy files to the SE
if [ $GC_PROCESS_CODE -eq 0 -a -n "$SE_OUTPUT_FILES" ]; then
	echo "==========================="
	echo
	export TRANSFERLOG="$GC_SCRATCH/SE.log"
	url_copy "file:///$GC_SCRATCH" "$SE_OUTPUT_PATH" "$SE_OUTPUT_FILES"
	(
	[ -f "$TRANSFERLOG" ] && cat "$TRANSFERLOG" | while read NAME_LOCAL NAME_DEST; do
		MD5HASH=$(cd "$GC_SCRATCH"; md5sum "$NAME_LOCAL" | cut -d " " -f 1)
		echo "FILE$IDX=\"$MD5HASH  $NAME_LOCAL  $NAME_DEST  $SE_OUTPUT_PATH\""
		echo "OUTPUT_FILE_${IDX:-0}_LOCAL=\"$NAME_LOCAL\""
		echo "OUTPUT_FILE_${IDX:-0}_DEST=\"$NAME_DEST\""
		echo "OUTPUT_FILE_${IDX:-0}_PATH=\"$SE_OUTPUT_PATH\""
		echo "OUTPUT_FILE_${IDX:-0}_HASH=$MD5HASH"
		echo "OUTPUT_FILE_${IDX:-0}_SIZE=$(cd "$GC_SCRATCH"; stat -c%s "$NAME_LOCAL")"
		IDX=$[IDX + 1]
	done
	) > "$LOG_MD5"
	export TRANSFERLOG=""
	echo
fi
echo "==========================="
timestamp "SE_OUT" "DONE"

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
checkdir "Start directory" "$GC_LANDINGZONE"
[ -d "$GC_SCRATCH" ] && checkdir "Scratch directory" "$GC_SCRATCH"

# Notify monitoring about job stop
export GC_WRAPTIME="$[ $(date +%s) - $STARTDATE ]"
if [ -n "$GC_MONITORING" ]; then
	echo "==========================="
	echo
	times > "$GC_LANDINGZONE/cputime"
	GC_CPUTIMEPARSER='{gsub("s","m"); split($1,x,"m"); SUM+=x[1]*60+x[2]}END{printf "%.0f\n", SUM}'
	export GC_CPUTIME=`cat "$GC_LANDINGZONE/cputime" | awk "$GC_CPUTIMEPARSER"`

	for MON_APP in $GC_MONITORING; do
		checkfile "$GC_LANDINGZONE/$MON_APP"
		source "$GC_LANDINGZONE/$MON_APP" "stop"
	done
	echo
fi

echo "==========================="
echo
cleanup
gc_untrap
if [ ! -f $GC_FAIL_MARKER ]; then # only write job exit code if nothing failed
	echo "Process $GC_PROCESS_ID exit code: $GC_PROCESS_CODE"
	updatejobinfo $GC_PROCESS_CODE
fi
echo
echo "Job $GC_JOB_ID finished - `date`"
echo "TIME=$GC_WRAPTIME" >> $GC_LANDINGZONE/job.info
[ -f "$LOG_MD5" ] && cat "$LOG_MD5" >> $GC_LANDINGZONE/job.info
cat $GC_LANDINGZONE/job.info
echo
echo "==========================="
timestamp "WRAPPER" "DONE"
echo
timereport

exit $GC_PROCESS_CODE
