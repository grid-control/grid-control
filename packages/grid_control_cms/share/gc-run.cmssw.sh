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

# 110 - project area setup failed
# 111 - CMSSW environment unpacking failed
# 112 - CMSSW environment setup failed
# 113 - Problem while hashing config file

source $GC_LANDINGZONE/gc-run.lib || exit 101

echo "CMSSW module starting"
echo
echo "---------------------------"
timestamp "CMSSW_STARTUP" "START"
echo "==========================="

echo "NEventsProcessed=${MAX_EVENTS:-0}" > ${GC_DASHBOARDINFO:-/dev/null}

checkvar "VO_CMS_SW_DIR"
checkfile "$VO_CMS_SW_DIR/cmsset_default.sh"

saved_SCRAM_VERSION="$SCRAM_VERSION"
saved_SCRAM_ARCH="$SCRAM_ARCH"
source "$VO_CMS_SW_DIR/cmsset_default.sh"
SCRAM_VERSION="$saved_SCRAM_VERSION"
export SCRAM_ARCH="$saved_SCRAM_ARCH"
declare +x SCRAM_VERSION

SCRAM="`which \"\$SCRAM_VERSION\"`"
checkbin "$SCRAM"

echo "Installed CMSSW versions:"
$SCRAM list -c CMSSW | sort | awk '{printf $2" "}'
echo

if ! $SCRAM project CMSSW $SCRAM_PROJECTVERSION; then
	echo "SCRAM project area setup failed" 1>&2
	fail 110
fi

checkdir "SCRAM project area" "$SCRAM_PROJECTVERSION"
cd "$SCRAM_PROJECTVERSION"

if ! [ "$HAS_RUNTIME" = no ]; then

	if [ "$SE_RUNTIME" = yes ]; then
		echo "Rename CMSSW environment package: ${GC_TASK_ID}.tar.gz"
		mv `_find ${GC_TASK_ID}.tar.gz` runtime.tar.gz || fail 101
		export SE_INPUT_FILES="${SE_INPUT_FILES/${GC_TASK_ID}.tar.gz/}"
	fi

	echo "Unpacking CMSSW environment"
	tar xvfz "`_find runtime.tar.gz`" || fail 111
fi

echo "Setup CMSSW environment"
eval `$SCRAM runtime -sh` || fail 112
checkvar "CMSSW_BASE"
checkvar "CMSSW_RELEASE_BASE"
checkbin "cmsRun"
checkbin "edmConfigHash"

# patch python path data
if [ -n "$CMSSW_OLD_RELEASETOP" ]; then
	for INITFILE in `find -iname __init__.py`; do
		echo "Fixing CMSSW path in file: $INITFILE"
		sed -i -e "s@$CMSSW_OLD_RELEASETOP@$CMSSW_RELEASE_BASE@" $INITFILE
	done
fi
echo

echo "---------------------------"
echo
export GC_WORKDIR="`pwd`/workdir"
export CMSSW_SEARCH_PATH="$CMSSW_SEARCH_PATH:$GC_WORKDIR"
mkdir -p "$GC_WORKDIR"; cd "$GC_WORKDIR"
my_move "$GC_SCRATCH" "$GC_WORKDIR" "$SB_INPUT_FILES $SE_INPUT_FILES $CMSSW_PROLOG_SB_IN_FILES $CMSSW_EPILOG_SB_IN_FILES"
echo
echo "==========================="
timestamp "CMSSW_STARTUP" "DONE"

GC_CMSSWRUN_RETCODE=0
# Additional prolog scripts in the CMSSW environment
if [ -n "$CMSSW_PROLOG_EXEC" ]; then
	timestamp "CMSSW_PROLOG1" "START"
	echo "---------------------------"
	echo
	echo "Starting $CMSSW_PROLOG_EXEC with arguments: $CMSSW_PROLOG_ARGS"
	eval "$CMSSW_PROLOG_EXEC $CMSSW_PROLOG_ARGS"
	GC_CMSSWRUN_RETCODE=$?
	echo
	timestamp "CMSSW_PROLOG1" "DONE"
	if [ "$GC_CMSSWRUN_RETCODE" != "0" ]; then
		echo "Prologue $CMSSW_EPILOG_EXEC failed with code: $GC_CMSSWRUN_RETCODE"
		echo "Aborting..."
	fi
fi

echo "---------------------------"
echo
checkdir "CMSSW working directory" "$GC_WORKDIR"

if [ "$GC_CMSSWRUN_RETCODE" == "0" ] && [ -n "$CMSSW_CONFIG" ]; then
	echo "---------------------------"
	echo
	cd "$GC_WORKDIR"
	for CFG_NAME in $CMSSW_CONFIG; do
		CFG_BASENAME="$(basename $CFG_NAME)"
		_CMSRUN_COUNT=1
		timestamp "CMSSW_CMSRUN${_CMSRUN_COUNT}" "START"
		echo "Config file: $CFG_NAME"
		echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
		checkfile "$CFG_NAME"
		DBSDIR="$GC_WORKDIR/cmssw.dbs/$CFG_BASENAME"
		mkdir -p "$DBSDIR"

		echo "Substituting variables..."
		cat "$CFG_NAME" | var_replacer "$CFG_BASENAME" > "$DBSDIR/config"

		echo "Calculating config file hash..."
		(
			echo "# grid-control fix for edmConfigHash"
			echo "import sys, shlex"
			echo "if not hasattr(sys, 'argv'): sys.argv = ['"$CFG_BASENAME"'] + shlex.split('"$@"')"
			echo "####################################"
			cat "$DBSDIR/config"
		) > "$DBSDIR/hash_config" # ensure arguments are forwarded to config file when running edmConfigHash

		cp "$DBSDIR/hash_config" "$CFG_BASENAME"
		edmConfigHash "$CFG_BASENAME" > "$DBSDIR/hash"
		CODE=$?
		if [ "$CODE" != "0" ]; then
			echo "Problem while hashing config file:"
			echo "---------------------------"
			echo "Executing python $CFG_BASENAME (modified for edmConfigHash) ..."
			python "$CFG_BASENAME" 2>&1
			echo "---------------------------"
			CODE=113
			break
		fi

		echo "Starting cmsRun..."
		cp "$DBSDIR/config" "$CFG_BASENAME"
		if [ "$GZIP_OUT" = "yes" ]; then
			(
				echo "Starting cmsRun with config file $CFG_NAME and arguments $@"
				cmsRun -j "$DBSDIR/report.xml" -e "$CFG_BASENAME" $@
				echo $? > "$GC_LANDINGZONE/exitcode.txt"
				echo
				echo "---------------------------"
				echo
			) 2>&1 | gzip -9 > "$CFG_BASENAME.rawlog.gz"
			[ -f "$GC_LANDINGZONE/exitcode.txt" ] && CODE=$(< "$GC_LANDINGZONE/exitcode.txt") && rm -f "$GC_LANDINGZONE/exitcode.txt"
		else 
			cmsRun -j "$DBSDIR/report.xml" -e "$CFG_BASENAME" $@
			CODE=$?
		fi
		[ "$CODE" == "" ] && export CODE="-2"
		echo "cmsRun finished with exit code $CODE"
		echo
		timestamp "CMSSW_CMSRUN${_CMSRUN_COUNT}" "DONE"
		_CMSRUN_COUNT=$[ $_CMSRUN_COUNT +1]
		if [ "$CODE" != "0" ];then
			echo "CMSSW config $CFG_NAME failed with code: $CODE"
			echo "Aborting..."
			break
		fi
	done
	echo -e "CMSSW output on stdout and stderr:\n" | gzip > "00000.rawlog.gz"
	[ "$GZIP_OUT" = "yes" ] && zcat -f *.rawlog.gz | gzip -9 > "cmssw.log.gz"

	# Calculate hash of output files for DBS
	echo "Calculating output file hash..."
	for OUT_NAME in $SE_OUTPUT_FILES; do
		[ -s "$OUT_NAME" ] && cksum "$OUT_NAME" >> "$GC_WORKDIR/cmssw.dbs/files"
	done
	echo "$SCRAM_PROJECTVERSION" > "$GC_WORKDIR/cmssw.dbs/version"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	echo
	(cd "$GC_WORKDIR/cmssw.dbs"; tar cvzf "$GC_WORKDIR/cmssw.dbs.tar.gz" * )
	GC_CMSSWRUN_RETCODE=$CODE
fi

# Additional epilog script in the CMSSW environment
if [ -n "$CMSSW_EPILOG_EXEC" ]; then
	if [ "$GC_CMSSWRUN_RETCODE" == "0" ]; then
		timestamp "CMSSW_EPILOG1" "START"
		echo "---------------------------"
		echo
		echo "Starting $CMSSW_EPILOG_EXEC with arguments: $CMSSW_EPILOG_ARGS"
		eval "$CMSSW_EPILOG_EXEC $CMSSW_EPILOG_ARGS"
		GC_CMSSWRUN_RETCODE=$?
		echo
		timestamp "CMSSW_EPILOG1" "DONE"
		if [ "$GC_CMSSWRUN_RETCODE" != "0" ]; then
			echo "Epilogue $CMSSW_EPILOG_EXEC failed with code: $GC_CMSSWRUN_RETCODE"
			echo "Aborting..."
		fi
	fi
fi

echo
echo "---------------------------"
echo
checkdir "CMSSW working directory after processing" "$GC_WORKDIR"

# Move output into scratch
echo "---------------------------"
echo
my_move "$GC_WORKDIR" "$GC_SCRATCH" "$SB_OUTPUT_FILES $SE_OUTPUT_FILES"

exit $GC_CMSSWRUN_RETCODE
