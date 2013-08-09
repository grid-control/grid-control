#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

# 110 - project area setup failed
# 111 - CMSSW environment unpacking failed
# 112 - CMSSW environment setup failed
# 113 - Problem while hashing config file

source $MY_LANDINGZONE/gc-run.lib || exit 101

echo "CMSSW module starting"
echo
echo "---------------------------"
timestamp "CMSSW_STARTUP" "START"
echo "==========================="

echo "NEventsProcessed=${MAX_EVENTS:-0}" > ${MY_DASHBOARDINFO:-/dev/null}

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
		echo "Rename CMSSW environment package: ${TASK_ID}.tar.gz"
		mv `_find ${TASK_ID}.tar.gz` runtime.tar.gz || fail 101
		export SE_INPUT_FILES="${SE_INPUT_FILES/${TASK_ID}.tar.gz/}"
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
export MY_WORKDIR="`pwd`/workdir"
export CMSSW_SEARCH_PATH="$CMSSW_SEARCH_PATH:$MY_WORKDIR"
mkdir -p "$MY_WORKDIR"; cd "$MY_WORKDIR"
my_move "$MY_SCRATCH" "$MY_WORKDIR" "$SB_INPUT_FILES $SE_INPUT_FILES $CMSSW_PROLOG_EXEC $CMSSW_EPILOG_EXEC"
echo
echo "==========================="
timestamp "CMSSW_STARTUP" "DONE"

# Additional prolog scripts in the CMSSW environment
for CMSSW_BIN in $CMSSW_PROLOG_EXEC; do
	_PROLOG_COUNT=1
	timestamp "CMSSW_PROLOG${_PROLOG_COUNT}" "START"
	echo "---------------------------"
	echo
	echo "Starting $CMSSW_BIN with arguments: $CMSSW_PROLOG_ARGS"
	checkbin "$CMSSW_BIN"
	eval "./$CMSSW_BIN $CMSSW_PROLOG_ARGS"
	CODE=$?
	echo
	timestamp "CMSSW_PROLOG${_PROLOG_COUNT}" "DONE"
	_PROLOG_COUNT=$[ $_PROLOG_COUNT +1]
	if [ "$CODE" != "0" ];then
		echo "Prologue $CMSSW_BIN failed with code: $CODE"
		echo "Aborting..."
		break
	fi
done

echo "---------------------------"
echo
checkdir "CMSSW working directory" "$MY_WORKDIR"

if [ "$CODE" == "0" ] && [ -n "$CMSSW_CONFIG" ]; then
	echo "---------------------------"
	echo
	cd "$MY_WORKDIR"
	for CFG_NAME in $CMSSW_CONFIG; do
		_CMSRUN_COUNT=1
		timestamp "CMSSW_CMSRUN${_CMSRUN_COUNT}" "START"
		echo "Config file: $CFG_NAME"
		echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
		checkfile "$CFG_NAME"
		DBSDIR="$MY_WORKDIR/cmssw.dbs/$CFG_NAME"
		mkdir -p "$DBSDIR"

		echo "Substituting variables..."
		cat "$CFG_NAME" | var_replacer "$CFG_NAME" > "$DBSDIR/config"
		cp "$DBSDIR/config" "$CFG_NAME"

		echo "Calculating config file hash..."
		edmConfigHash "$CFG_NAME" > "$DBSDIR/hash"
		CODE=$?
		if [ "$CODE" != "0" ]; then
			echo "Problem while hashing config file:"
			echo "---------------------------"
			echo "Executing python $CFG_NAME..."
			python "$CFG_NAME" 2>&1
			echo "---------------------------"
			CODE=113
			break
		fi

		echo "Starting cmsRun..."
		if [ "$GZIP_OUT" = "yes" ]; then
			(
				echo "Starting cmsRun with config file $CFG_NAME and arguments $@"
				cmsRun -j "$DBSDIR/report.xml" -e "$CFG_NAME" $@
				echo $? > "$MY_LANDINGZONE/exitcode.txt"
				echo
				echo "---------------------------"
				echo
			) 2>&1 | gzip -9 > "$CFG_NAME.rawlog.gz"
			[ -f "$MY_LANDINGZONE/exitcode.txt" ] && CODE=$(< "$MY_LANDINGZONE/exitcode.txt") && rm -f "$MY_LANDINGZONE/exitcode.txt"
		else 
			cmsRun -j "$DBSDIR/report.xml" -e "$CFG_NAME" $@
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
		[ -s "$OUT_NAME" ] && cksum "$OUT_NAME" >> "$MY_WORKDIR/cmssw.dbs/files"
	done
	echo "$SCRAM_PROJECTVERSION" > "$MY_WORKDIR/cmssw.dbs/version"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	echo
	(cd "$MY_WORKDIR/cmssw.dbs"; tar cvzf "$MY_WORKDIR/cmssw.dbs.tar.gz" * )
fi

# Additional epilog scripts in the CMSSW environment
if [ "$CODE" == "0" ]; then
	for CMSSW_BIN in $CMSSW_EPILOG_EXEC; do
		_EPILOG_COUNT=1
		timestamp "CMSSW_EPILOG${_EPILOG_COUNT}" "START"
		echo "---------------------------"
		echo
		echo "Starting $CMSSW_BIN with arguments: $CMSSW_EPILOG_ARGS"
		checkbin "$CMSSW_BIN"
		eval "./$CMSSW_BIN $CMSSW_EPILOG_ARGS"
		CODE=$?
		echo
		timestamp "CMSSW_EPILOG${_EPILOG_COUNT}" "DONE"
		_EPILOG_COUNT=$[ $_EPILOG_COUNT +1]
		if [ "$CODE" != "0" ];then
			echo "Epilogue $CMSSW_BIN failed with code: $CODE"
			echo "Aborting..."
			break
		fi
	done
fi

echo
echo "---------------------------"
echo
checkdir "CMSSW working directory after processing" "$MY_WORKDIR"

# Move output into scratch
echo "---------------------------"
echo
my_move "$MY_WORKDIR" "$MY_SCRATCH" "$SB_OUTPUT_FILES $SE_OUTPUT_FILES"

exit $CODE
