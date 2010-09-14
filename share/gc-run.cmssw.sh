#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

# 110 - project area setup failed
# 111 - CMSSW environment unpacking failed
# 112 - CMSSW environment setup failed

source $MY_LANDINGZONE/gc-run.lib || exit 101

echo "CMSSW module starting"
echo
echo "---------------------------"

echo "NEventsProcessed=${MAX_EVENTS:-0}" > $MY_DASHBOARDINFO

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

# additional setup of the CMSSW environment
SETUP_CMSSW="`_find _setup.sh`"
if [ -f "$SETUP_CMSSW" ]; then
	echo -e "Found setup script: \"$SETUP_CMSSW\""
	cat "$SETUP_CMSSW"
	checkbin "$SETUP_CMSSW"
	eval "$SETUP_CMSSW"
fi

export MY_WORKDIR="`pwd`/workdir"
export CMSSW_SEARCH_PATH="$CMSSW_SEARCH_PATH:$MY_WORKDIR"
mkdir -p "$MY_WORKDIR"

my_move "$MY_SCRATCH" "$MY_WORKDIR" "$SE_INPUT_FILES"

cd "$MY_WORKDIR"

echo
echo "---------------------------"
echo
checkdir "CMSSW working directory" "$MY_WORKDIR"

echo "---------------------------"
echo
for CFG_NAME in $CMSSW_CONFIG; do
	echo "Config file: $CFG_NAME"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	DBSDIR="$MY_WORKDIR/cmssw.dbs/$CFG_NAME"
	mkdir -p "$DBSDIR"

	echo "Substituting variables..."
	cat "$MY_SCRATCH/$CFG_NAME" | var_replacer "$CFG_NAME" | tee "$DBSDIR/config" > "$CFG_NAME"

	echo "Calculating config file hash..."
	edmConfigHash "$CFG_NAME" > "$DBSDIR/hash"

	echo "Starting cmsRun..."
	if [ "$GZIP_OUT" = "yes" ]; then
		(
			echo "Starting cmsRun with config file: $CFG_NAME"
			cmsRun -j "$DBSDIR/report.xml" -e "$CFG_NAME"
			echo $? > exitcode.txt
			echo
			echo "---------------------------"
			echo
		) 2>&1 | gzip -9 > "$CFG_NAME.log.gz"
		[ -f "exitcode.txt" ] && CODE=$(<exitcode.txt) && rm -f exitcode.txt
	else 
		cmsRun -j "$DBSDIR/report.xml" -e "$CFG_NAME"
		CODE=$?
	fi
	echo "cmsRun finished with exit code $CODE"
	echo
	[ "$CODE" != "0" ] && break
done
echo -e "CMSSW output on stdout and stderr:\n" | gzip > "00000.log.gz"
[ "$GZIP_OUT" = "yes" ] && zcat -f *.log.gz | gzip -9 > "cmssw.log.gz"

# Calculate hash of output files for DBS
echo "Calculating output file hash..."
for OUT_NAME in $SE_OUTPUT_FILES; do
	[ -s "$OUT_NAME" ] && cksum "$OUT_NAME" >> "$MY_WORKDIR/cmssw.dbs/files"
done
echo "$SCRAM_PROJECTVERSION" > "$MY_WORKDIR/cmssw.dbs/version"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo
(cd "$MY_WORKDIR/cmssw.dbs"; tar cvzf "$MY_WORKDIR/cmssw.dbs.tar.gz" * )

echo
echo "---------------------------"
echo
checkdir "CMSSW working directory after cmsRun" "$MY_WORKDIR"

# Move output into scratch
echo "---------------------------"
echo
my_move "$MY_WORKDIR" "$MY_SCRATCH" "$SB_OUTPUT_FILES $SE_OUTPUT_FILES"

exit $CODE
