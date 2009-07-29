#!/bin/bash
# 110 - project area setup failed
# 111 - CMSSW environment unpacking failed
# 112 - CMSSW environment setup failed

source $MY_LANDINGZONE/run.lib || exit 101

echo "CMSSW module starting"
echo
echo "---------------------------"

MAX_EVENTS="$1"
SKIP_EVENTS="$2"
if [ -n "$3" ]; then
	FILE_NAMES="\"$3\""
	shift 3
	for i in "$@"; do
		FILE_NAMES="$FILE_NAMES, \"$i\""
	done
fi

echo "NEventsProcessed=$MAX_EVENTS" > $MY_DASHBOARDINFO

if [ -d "$CMSSW_OLD_RELEASETOP" ]; then
	export VO_CMS_SW_DIR="$(cd $CMSSW_OLD_RELEASETOP/../../../../; pwd)"
	echo "[LOCAL-SITE] Using $VO_CMS_SW_DIR"
elif [ -z "$VO_CMS_SW_DIR" -a -d "/wlcg/sw/cms" ]; then
	export VO_CMS_SW_DIR="/wlcg/sw/cms"
	echo "[WLCG-SITE] Using $VO_CMS_SW_DIR"
elif [ -z "$VO_CMS_SW_DIR" -a -n "$OSG_APP" ]; then
	export VO_CMS_SW_DIR="$OSG_APP/cmssoft/cms"
	echo "[OSG-SITE] Using $VO_CMS_SW_DIR"
elif [ -z "$VO_CMS_SW_DIR" -a -d "/afs/cern.ch/cms/sw" ]; then
	export VO_CMS_SW_DIR="/afs/cern.ch/cms/sw"
	echo "[AFS-SITE] Using $VO_CMS_SW_DIR"
fi

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
# Do variable substitutions
for SFILE in $CMSSW_CONFIG; do
	echo
	echo "Substitute variables in file $SFILE"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	var_replacer "$SFILE" < "$MY_SCRATCH/$SFILE" | tee "$MY_WORKDIR/$SFILE"
done

echo
echo "---------------------------"
echo
for CFG_NAME in $CMSSW_CONFIG; do
	echo -e "Starting cmsRun with config file: $CFG_NAME"
	FWK_NAME="`echo $CFG_NAME | sed -e 's/\(.*\)\.\([^\.]*\)/\1.xml/'`"

	if [ "$GZIP_OUT" = "yes" ]; then
		( cmsRun -j "$FWK_NAME" -e "$CFG_NAME"; echo $? > exitcode.txt ) 2>&1 | gzip -9 > cmssw_out.txt.gz
		[ -f "exitcode.txt" ] && CODE=$(<exitcode.txt) && rm -f exitcode.txt
	else 
		cmsRun -j "$FWK_NAME" -e "$CFG_NAME"
		CODE=$?
	fi
	[ -f "$FWK_NAME" ] && gzip "$FWK_NAME"
done

echo
echo "---------------------------"
echo
checkdir "CMSSW working directory after cmsRun" "$MY_WORKDIR"

# Move output into scratch
echo "---------------------------"
echo
my_move "$MY_WORKDIR" "$MY_SCRATCH" "$SB_OUTPUT_FILES $SE_OUTPUT_FILES"

exit $CODE
