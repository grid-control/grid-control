#!/bin/bash
# 110 - project area setup failed
# 111 - CMSSW environment unpacking failed
# 112 - CMSSW environment setup failed

source $MY_LANDINGZONE/run.lib || exit 101

echo "CMSSW module starting"
echo "---------------------------"

checkfile "$MY_SCRATCH/_config.sh"
source "$MY_SCRATCH/_config.sh"

if [ "$DASHBOARD" == "yes" ]; then
	export REPORTID="taskId=$TASK_ID jobId=${MY_JOBID}_$GLITE_WMS_JOBID MonitorID=$TASK_ID MonitorJobID=${MY_JOBID}_$GLITE_WMS_JOBID"
	echo "Update Dashboard: $REPORTID"
	checkfile "$MY_SCRATCH/report.py"
	chmod u+x "$MY_SCRATCH/report.py"
	checkbin "$MY_SCRATCH/report.py"
	export
	echo $MY_SCRATCH/report.py $REPORTID \
		SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
		WNname="$(hostname -f)" ExeStart="cmsRun"
	$MY_SCRATCH/report.py $REPORTID \
		SyncGridJobId="$GLITE_WMS_JOBID" SyncGridName="$TASK_USER" SyncCE="$GLOBUS_CE" \
		WNname="$(hostname -f)" ExeStart="cmsRun"
fi

WALL_START="$(date +%s)"

MAX_EVENTS="$1"
SKIP_EVENTS="$2"
FILE_NAMES="\"$3\""
shift 3
for i in "$@"; do
	FILE_NAMES="$FILE_NAMES, \"$i\""
done

if [ -z "$VO_CMS_SW_DIR" -a -n "$OSG_APP" ]; then
	export VO_CMS_SW_DIR="$OSG_APP/cmssoft/cms"
	echo "[OSG-SITE] Using $VO_CMS_SW_DIR"
elif [ -z "$VO_CMS_SW_DIR" -a -d "/afs/cern.ch/cms/sw" ]; then
	export VO_CMS_SW_DIR="/afs/cern.ch/cms/sw"
	echo "[AFS-SITE] Using $VO_CMS_SW_DIR"
elif [ -z "$VO_CMS_SW_DIR" -a -d "/wlcg/sw/cms" ]; then
	export VO_CMS_SW_DIR="/wlcg/sw/cms"
	echo "[WLCG-SITE] Using $VO_CMS_SW_DIR"
fi

checkvar "VO_CMS_SW_DIR"
checkfile "$VO_CMS_SW_DIR/cmsset_default.sh"

saved_SCRAM_VERSION="$SCRAM_VERSION"
saved_SCRAM_ARCH="$SCRAM_ARCH"
source "$VO_CMS_SW_DIR/cmsset_default.sh"
SCRAM_VERSION="$saved_SCRAM_VERSION"
export SCRAM_ARCH="$saved_SCRAM_ARCH"

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
	fi
	
	echo "Unpacking CMSSW environment"
	tar xvfz "`_find runtime.tar.gz`" || fail 111
fi

echo "Setup CMSSW environment"
eval `$SCRAM runtime -sh` || fail 112
checkvar "CMSSW_BASE"
checkvar "CMSSW_RELEASE_BASE"
checkbin "cmsRun"

# additional setup of the CMSSW environment
SETUP_CMSSW="`_find setup.sh`"
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
checkdir "CMSSW working directory" "$MY_WORKDIR"
echo "---------------------------"
for i in $CMSSW_CONFIG; do
	echo -e "\nConfig file: $i"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	var_replacer "" < "`_find $i`" | tee "CMSRUN-$i"
	if [ "$GZIP_OUT" = "yes" ]; then
		( cmsRun "CMSRUN-$i"; echo $? > exitcode.txt ) 2>&1 | gzip -9 > cmssw_out.txt.gz
		[ -f "exitcode.txt" ] && CODE=$(<exitcode.txt) && rm -f exitcode.txt
	else 
		cmsRun "CMSRUN-$i"
		CODE=$?
	fi
done

echo "---------------------------"
checkdir "CMSSW working directory after cmsRun" "$MY_WORKDIR"

# Move output into scratch
my_move "$MY_WORKDIR" "$MY_SCRATCH" "$SB_OUTPUT_FILES $SE_OUTPUT_FILES"

if [ "$DASHBOARD" == "yes" ]; then
	echo "Update Dashboard: $REPORTID"
	checkbin "$MY_SCRATCH/report.py"
	echo $MY_SCRATCH/report.py $REPORTID \
		ExeEnd="cmsRun" WCCPU="$[ $(date +%s) - $WALL_START ]" \
		ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE"
	$MY_SCRATCH/report.py $REPORTID \
		ExeEnd="cmsRun" WCCPU="$[ $(date +%s) - $WALL_START ]" \
		ExeExitCode="$CODE" JobExitCode="$CODE" JobExitReason="$CODE"
fi

exit $CODE
