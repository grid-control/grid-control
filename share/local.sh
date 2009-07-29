#!/bin/bash

# (VO_CMS_SW_DIR == "") => get from CMSSW_OLD_RELEASETOP
if [ -d "/wlcg/sw/cms/experimental" ]; then
	export VO_CMS_SW_DIR="/wlcg/sw/cms/experimental"
	echo "[EKP-SITE] Using $VO_CMS_SW_DIR"
elif [ -d "/software/kit/bd00/CMSSW" ]; then
	export VO_CMS_SW_DIR="/software/kit/bd00/CMSSW"
	echo "[IC1-SITE] Using $VO_CMS_SW_DIR"
fi

SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
mkdir scratch
export SCRATCH_DIRECTORY="$SANDBOX/scratch"
source _jobconfig.sh
./run.sh $ARGS
cd $SANDBOX
rmdir scratch
