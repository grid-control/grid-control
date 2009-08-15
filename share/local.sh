#!/bin/bash

# (VO_CMS_SW_DIR == "") => get from CMSSW_OLD_RELEASETOP

if [ -d "/wlcg/sw/cms/experimental" ]; then
	export VO_CMS_SW_DIR="/wlcg/sw/cms/experimental"
	echo "[EKP-SITE] Using CMSSW $VO_CMS_SW_DIR"
elif [ -d "/software/kit/bd00/CMSSW" ]; then
	export VO_CMS_SW_DIR="/software/kit/bd00/CMSSW"
	echo "[IC1-SITE] Using CMSSW $VO_CMS_SW_DIR"
fi

if [ -z "$GLITE_LOCATION" -a -d "/wlcg/sw/UI_glite-3_1" ]; then
	source "/wlcg/sw/UI_glite-3_1/external/etc/profile.d/grid-env.sh"
	echo "[WLCG-SITE] Using gLite `glite-version`"
elif [ -z "$GLITE_LOCATION" -a -d "/afs/desy.de/project/glite" ]; then
	source "/afs/desy.de/project/glite/UI/etc/profile.d/grid-env.sh"
	echo "[AFS-SITE] Using gLite `glite-version`"
fi

SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
mkdir scratch
export GC_SCRATCH="$SANDBOX/scratch"
cat "$SANDBOX/_jobconfig.sh"
source "$SANDBOX/_jobconfig.sh"
./run.sh $ARGS
cd $SANDBOX
rmdir scratch
