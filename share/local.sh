#!/bin/bash

# (VO_CMS_SW_DIR == "") => get from CMSSW_OLD_RELEASETOP
[ -n "$CMSSW_OLD_RELEASETOP" ] && export VO_CMS_SW_DIR=""
SANDBOX=${SANDBOX:-$1}
cd $SANDBOX
mkdir scratch
export SCRATCH_DIRECTORY="$SANDBOX/scratch"
source _jobconfig.sh
./run.sh $ARGS
cd $SANDBOX
rmdir scratch
