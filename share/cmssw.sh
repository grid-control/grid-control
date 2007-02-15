#!/bin/sh

echo "CMSSW module starting"
echo "---------------------"

source config.sh

if ! test -f "$MY_SCRATCH/runtime.tar.gz"; then
	echo "runtime.tar.gz not found" 2>&1
	exit 1
fi

if ! [ -n "$VO_CMS_SW_DIR" ]; then
	echo VO_CMS_SW_DIR undefined 2>&1
	exit 1
fi

if [ ! -f "$VO_CMS_SW_DIR/cmsset_default.sh" ]; then
	echo "$VO_CMS_SW_DIR/cmsset_default.sh" not found 2>&1
	exit 1
fi

source "$VO_CMS_SW_DIR/cmsset_default.sh"

SCRAM="`which \"\$SCRAM_VERSION\"`"
if [ -z "$SCRAM" ]; then
	echo "$SCRAM_VERSION not found" 2>&1
	exit 1
fi

if ! $SCRAM project CMSSW $SCRAM_PROJECTVERSION; then
	echo "SCRAM project area setup failed" 2>&1
	exit 1
fi

if ! test -d "$SCRAM_PROJECTVERSION"; then
	echo "SCRAM project area not found" 2>&1
	exit 1
fi

cd "$SCRAM_PROJECTVERSION"
tar xvfz $MY_SCRATCH/runtime.tar.gz

eval `$SCRAM runtime -sh`

echo "---------------------------"

ls -l

exit 0
