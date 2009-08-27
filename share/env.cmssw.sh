#!/bin/bash

echo "Searching for CMSSW environment..."
if [ -d "/wlcg/sw/cms/experimental" ]; then
	export VO_CMS_SW_DIR="/wlcg/sw/cms/experimental"
	echo "[EKP-SITE] Using CMSSW $VO_CMS_SW_DIR"
elif [ -d "/software/kit/bd00/CMSSW" ]; then
	export VO_CMS_SW_DIR="/software/kit/bd00/CMSSW"
	echo "[IC1-SITE] Using CMSSW $VO_CMS_SW_DIR"
fi

# (VO_CMS_SW_DIR == "") => get from CMSSW_OLD_RELEASETOP
if [ -n "$CMSSW_OLD_RELEASETOP" -a -d "$CMSSW_OLD_RELEASETOP" ]; then
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
