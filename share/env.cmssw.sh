#!/bin/bash

echo "Searching for CMSSW environment..."
# Fix for sites with overlay setup - overwrites defaults
if [ -d "/wlcg/sw/cms/experimental" ]; then
	export VO_CMS_SW_DIR="/wlcg/sw/cms/experimental"
	echo "[EXP-SITE] Using $VO_CMS_SW_DIR"
fi

# Force directories known during job init / development
if [ -n "$CMSSW_DIR_UI" -a -d "$CMSSW_DIR_UI" ]; then
	export VO_CMS_SW_DIR="$CMSSW_DIR_UI"
	echo "[UI-SITE] Using $VO_CMS_SW_DIR"
elif [ -n "$CMSSW_DIR_PRO" -a -d "$CMSSW_DIR_PRO" ]; then
	export VO_CMS_SW_DIR="$CMSSW_DIR_PRO"
	echo "[PROJ-SITE] Using $VO_CMS_SW_DIR"
fi

# Fallback - try different default values for CMSSW directory
if [ -z "$VO_CMS_SW_DIR" ]; then
	if [ -d "/software/kit/bd00/CMSSW" ]; then
		export VO_CMS_SW_DIR="/software/kit/bd00/CMSSW"
		echo "[IC1-SITE] Using $VO_CMS_SW_DIR"
	elif [ -d "/wlcg/sw/cms" ]; then
		export VO_CMS_SW_DIR="/wlcg/sw/cms"
		echo "[WLCG-SITE] Using $VO_CMS_SW_DIR"
	elif [ -n "$OSG_APP" ]; then
		export VO_CMS_SW_DIR="$OSG_APP/cmssoft/cms"
		echo "[OSG-SITE] Using $VO_CMS_SW_DIR"
	elif [ -d "/afs/cern.ch/cms/sw" ]; then
		export VO_CMS_SW_DIR="/afs/cern.ch/cms/sw"
		echo "[AFS-SITE] Using $VO_CMS_SW_DIR"
	fi
fi
