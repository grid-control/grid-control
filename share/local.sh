#!/bin/bash

# TODO: Find out from CMSSW_RELEASE_BASE_OLD ?
case "$HOSTNAME" in
	"rz-ic"*)
		export VO_CMS_SW_DIR=/wlcg/sw/cms
		;;
	"ic1"*)
		export VO_CMS_SW_DIR=/software/kit/bd00/CMSSW
		;;
	"ekp"*)
		export VO_CMS_SW_DIR=/wlcg/sw/cms/experimental
		;;
	"lxplus"*)
		# VO_CMS_SW_DIR already set
		;;
	*)
		;;
esac

cd $SANDBOX
./grid.sh $ARGS
