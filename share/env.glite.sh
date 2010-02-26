#!/bin/bash

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

echo "Searching for gLite environment..."

if [ -z "$GLITE_LOCATION" ]; then
	# Save local VO environment variables
	VO_KEEPER="$MY_LANDINGZONE/env.glite.old"
	VO_REVERT="$MY_LANDINGZONE/env.glite.new"
	export | grep VO_ | sed -e "s/^.*VO_/VO_/" > "$VO_KEEPER"

	cat $VO_KEEPER
	# Source UI
	if [ -d "/wlcg/sw/UI_glite-3_1" ]; then
		source "/wlcg/sw/UI_glite-3_1/external/etc/profile.d/grid-env.sh"
		echo "[WLCG-SITE] Using gLite `glite-version`"
	elif [ -d "/afs/desy.de/project/glite" ]; then
		source "/afs/desy.de/project/glite/UI/etc/profile.d/grid-env.sh"
		echo "[AFS-SITE] Using gLite `glite-version`"
	fi

	# We want to keep the local VO environment variables
	export | grep VO_ | sed -e "s/^.*VO_/VO_/;s/=/ /" | while read VAR VALUE; do
		if [ $(grep $VAR "$VO_KEEPER") ]; then
			echo export $(grep $VAR "$VO_KEEPER")
		else
			echo "export $VAR=''"
		fi
	done > "$VO_REVERT"
	source "$VO_REVERT"
	rm "$VO_KEEPER" "$VO_REVERT"
fi
