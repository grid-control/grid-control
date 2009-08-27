#!/bin/bash

echo "Searching for gLite environment..."
if [ -z "$GLITE_LOCATION" -a -d "/wlcg/sw/UI_glite-3_1" ]; then
	source "/wlcg/sw/UI_glite-3_1/external/etc/profile.d/grid-env.sh"
	echo "[WLCG-SITE] Using gLite `glite-version`"
elif [ -z "$GLITE_LOCATION" -a -d "/afs/desy.de/project/glite" ]; then
	source "/afs/desy.de/project/glite/UI/etc/profile.d/grid-env.sh"
	echo "[AFS-SITE] Using gLite `glite-version`"
fi
