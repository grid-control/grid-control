#!/bin/bash
#-#  Copyright 2009-2015 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

echo "Searching for GRID environment..."

# Limit memory used by java tools
export _JAVA_OPTIONS="-Xms128m -Xmx512m"

function gc_find_grid() {
	echo "[GRID] Searching in $1"
	# Save local VO environment variables
	VO_KEEPER="${GC_LANDINGZONE:-/tmp}/env.grid.old"
	VO_REVERT="${GC_LANDINGZONE:-/tmp}/env.grid.new"
	export | grep VO_ | sed -e "s/^.*VO_/VO_/" > "$VO_KEEPER"

	if [ -f "$2" ]; then
		source "$2"
	fi
	if [ -z "$GRID_LOCATION" ]; then
		return 1
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
	return 0
}

if [ -n "$GRID_LOCATION" ]; then
	echo -n "[GRID-LOCAL] "
elif gc_find_grid "CVMFS" $(ls -1t /cvmfs/grid.cern.ch/*/etc/profile.d/setup*.sh 2> /dev/null | head -n 1); then
	echo -n "[GRID-CVMFS] "
elif gc_find_grid "CVMFS - 2nd try" $(ls -1t /cvmfs/grid.cern.ch/*/etc/profile.d/grid*.sh 2> /dev/null | head -n 1); then
	echo -n "[GRID-CVMFS] "
elif gc_find_grid "OSG" "/uscmst1/prod/grid/gLite_SL5.sh"; then
	echo -n "[GRID-OSG] "
elif gc_find_grid "AFS" "/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh"; then
	echo -n "[GRID-AFS] "
else
	echo "[WARNING] No GRID environment found!"
	return 1
fi
echo "Using GRID UI `glite-version 2> /dev/null` at $GLITE_LOCATION"

# Use proxy from input sandbox if available
if [ -s "$GC_SCRATCH/_proxy.dat" ]; then
	mv "$GC_SCRATCH/_proxy.dat" "$GC_LANDINGZONE/_proxy.dat"
	chmod 400 "$GC_LANDINGZONE/_proxy.dat"
	[ ! -s "$X509_USER_PROXY" ] && export X509_USER_PROXY="$GC_LANDINGZONE/_proxy.dat"
fi
echo "Using GRID proxy $X509_USER_PROXY"
