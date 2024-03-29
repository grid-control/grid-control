#!/bin/bash
# | Copyright 2009-2017 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

# Source: github.com/grid-control

echo "Searching for GRID environment..."

# Limit memory used by java tools
export _JAVA_OPTIONS="-Xms128m -Xmx512m"

gc_find_grid() {
	echo "[GRID] Searching in $1"
	# Save local VO environment variables
	VO_KEEPER="${GC_LANDINGZONE:-/tmp}/env.grid.old"
	VO_REVERT="${GC_LANDINGZONE:-/tmp}/env.grid.new"
	export | grep VO_ | sed -e "s/^.*VO_/VO_/" > "$VO_KEEPER"

	if [ -f "$2" ]; then
		echo "       Found script $2"
		. "$2" # shellcheck source=/dev/null
	else
		echo "       Script '$2' does not exist!"
		return 1
	fi
	if [ -z "$GLITE_LOCATION" ]; then
		echo "       \$GLITE_LOCATION is empty!"
		return 1
	fi

	# We want to keep the local VO environment variables
	export | grep VO_ | sed -e "s/^.*VO_/VO_/;s/=/ /" | while read -r VAR _; do
		if grep -q "$VAR" "$VO_KEEPER"; then
			echo "export $(grep "$VAR" "$VO_KEEPER")"
		else
			echo "export $VAR=''"
		fi
	done > "$VO_REVERT"
	. "$VO_REVERT" # shellcheck source=/dev/null
	rm "$VO_KEEPER" "$VO_REVERT"
	return 0
}

gc_set_proxy() {
	# Use proxy from input sandbox if available
	if [ -s "$GC_SCRATCH/_proxy.dat" ]; then
		mv "$GC_SCRATCH/_proxy.dat" "$GC_LANDINGZONE/_proxy.dat"
		chmod 400 "$GC_LANDINGZONE/_proxy.dat"
		[ ! -s "$X509_USER_PROXY" ] && export X509_USER_PROXY="$GC_LANDINGZONE/_proxy.dat"
	fi
	echo "Using GRID proxy $X509_USER_PROXY"
}

gc_find_os_release() {
	# First try to read /etc/redhat-release, because it should be present on SL6 and CentOS7.
	# Then try to read any /etc/*-release file, except for lsb-release and os-release, because
	# that is a more structured standard, not present for SL6. It could be added as fallback, if
	# necessary.
	# http://linuxmafia.com/faq/Admin/release-files.html
	# https://www.freedesktop.org/software/systemd/man/os-release.html

	if [ -f "/etc/redhat-release" ]; then
		echo $(head -1 /etc/redhat-release)
		return 0
	fi
	for f in $(ls -1 /etc/*-release | grep -v -e "/etc/os-release" -e "/etc/lsb-release"); do
		echo $(head -1 "$f")
		return 0
	done

	# Nothing found. Should not happen on EL systems. If this is somewhere else, we should implement
	# a fallback, e.g., using /etc/os-release.
	return 1
}

if gc_find_os_release | grep -q -e "Scientific Linux.* 6.*" -e "CentOS Linux.* 6.*"; then
	# EL6 installations need a different UI setup script
	GC_GLITE_TRY_CVMFS_LOCATION="/cvmfs/grid.cern.ch/umd-sl6ui-latest/etc/profile.d/setup-ui-example.sh"
else
	# EL7 (and above?) should use this one
	GC_GLITE_TRY_CVMFS_LOCATION="/cvmfs/grid.cern.ch/umd-c7ui-latest/etc/profile.d/setup-c7-ui-example.sh"
fi


if [ -z "$GLITE_LOCATION" ] && [ -d "$GLITE_LOCATION" ]; then
	GC_GLITE_TYPE="LOCAL"
elif gc_find_grid "USER" "$GC_GLITE_LOCATION"; then
	GC_GLITE_TYPE="USER"
elif gc_find_grid "CVMFS" "$GC_GLITE_TRY_CVMFS_LOCATION"; then
	GC_GLITE_TYPE="CVMFS"
elif gc_find_grid "CVMFS - 2nd try" $(ls -1t /cvmfs/grid.cern.ch/*/etc/profile.d/grid*.sh 2> /dev/null | head -n 1); then
	GC_GLITE_TYPE="CVMFS-2"
elif gc_find_grid "OSG" "/uscmst1/prod/grid/gLite_SL5.sh"; then
	GC_GLITE_TYPE="OSG"
elif gc_find_grid "AFS" "/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh"; then
	GC_GLITE_TYPE="AFS"
else
	echo "[WARNING] No GRID environment found!"
	gc_set_proxy # still setting proxy
	return 1
fi
echo "[GRID-$GC_GLITE_TYPE] Using GRID UI $(glite-version 2> /dev/null) located at '$GLITE_LOCATION'"

gc_set_proxy
