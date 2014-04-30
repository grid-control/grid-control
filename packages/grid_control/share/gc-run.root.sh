#!/bin/sh
#-#  Copyright 2010 Karlsruhe Institute of Technology
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

# 110 - ROOT area not found

source $MY_LANDINGZONE/gc-run.lib || exit 101

echo "ROOT module starting"
echo
echo "---------------------------"

export ROOTSYS=$MY_ROOTSYS
export PATH="$PATH:$ROOTSYS/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$ROOTSYS/lib:$ROOTSYS/lib/root:."
echo -n "ROOT Version: "
$ROOTSYS/bin/root-config --version || fail 110
echo "---------------------------"
echo

eval $@
