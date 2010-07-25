#!/bin/sh

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

# 110 - ROOT area not found

source $MY_LANDINGZONE/run.lib || exit 101

echo "ROOT module starting"
echo
echo "---------------------------"

#[ -z "$ROOTSYS" -a -n "$SW_ROOTPATH" ] && export ROOTSYS=$SW_ROOTPATH
#[ -z "$ROOTSYS" -a -n "$VO_CMS_SW_DIR" ] && export ROOTSYS=$(ls -d $VO_CMS_SW_DIR/*/lcg/root/* | tail -n1)

export ROOTSYS=$MY_ROOTSYS
export PATH="$PATH:$ROOTSYS/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$ROOTSYS/lib:$ROOTSYS/lib/root:."
echo -n "ROOT Version: "
$ROOTSYS/bin/root-config --version
echo "---------------------------"
echo

eval $@
