#!/bin/sh

echo "HERWIG++ module starting"
echo
echo "---------------------------"

# This script uses the standalone Herwig++ which is distributed
# together with CMSSW ... so first find the CMSSW directory
if [ -d "$CMSSW_OLD_RELEASETOP" ]; then
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

export SW_CMSSW_EXT="$VO_CMS_SW_DIR/slc4_ia32_gcc345/external"

# Dir shortcuts to all Herwig++ dependencies
export SW_HERWIGPP="$SW_CMSSW_EXT/herwigpp/2.3.2-cms"
export SW_THEPEG="$SW_CMSSW_EXT/thepeg/1.4.2"
export SW_LHA="$SW_CMSSW_EXT/lhapdf/5.6.0-cms2"
export SW_GSL="$SW_CMSSW_EXT/gsl/1.10-cms"
export SW_GCCLIB="$SW_CMSSW_EXT/gcc/3.4.5-CMS19"
export SW_HEPMC="$SW_CMSSW_EXT/hepmc/2.03.06-cms3"

# Set all the necessary lib paths for Herwig++ 
export ThePEG_INSTALL_PATH="$SW_THEPEG/lib/ThePEG"
export PATH="$PATH:$SW_THEPEG/bin:$SW_HERWIGPP/bin:$SW_LHA/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$SW_THEPEG/lib/ThePEG:$SW_HERWIGPP/lib/Herwig++"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$SW_LHA/lib:$SW_GSL/lib:$SW_GCCLIB/lib:$SW_HEPMC/lib"

cp $SW_HERWIGPP/share/Herwig++/HerwigDefaults.rpo .

HW_PARA=""
[ -n "$1" ] && HW_PARA="-N $1"
[ -n "$EVENTS" ] && HW_PARA="-N $EVENTS"

for FILE in $SUBST_FILES; do
	echo
	echo "Running over file $FILE"
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	cat $FILE | uniq > tmp.$FILE
	mv tmp.$FILE $FILE
	echo

	echo "Reading $FILE"
	Herwig++ read $FILE
	echo "Running " *.run
	Herwig++ run *.run $HW_PARA || exit $?
	echo
	rm *.run
done
echo
