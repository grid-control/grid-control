#!/bin/sh

source $MY_LANDINGZONE/gc-run.lib || exit 101

echo "HERWIG++ module starting"
echo
echo "---------------------------"

# This script uses the standalone Herwig++ which is distributed
# together with CMSSW ... so first find the CMSSW directory
checkvar "VO_CMS_SW_DIR"
export SW_CMSSW_EXT="$VO_CMS_SW_DIR/slc4_ia32_gcc345/external"

# Dir shortcuts to all Herwig++ dependencies
export SW_HERWIGPP="$(ls -d $SW_CMSSW_EXT/herwigpp/2.3.2-* | tail -n 1)"
export SW_THEPEG="$(ls -d $SW_CMSSW_EXT/thepeg/1.4.2-* | tail -n 1)"
export SW_LHA="$(ls -d $SW_CMSSW_EXT/lhapdf/5.6.0-* | tail -n 1)"
export SW_HEPMC="$(ls -d $SW_CMSSW_EXT/hepmc/2.03.* | tail -n 1)"
export SW_GSL="$SW_CMSSW_EXT/gsl/1.10-cms"
export SW_GCCLIB="$SW_CMSSW_EXT/gcc/3.4.5-CMS19"

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
	cat $FILE
	echo "Reading $FILE"
	Herwig++ read $FILE
	echo "Running " *.run
	Herwig++ run --seed $RANDOM *.run $HW_PARA || exit $?
	echo
	rm *.run
done
echo
