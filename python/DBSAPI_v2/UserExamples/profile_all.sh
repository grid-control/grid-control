#!/bin/sh
#
all=`ls -1 dbsList*.py`
for afile in $all; do
   echo "Profiling $afile"
   time python $afile 
done;
#     

