#!/bin/sh

calculateAverage() 
{
   # $1: First argument is number of parallel clients
   # $2: second argument is the API call needs to be run
   # generates output in $2.timelog.txt
   # The average is printed out in TEST_AVERAGE.txt
   
   # remove older log file if any, otherwise calculations will be wrong  
   rm -f $2.timelog.txt
   for cycle in 1 2 3 ; do
     echo "CYCLE $cycle" >> $2.timelog.txt
     
     echo "Time for $1 parallel clients running $2" >> $2.timelog.txt
     for i in `seq 1 10` ; do
       { time python $2 > /dev/null 2>&1 ; } 2>> $2.timelog.txt  & 
     done
     # Give enough time to finish (100 sec ?)
     sleep 100
   done
   average=`cat $2.timelog.txt |grep real| awk '{print $2}'|awk -F0m '{print $2}'|awk -Fs '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg}'`
   echo "Average Time Take By $2 is: $average" >> TEST_AVERAGE.txt

}

date=`date`
echo "Test Starting at $date" >> TEST_AVERAGE.txt

# 10 parallel clients
calculateAverage 10 dbsListPrimaryDatasets.py
calculateAverage 10 dbsListAlgorithm.py
calculateAverage 10 dbsListProcessedDatasets.py
calculateAverage 10 dbsListTiers.py 
calculateAverage 10 dbsListBlocks.py 
calculateAverage 10 dbsListRuns.py 
calculateAverage 10 dbsListFiles.py

cat *.timelog.txt | mail -s "Time Profile Test Done" anzar@fnal.gov
echo "DONE"

