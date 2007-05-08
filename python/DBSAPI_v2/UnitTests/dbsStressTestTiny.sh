#!/bin/sh

tstamp=`date +%m%y%d%M%S`
result_file=$tstamp.TEST_AVERAGE.txt


calculateAverage() 
{
   # $1: First argument is number of parallel clients
   # $2: second argument is the code needs to be run
   # $3: number of files
   # $4: number of iterations
   # generates output in $2.$3.$4.timelog.txt
   # The average is printed out in $result_file
   # remove older log file if any, otherwise calculations will be wrong  
   timeLog=$1.$2.$3.$4.timelog.txt  
   rm -f $timeLog
   for cycle in 1 2 3 ; do
     echo "CYCLE $cycle" >> $timeLog
     
     echo "Time for $1 parallel clients running $2" >> $timeLog
     for i in `seq 1 $1` ; do
       { time python $2 $3 $4 $timeLog.$i.$cycle > /dev/null 2>&1 ; } 2>> $timeLog  & 
     done
     # Give enough time to finish (100 sec ?)
     wait
     #sleep 600
   done
   maverage=`cat $timeLog |grep real| awk '{print $2}'|awk -Fm '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg }'` 
   saverage=`cat $timeLog |grep real| awk '{print $2}'|awk -Fm '{print $2}'|awk -Fs '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg}'`
   echo "Average Time Taken By $2 with $3 files in $4 iterations is: $maverage MINS : $saverage SECS" >> $result_file
}

date=`date`
echo "Test Starting at $date" >> $result_file

# 10 parallel clients: each inserting 1000 files, 1 at a time
echo "10 parallel clients: each inserting 100 files, 100 at a time" >> $result_file
calculateAverage 10 dbsStressTest.py 1 100

date=`date`
echo "Test Finishing at $date" >> $result_file

cat $result_file | mail -s "Time Profile Test Done" anzar@fnal.gov
echo "DONE"

