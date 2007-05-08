#!/bin/sh

tmpdir=`date +%m%h%s`
mkdir -p $tmpdir
cp dbsCgiRead.py  $tmpdir
cd $tmpdir

tstamp=`date +%m%y%d%M%S`
result_file=$tstamp.TEST_AVERAGE.txt
SERVER_DESC="Server:CMSCGI,DB:ORACLE-MCGlobal/Writer,Client:http-cmssrv17"

calculateAverage()
{
   # $1: First argument is number of parallel clients
   # $2: second argument is the code needs to be run
   # $3: number of files
   # $4: number of iterations
   # generates output in $2.$3.$4.timelog.txt
   # The average is printed out in $result_file
   # remove older log file if any, otherwise calculations will be wrong
   timeLog=$1.$2.timelog.txt
   rm -f $timeLog
   for cycle in 1 2 3 ; do
     echo "CYCLE $cycle" >> $timeLog

     echo "Time for $1 parallel clients running $2" >> $timeLog
     for i in `seq 1 $1` ; do
       { time python $2 MCGlobal/Writer > $1.readLog.$i.$cycle 2>&1 ; } 2>> $timeLog  &
     done
     # Give enough time to finish (100 sec ?)
     sleep 30
     #wait
   done
   maverage=`cat $timeLog |grep real| awk '{print $2}'|awk -Fm '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg }'`
   saverage=`cat $timeLog |grep real| awk '{print $2}'|awk -Fm '{print $2}'|awk -Fs '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg}'`
   echo "Average Time Taken By $SERVER_DESC reading 1000 files at a time, is: $maverage MINS : $saverage SECS for $1 parallel clients" >> $result_file
   echo "Average Time Taken By $SERVER_DESC reading 1000 files at a time, is: $maverage MINS : $saverage SECS for $1 parallel clients" | mail -s "DBS READ Test Complete" anzar@fnal.gov
}


date=`date`
echo "Test Starting at $date" >> $result_file 
# In one Client inserting the data that will be used for testing the read operations
#python dbsStressTest4READ.py 1000 1 dbsStressTest4Read  > dbsStressTest4Read_Insert.log 2>&1

echo "10 parallel clients: each reading 1000 files from the database in one go" >> $result_file
calculateAverage 10 dbsCgiRead.py
echo "15 parallel clients: each reading 1000 files from the database in one go" >> $result_file
calculateAverage 15 dbsCgiRead.py
echo "20 parallel clients: each reading 1000 files from the database in one go" >> $result_file
calculateAverage 20 dbsCgiRead.py
echo "30 parallel clients: each reading 1000 files from the database in one go" >> $result_file
calculateAverage 30 dbsCgiRead.py
echo "40 parallel clients: each reading 1000 files from the database in one go" >> $result_file
calculateAverage 40 dbsCgiRead.py
echo "50 parallel clients: each reading 1000 files from the database in one go" >> $result_file
calculateAverage 50 dbsCgiRead.py

date=`date`
echo "Test Finishing at $date" >> $result_file

cat $result_file | mail -s "Time Profile Test Done" anzar@fnal.gov 
echo "DONE"
cd -

