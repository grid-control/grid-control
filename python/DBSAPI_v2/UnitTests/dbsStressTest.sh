#!/bin/sh

tstamp=`date +%m%y%d%M%S`
result_file=$tstamp.TEST_AVERAGE.txt
SERVER_DESC="Server:cmssrv17,DB:Oracle-cmsclad,Client:http-cmssrv17"

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
       { time python $2 $3 $4 $1.$3.$4.$i.$cycle > /dev/null 2>&1 ; } 2>> $timeLog  &
     done
     # Give enough time to finish (100 sec ?)
     #sleep 1000
     wait
   done
   maverage=`cat $timeLog |grep real| awk '{print $2}'|awk -Fm '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg }'`
   saverage=`cat $timeLog |grep real| awk '{print $2}'|awk -Fm '{print $2}'|awk -Fs '{sum = sum + $1} END {print sum}'| awk '{avg = $1/30} END {print avg}'`
   echo "Average Time Taken By $SERVER_DESC inserting 1000 files, $3 at a time, is: $maverage MINS : $saverage SECS for $1 parallel clients" >> $result_file
   echo "Average Time Taken By $SERVER_DESC inserting 1000 files, $3 at a time, is: $maverage MINS : $saverage SECS for $1 parallel clients" |mail -s "DBS Test Complete" anzar@fnal.gov
}


date=`date`
echo "Test Starting at $date" >> $result_file 

# 10 parallel clients: each inserting 1000 files, 1 at a time
#echo "10 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
#calculateAverage 10 dbsStressTest.py 1000 1
# 10 parallel clients: each inserting 1000 files, 10 at a time
echo "10 parallel clients: each inserting 1000 files, 100 at a time" >> $result_file
calculateAverage 10 dbsStressTest.py 100 10
# 10 parallel clients: each inserting 1000 files, 100 at a time
echo "10 parallel clients: each inserting 1000 files, 10 at a time" >> $result_file
calculateAverage 10 dbsStressTest.py 10 100
# 10 parallel clients: each inserting 1000 files, 1000 at a time
echo "10 parallel clients: each inserting 1000 files, 1 at a time" >> $result_file
calculateAverage 10 dbsStressTest.py 1 1000


# 15 parallel clients: each inserting 1000 files, 1 at a time
#echo "15 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
#calculateAverage 15 dbsStressTest.py 1000 1
# 15 parallel clients: each inserting 1000 files, 10 at a time
echo "15 parallel clients: each inserting 1000 files, 100 at a time" >> $result_file
calculateAverage 15 dbsStressTest.py 100 10
# 15 parallel clients: each inserting 1000 files, 100 at a time
echo "15 parallel clients: each inserting 1000 files, 10 at a time" >> $result_file
calculateAverage 15 dbsStressTest.py 10 100
# 15 parallel clients: each inserting 1000 files, 1000 at a time
echo "15 parallel clients: each inserting 1000 files, 1 at a time" >> $result_file
calculateAverage 15 dbsStressTest.py 1 1000


# 20 parallel clients: each inserting 1000 files, 1 at a time
#echo "20 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
#calculateAverage 20 dbsStressTest.py 1000 1
# 20 parallel clients: each inserting 1000 files, 10 at a time
echo "20 parallel clients: each inserting 1000 files, 100 at a time" >> $result_file
calculateAverage 20 dbsStressTest.py 100 10
# 20 parallel clients: each inserting 1000 files, 100 at a time
echo "20 parallel clients: each inserting 1000 files, 10 at a time" >> $result_file
calculateAverage 20 dbsStressTest.py 10 100
# 20 parallel clients: each inserting 1000 files, 1000 at a time
echo "20 parallel clients: each inserting 1000 files, 1 at a time" >> $result_file
calculateAverage 20 dbsStressTest.py 1 1000


# 30 parallel clients: each inserting 1000 files, 1 at a time
#echo "30 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
#calculateAverage 30 dbsStressTest.py 1000 1
# 30 parallel clients: each inserting 1000 files, 10 at a time
echo "30 parallel clients: each inserting 1000 files, 100 at a time" >> $result_file
calculateAverage 30 dbsStressTest.py 100 10
# 30 parallel clients: each inserting 1000 files, 100 at a time
echo "30 parallel clients: each inserting 1000 files, 10 at a time" >> $result_file
calculateAverage 30 dbsStressTest.py 10 100
# 30 parallel clients: each inserting 1000 files, 1000 at a time
echo "30 parallel clients: each inserting 1000 files, 1 at a time" >> $result_file
calculateAverage 30 dbsStressTest.py 1 1000


date=`date`
echo "Test Finishing at $date" >> $result_file
echo "NOW Going to Try the worst of it, 1000 files at a time thingy" >> $result_file
cat $result_file | mail -s "Time Profile Test Done" anzar@fnal.gov
echo "DONE"

echo "NOW Trying the worst of it 1000 files at a time thingy"


# 30 parallel clients: each inserting 1000 files, 1 at a time
echo "10 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
calculateAverage 10 dbsStressTest.py 1000 1
# 30 parallel clients: each inserting 1000 files, 1 at a time
echo "15 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
calculateAverage 15 dbsStressTest.py 1000 1
# 30 parallel clients: each inserting 1000 files, 1 at a time
echo "20 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
calculateAverage 20 dbsStressTest.py 1000 1
# 30 parallel clients: each inserting 1000 files, 1 at a time
echo "30 parallel clients: each inserting 1000 files, 1000 at a time" >> $result_file
calculateAverage 30 dbsStressTest.py 1000 1

cat $result_file | mail -s "Time Profile Test Done" anzar@fnal.gov
echo "DONE"



