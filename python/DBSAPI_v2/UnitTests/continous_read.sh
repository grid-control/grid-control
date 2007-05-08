#!/bin/sh
# 20 parallel clients, lets BLOW UP the server !!
while [ "1" == "1" ]; do
  for i in `seq 1 30` ; do
     { time python dbsStressTestListFiles.py; } >> continous_read.log.$i 2>&1  &
  done
  wait
  #cat continous_read.log* | grep -i exception| wc -l | mail -s "continous_read.log failure count" anzar@fnal.gov
done
