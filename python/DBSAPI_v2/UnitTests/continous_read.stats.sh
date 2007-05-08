#!/bin/sh
export total_tests=`cat continous_read.log.* |grep real|wc -l`
echo $total_tests

mintsecs=`cat continous_read.log.* | grep real| awk '{print $2}'|awk -Fm '{sum = sum + $1} END {print sum}'| awk '{avg = $1/ENVIRON["total_tests"]} END {print avg }'`
#mintsecs=`cat continous_read.log.* |grep real| awk '{print $2}'|awk -Fm '{print $2}'|awk -Fs '{sum = sum + $1} END {print sum}'| awk '{avg = $1/ENVIRON["total_tests"]} END {print avg}'`
secs=`cat continous_read.log.* |grep real| awk '{print $2}'|awk -Fm '{print $2}'|awk -Fs '{sum = sum + $1} END {print sum}'| awk '{avg = $1/ENVIRON["total_tests"] } END {print avg}'`


tot=`expr $mintsecs + $secs`
echo $tot
echo $mintsecs  $secs
#total=`awk '{sum = ENVIRON["mintsecs"] + ENVIRON["secs"]} END {print sum}'`
