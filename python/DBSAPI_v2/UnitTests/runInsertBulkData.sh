#!/bin/sh
cd ..
source setup.sh
cd -
echo "Please wait ... The tests can take up to 5 minutes"
python dbsInsertBulkData.py > /dev/null
echo "Test results are written in $PWD/bulkDataResult.txt"
echo ""
message=`cat bulkDataResult.txt | grep FAILED`
len=${#message}
if [ "$len" -lt "1" ] ; then
	echo "All tests PASSED OK. For more details look in the bulkDataResult.txt file"
else
	echo $message
fi
