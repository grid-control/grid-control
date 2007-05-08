#!/bin/sh
while [ "1" == "1" ] ; do 
	for i in `seq 1 20` ; do
		python dbsListPrimaryDatasets.py & 
		python dbsListAlgorithm.py &
		python dbsListProcessedDatasets.py &
		python dbsListTiers.py &
		python dbsListBlocks.py &
		python dbsListRuns.py &
		python dbsListFiles.py &
		sleep 10
	done
done

