#/bin/sh
python dbsMigrateDBS1toDBS2Diff.py
cat DatasetToTransferFromDBS1ToDBS2.txt | grep CMSSW_1_3_0 > FilteredDatasetToTransferFromDBS1ToDBS2.txt
cat DatasetToTransferFromDBS1ToDBS2.txt | grep CMSSW_1_2_0 >> FilteredDatasetToTransferFromDBS1ToDBS2.txt
cat DatasetToTransferFromDBS1ToDBS2.txt | grep CMSSW_1_2_1 >> FilteredDatasetToTransferFromDBS1ToDBS2.txt
for i in $(cat FilteredDatasetToTransferFromDBS1ToDBS2.txt) ; 
do
	echo "./migrateDBStoDBS2.sh $i"
done
