rm -rf CloseBlock.txt
for i in $(cat datasetToTranfer.txt) ; do echo $i ; ./migrateDBStoDBS2Merge.sh $i ;done
for i in $(cat CloseBlock.txt) ; do echo $i ; python dbsCloseAllMigratedBlocks.py $i ;done

