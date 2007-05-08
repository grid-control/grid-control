#/bin/sh
dataset=$1
leng=${#dataset}
if [ $leng -eq 0 ] ; then 
	echo "Usage: ./migrateDBStoDBS2.sh <datasetpath>"
	echo "Example: ./migrateDBStoDBS2.sh /CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged"
else
	instanceFrom="MCGlobal/Writer"
	instanceTo="DevMC/Writer"
	tmp=${dataset//\//_}
	tmpIFrom=${instanceFrom//\//_}
	tmpITo=${instanceTo//\//_}
	fileName="${tmpIFrom}_${tmpITo}${tmp}.xml"
	if [ -f $fileName ] ; then
		echo "****************************** WARNNING ***************************************"
		echo "WARNNING $fileName already exists and will be used instead of fetcihng the dataset again"
		echo "****************************** WARNNING ***************************************"
	else
		#echo $fileName
		#cd ../../PythonAPI/
		cd ../../DBSAPIOLD/
		echo ""
		echo "Fetching dataset conetents from DBS-1"
		echo ""
		python dbsCgiMigrate.py $instanceFrom $instanceTo $dataset get
		#mv $fileName ../Python/UserExamples/
		mv $fileName ../DBSAPI/UserExamples/
		cd -
	fi	
	echo ""
	echo "Inserting dataset into DBS-2"
	python dbsMigrateDBS1toDBS2.py $fileName &> ${fileName}.log
	echo ""
	echo "************************************************************************"
	echo "Log of transfer is written in ${fileName}.log"
	echo "************************************************************************"
	echo ""
	echo ""
	grep -i "exception" ${fileName}.log
fi
