#!/usr/bin/env python
#
# API Unit tests for the DBS JavaServer.

import sys
import time
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsAlgorithm import DbsAlgorithm
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsLumiSection import DbsLumiSection
from DBSAPI.dbsOptions import DbsOptionParser
from DBSAPI.dbsUnitTestApi import DbsUnitTestApi

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)
mytime = str(time.time())

maxDS = 1
maxFiles = 10000
f = open("bulkDataResult.txt", "w")

for i in range(maxDS):
	mytime = str(time.time())
	#Insert Primary
	apiObj = DbsUnitTestApi(api.insertPrimaryDataset, f)
	primary = 'TestPrimary' + mytime
	pri1 = DbsPrimaryDataset (Name = primary, Type="TEST")
	apiObj.run(pri1, excep = False)

	#Insert Algorithm
	apiObj = DbsUnitTestApi(api.insertAlgorithm,f)
	algo1 = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstParam01", 
							Version="V001", 
							Type="test", 
							Annotation="This is test", 
							Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
	apiObj.run(algo1, excep = False)
	
	#Insert Tier
	apiObj = DbsUnitTestApi(api.insertTier, f)
	tierName1 = "HIT" + mytime
	tierName2 = "SIM" + mytime
	apiObj.run(tierName1, excep = False)
	apiObj.run(tierName2, excep = False)

	tierList = [tierName1, tierName2]
	
	#Insert Processed Datatset
	apiObj = DbsUnitTestApi(api.insertProcessedDataset,f)
	proc1 = DbsProcessedDataset(PrimaryDataset=pri1,
			Name="TestProcessed" + mytime,
			PhysicsGroup="BPositive",
			Status="Valid",
			TierList=tierList,
			AlgoList=[algo1])
	apiObj.run(proc1, excep = False)

	apiObj = DbsUnitTestApi(api.insertBlock, f)
	path = "/" + str(proc1['PrimaryDataset']['Name']) + "/" + str(proc1['TierList'][0]) + "/" + str(proc1['Name'])

	#Insert Block
	block1 = DbsFileBlock (Name = "/" + mytime + "this/isatestblock#016712", Path = path)
	apiObj.run(path, "/" + mytime + "this/isatestblock#016712" , excep = False)

	#Insert Run
	apiObj = DbsUnitTestApi(api.insertRun, f)
	runNumber1 = 101 + int(time.time()%1000)
	run1 = DbsRun (RunNumber=runNumber1,
			NumberOfEvents= 100,
			NumberOfLumiSections= 20,
			TotalLuminosity= 2222,
			StoreNumber= 123,
			StartOfRun= 'now',
			EndOfRun= 'never',
	)
	apiObj.run(run1, excep = False)

	#Insert Lumi Section
	apiObj = DbsUnitTestApi(api.insertLumiSection, f)
	lumiNumber1 = 111 + int(time.time()%100)
	lumiNumber2 = 112 + int(time.time()%100)

	lumi1 = DbsLumiSection (LumiSectionNumber=lumiNumber1,
			StartEventNumber=100,
			EndEventNumber=200,
			LumiStartTime='notime',
			LumiEndTime='neverending',
			RunNumber=runNumber1,
			)

	apiObj.run(lumi1, excep = False)

	lumi2 = DbsLumiSection (LumiSectionNumber=lumiNumber2,
			StartEventNumber=100,
			EndEventNumber=200,
			RunNumber=runNumber1)
	apiObj.run(lumi2, excep = False)

	#Insert File
	for j in range(maxFiles):
		mytime = str(time.time())
		apiObj = DbsUnitTestApi(api.insertFiles, f)
		lfn1 = '1111-1919-9767-8764' + mytime
		lfn2 = '1111-1919-9767-876411' + mytime
		file1= DbsFile (
			Checksum= '999',
			LogicalFileName= lfn1,
			#QueryableMetadata= 'This is a test file',
			NumberOfEvents= 10000,
			FileSize= 12340,
			Status= 'Valid',
			FileType= 'EVD',
			LumiList= [lumi1, lumi2],
			TierList= tierList,
			)
	
		file2= DbsFile (
			Checksum= '999',
			LogicalFileName= lfn2,
			#QueryableMetadata= 'This is a test file',
			NumberOfEvents= 10000,
			FileSize= 12340,
			Status= 'Valid',
			FileType= 'EVD',
			LumiList= [lumi1, lumi2],
			TierList= tierList,
			)
	
		#apiObj.run(proc1 ,[file1,file2], block1,  excep = False)
                #Lets try without block
		apiObj.run(proc1 ,[file1,file2], None,  excep = False)

f.close()
