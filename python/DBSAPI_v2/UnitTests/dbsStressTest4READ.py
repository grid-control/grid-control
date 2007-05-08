#!/usr/bin/env python
#
# API Unit tests for the DBS JavaServer.

import sys
import os
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
from dbsUnitTestApi import DbsUnitTestApi
import random
import pdb

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()


if len(args) < 3:
   print "You must provide number of <iterations> each insertinghow <number-of-files> and <logfile-pretext>" 
   print "python dbsStressTest.py <iterations> <number-of-files> <logfile-pretext>"
   print "python dbsStressTest.py 1 10 mylog : WILL MEAN 1 Iteration each inserting 10 files"
   print "python dbsStressTest.py 10 10 mylog : WILL MEAN 10 Iterations each inserting 10 files"
   print "python dbsStressTest.py 10 100 mylog : WILL MEAN 10 Iterations each inserting 100 files"
   sys.exit(1)
else:
   maxFiles=int(args[0]) 
   maxDS=int(args[1])
   logext=str(args[2])

api = DbsApi(opts.__dict__)

#Get this just once

filename="bulkDataResult."+logext
#print "FILENAME: "+filename
f = open(filename, "w")

#################  WRITING TO STDOUT COMMENT THIS LINE (UNCOMMENT ABOVE) TO WRITE TO A FILE ###########
#f=sys.stdout

for i in range(maxDS):
        # Make this cycle unique
        mytime = os.popen('uuidgen').readline().strip()
        mytime += str(i)
        fileList = []
	#Insert Primary
	apiObj = DbsUnitTestApi(api.insertPrimaryDataset, f)
	primary = 'StressTestPrimary' + mytime
	pri1 = DbsPrimaryDataset (Name = primary)
	apiObj.run(pri1, excep = False)

	#Insert Algorithm
	apiObj = DbsUnitTestApi(api.insertAlgorithm,f)
	algo1 = DbsAlgorithm (ExecutableName="StressTestExe01", 
		ApplicationVersion= "StressTestVersion01" + mytime, 
		ApplicationFamily="StressTestAppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="StressTestParam01", 
							Version="V001", 
							Type="test", 
							Annotation="This is a stress test param", 
							Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
	apiObj.run(algo1, excep = False)
	
	#Insert Tier
	apiObj = DbsUnitTestApi(api.insertTier, f)
	tierName1 = "GEN"
	tierName2 = "SIM"
	apiObj.run(tierName1, excep = False)
	apiObj.run(tierName2, excep = False)

	tierList = [tierName1, tierName2]
	
	#Insert Processed Datatset
	apiObj = DbsUnitTestApi(api.insertProcessedDataset,f)
	proc1  = DbsProcessedDataset(PrimaryDataset=pri1,
			Name="StressTestProcessed" + mytime,
			PhysicsGroup="BPositive",
			Status="VALID",
			TierList=tierList,
			AlgoList=[algo1])
	apiObj.run(proc1, excep = False)

	apiObj = DbsUnitTestApi(api.insertBlock, f)

	path = "/" + str(proc1['PrimaryDataset']['Name']) + "/" + str(proc1['Name']) + "/" + tierName1 + "-" + tierName2
        print "PATH: %s" % path

	#Insert Block
	block1 = DbsFileBlock (Name = path+'#01234-0567', Path = path)
	apiObj.run(path, block1 , excep = False)

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
		apiObj = DbsUnitTestApi(api.insertFiles, f)
                lfn1 = mytime+str(j)
		file1= DbsFile (
			Checksum= '999',
			LogicalFileName= lfn1,
			#QueryableMetadata= 'This is a test file',
			NumberOfEvents= 10000,
			FileSize= 12340,
			Status= 'VALID',
			FileType= 'EDM',
			LumiList= [lumi1, lumi2],
			TierList= tierList,
			)
	
		fileList.append(file1)
        print "\n\n\nNUMBER of FILES with which insertFile API is called: %s" %str(len(fileList))               
	apiObj.run(proc1 ,fileList, block1,  excep = False)

f.close()
