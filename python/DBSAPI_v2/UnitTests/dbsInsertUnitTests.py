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
from DBSAPI.dbsAnalysisDatasetDefinition import DbsAnalysisDatasetDefinition
from DBSAPI.dbsAnalysisDataset import DbsAnalysisDataset
from DBSAPI.dbsLumiSection import DbsLumiSection
from DBSAPI.dbsOptions import DbsOptionParser
from dbsUnitTestApi import DbsUnitTestApi

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)
#mytime = str(time.time())
mytime = time.strftime("_%Y%m%d_%Hh%Mm%Ss",time.localtime())

f = open("result.txt", "a+")

apiObj = DbsUnitTestApi(api.insertPrimaryDataset, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertPrimaryDataset API tests***************************")

primary = 'TestPrimary_001' + mytime

pri1 = DbsPrimaryDataset (Name = primary, Type="test")
apiObj.run(pri1, excep = False)

primary = 'TestPrimary_002' + mytime
pri2 = DbsPrimaryDataset (Name = primary, Type="test")
apiObj.run(pri2, excep = False)

pri = DbsPrimaryDataset ()
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Test Het", Type="test")
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Test;Het", Type="test")
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Test*Het", Type="test")
apiObj.run(pri, excep = True)

# / is allowed
pri = DbsPrimaryDataset (Name = "Ta/estHet", Type="test")
apiObj.run(pri, excep = False)

pri = DbsPrimaryDataset (Name = primary, Type="test")
apiObj.run(pri,"", excep = True)

f.write("\n***********************insertPrimaryDataset API tests***************************")



apiObj = DbsUnitTestApi(api.insertAlgorithm,f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertAlgorithm API tests***************************")
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


algo2 = DbsAlgorithm (ExecutableName="TestExe011", 
		ApplicationVersion= "TestVersion011" + mytime, 
		ApplicationFamily="AppFamily011", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstParam01",
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo2, excep = False)

# Selection based on HASh, But hash is missing, must raise exception
algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Name="001234565798685", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
#No PSet_Hash, must give default PSet Hash
apiObj.run(algo, excep = False)


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstParam01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstParam01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)


algo = DbsAlgorithm (
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
apiObj.run(algo, excep = True)


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ApplicationFamily="AppFamily01" 
	)

# No PsetHash is OK 
apiObj.run(algo, excep = False)


algo = DbsAlgorithm (ExecutableName="TestE xe01", 
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
apiObj.run(algo, excep = True)

algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstPddaram01", 
							Version="V00/1", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = False)


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersQQQ01" + mytime, 
		ApplicationFamily="AppFami;ly01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstParam01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)


algo = DbsAlgorithm (ExecutableName="TestEDDDe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="00123 4565798685", 
							Name="MyaaaaddddffFirstParam01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                          Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)

# ; is not allowed in ExecutableName
algo = DbsAlgorithm (ExecutableName="Test;Exe01", 
		ApplicationVersion= "Tewwwrsion01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstP;aram01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)


algo = DbsAlgorithm (ExecutableName="Testaae01", 
		ApplicationVersion= "TestVeggrsion01" + mytime, 
		ApplicationFamily="AppFami*ly01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstParam01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)

algo = DbsAlgorithm (ExecutableName="TestE01", 
		ApplicationVersion= "TestVn01" , 
		ApplicationFamily="AppFami1", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234a5*5798685", 
							Name="MyFirstPzzzzaram01", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)

f.write("\n***********************insertAlgorithm API tests***************************")

apiObj = DbsUnitTestApi(api.insertTier, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertTier API tests***************************")
#tierName1 = "HIT" + mytime
tierName1 = "GEN"
#tierName2 = "SIM" + mytime
tierName2 = "SIM"
apiObj.run(tierName1, excep = False)
apiObj.run(tierName1, excep = False)
apiObj.run(tierName2, excep = False)
apiObj.run("", excep = True)
apiObj.run(tierName1 + "sjhd*lk", excep = True)
apiObj.run(tierName1 + "sjhd;lk", excep = True)
apiObj.run(tierName1 + "sjhd lk", excep = True)
apiObj.run(tierName1, "",  excep = True)

f.write("\n***********************insertTier API tests***************************")

apiObj = DbsUnitTestApi(api.insertRun, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertRun API tests***************************")
runNumber1 = 101 + int(time.time()%10000)
runNumber2 = 102 + int(time.time()%10000)
runNumber3 = 103 + int(time.time()%10000)

run1 = DbsRun (RunNumber=runNumber1,
		NumberOfEvents= 100,
		NumberOfLumiSections= 20,
		TotalLuminosity= 2222,
		StoreNumber= 123,
		StartOfRun= 'now',
		EndOfRun= 'never',
)
apiObj.run(run1, excep = False)

run2 = DbsRun (RunNumber=runNumber2,
		NumberOfEvents= 100,
		NumberOfLumiSections= 20,
		TotalLuminosity= 2222,
		StoreNumber= 123,
		StartOfRun= 'now',
		EndOfRun= 'never',

		)
apiObj.run(run2, excep = False)

run = DbsRun (RunNumber=runNumber3,
		StartOfRun= 'no*w')
apiObj.run(run, excep = True)

run = DbsRun (RunNumber=runNumber3,
		StartOfRun= 'no w')
apiObj.run(run, excep = True)

run = DbsRun (RunNumber=runNumber3,
		StartOfRun= 'no;w')
apiObj.run(run, excep = True)

run = DbsRun (RunNumber=runNumber3,
		EndOfRun= 'nev*er')
apiObj.run(run, excep = True)

run = DbsRun (RunNumber=runNumber3,
		EndOfRun= 'nev er')
apiObj.run(run, excep = True)

run = DbsRun (RunNumber=runNumber3,
		EndOfRun= 'nev;er')
apiObj.run(run, excep = True)

f.write("\n***********************insertRun API tests***************************")

apiObj = DbsUnitTestApi(api.updateRun, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************updateRun API tests***************************")

run1 = DbsRun (RunNumber=runNumber1,
		NumberOfEvents= 9999,
		NumberOfLumiSections= 20,
		TotalLuminosity= 2222,
		StoreNumber= 123,
		StartOfRun= 'dec',
		EndOfRun= 'never',
)
apiObj.run(run1, excep = False)

run2 = DbsRun (RunNumber=111111)
apiObj.run(run2, excep = True)


f.write("\n\n***********************updateRun API tests***************************")



f.write("\n\n***********************insertProcessedDataset API tests***************************")
tierList = [tierName1, tierName2]

apiObj = DbsUnitTestApi(api.insertProcessedDataset,f)
apiObj.setVerboseLevel(opts.verbose)
proc1 = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcessed" + mytime,
		PhysicsGroup="BPositive",
		Status="VALID",
		TierList=tierList,
		AlgoList=[algo1, algo2],
                RunsList=[runNumber1],
                )
apiObj.run(proc1, excep = False)

proc3 = DbsProcessedDataset(PrimaryDataset=pri2,
		Name="TestProcessed" + mytime,
		PhysicsGroup="BPositive",
		Status="VALID",
		TierList=tierList,
		AlgoList=[algo1, algo2],
                RunsList=[runNumber1],
                )
apiObj.run(proc3, excep = False)

proc3 = DbsProcessedDataset(PrimaryDataset=pri2,
		Name="TestProcessed" + mytime,
		PhysicsGroup="BPositive",
		Status="VALID",
		TierList=tierList,
		AlgoList=[algo1, algo2],
                RunsList=[runNumber1],
                )
apiObj.run(proc3, excep = False)


proc2 = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcessed1" + mytime)
apiObj.run(proc2, excep = False)

proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Ta estHet", Type="test"),
		Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Ta*estHet", Type="test"),
		Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Taes;tHet", Type="test"),
		Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProce ssed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProce*ssed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProce/ssed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestPro;cessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcesssssssed" + mytime,
		PhysicsGroup="BPosit*aive")
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcewwwwwwwed" + mytime,
		PhysicsGroup="BPosit;aive")
apiObj.run(proc, excep = True)

algo = DbsAlgorithm (ExecutableName="TeaaaaaastExe011", 
		ApplicationVersion= "TestVersaaaaaaaaion011" + mytime, 
		ApplicationFamily="AppFamilaaaaaaay011", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstaaaaaParam01" 
			                              )
	)
proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProceqqqqqqd" + mytime,
		AlgoList=[algo])
apiObj.run(proc, excep = True)

algo = DbsAlgorithm (ExecutableName="Teaaaaa/tExe011", 
		ApplicationVersion= "TestVersaaaaaaaaion011" + mytime, 
		ApplicationFamily="AppFamilaaaaaaay011", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstaaaaaParam01" 
			                              )
	)
proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProceqqqqqqd" + mytime,
		AlgoList=[algo])
apiObj.run(proc, excep = True)


algo = DbsAlgorithm (ExecutableName="TeaaaaatExe011", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFirstaaaaaParam01" 
			                              )
	)
proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProceqqqqqqd" + mytime,
		AlgoList=[algo])
apiObj.run(proc, excep = True)


algo = DbsAlgorithm (ExecutableName="TeaaaaatExe011", 
		ApplicationVersion= "TestVersaaaaaaaaion011" + mytime, 
		ApplicationFamily="AppFamilaaaaaaay011")
proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProceqqqqqqd" + mytime,
		AlgoList=[algo])
apiObj.run(proc, excep = True)
apiObj.run(proc,"", excep = True)

f.write("\n***********************insertProcessedDataset API tests***************************")

path = "/" + str(proc1['PrimaryDataset']['Name']) + "/" + str(proc1['Name']) + "/" + str(proc1['TierList'][0])+ "-"+str(proc1['TierList'][1])

apiObj = DbsUnitTestApi(api.insertTierInPD, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertTierInPD API tests***************************")
tierName1Test = "GENTEST"
tierName2Test = "SIMTEST"
apiObj.run(proc1, "GEN", excep = False)
apiObj.run(proc1, "SIM", excep = False)
apiObj.run(path, "GEN", excep = False)
apiObj.run("/Does/Not/Exist", tierName2Test, excep = True)
apiObj.run("", excep = True)
apiObj.run(proc1, tierName1Test + "sjhd*lk", excep = True)
apiObj.run(proc1, tierName1Test + "sjhd;lk", excep = True)
apiObj.run(proc1, tierName1Test + "sjhd lk", excep = True)
apiObj.run(proc1, tierName1Test,   excep = True)

f.write("\n***********************insertTierInPD API tests***************************")


apiObj = DbsUnitTestApi(api.insertParentInPD, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertParentInPD API tests***************************")
apiObj.run(proc1, proc3, excep = False)
apiObj.run(proc1, "", excep = True)
apiObj.run(proc1, "/Does/Not/Exist", excep = True)
apiObj.run("/Does/Not/Exist", proc3, excep = True)
apiObj.run(proc1, "sjhd lk", excep = True)
apiObj.run(proc1, "abd;def",  excep = True)

f.write("\n***********************insertParentInPD API tests***************************")


apiObj = DbsUnitTestApi(api.insertAlgoInPD, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertAlgoInPD API tests***************************")
apiObj.run(proc1, algo1, excep = False)
apiObj.run(proc1, algo2, excep = False)
apiObj.run("/Does/Not/Exist", algo1, excep = True)

algo = DbsAlgorithm (ExecutableName="Does", 
		ApplicationVersion= "Not" + mytime, 
		ApplicationFamily="Exist", 
		ParameterSetID=DbsQueryableParameterSet(Name="001234565798685", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)

apiObj.run(proc1, algo, excep = True)

f.write("\n***********************insertAlgoInPD API tests***************************")


apiObj = DbsUnitTestApi(api.insertRunInPD, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertRunInPD API tests***************************")
apiObj.run(proc1, runNumber2, excep = False)
apiObj.run(proc1, runNumber3, excep = True)
apiObj.run("/Does/Not/Exist", runNumber1, excep = True)
apiObj.run(proc1, "123456", excep = True)
apiObj.run(proc1, "123a456", excep = True)

f.write("\n***********************insertRunInPD API tests***************************")




f.write("\n\n***********************updateProcDSStatus API tests***************************")

apiObj = DbsUnitTestApi(api.updateProcDSStatus,f)
apiObj.setVerboseLevel(opts.verbose)
apiObj.run(proc1, "INVALID", excep = False)
apiObj.run(path, "INVALID", excep = False)
apiObj.run("/aa/does/notexist", "INVALID", excep = True)
apiObj.run(path, "IN;VALID", excep = True)

f.write("\n\n***********************updateProcDSStatus API tests***************************")

apiObj = DbsUnitTestApi(api.insertBlock, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertBlock API tests***************************")
#path = "/" + str(proc1['PrimaryDataset']['Name']) + "/" + str(proc1['TierList'][0]) + "/" + str(proc1['Name'])
#blockName =  "/"+ mytime + "this/isatestblock#016712"
blockName =   path + "#016712"
blockName1 =   path + "#016713"
blockName2 =   path + "#016714"
#blockName1 =  "/"+ mytime + "this/isatestskljblock#016712"
#blockName2 =  "/"+ mytime + "thislkss/isatestskljblock#016712"
block = DbsFileBlock (Path = path)
apiObj.run(path, excep = False)

block1 = DbsFileBlock (Name = blockName)
apiObj.run(path, blockName, excep = False)

block = DbsFileBlock (Name= blockName1)
apiObj.run(path, block,  excep = False)

block = DbsFileBlock (Name= blockName2)
apiObj.run(proc1, block,  excep = False)

apiObj.run(path, "/" + mytime + "this/isatestblock016712", excep = True)
apiObj.run("/absssssssc/dessssssf/hijaaaaaaa", excep = True)
apiObj.run("/abcaaaa/deaaaaaaf/hiaaaaaaaj", "/this/isatestblock#016712", excep = True)
apiObj.run(path, "/thisisatestblock#016712", excep = True)
apiObj.run(path, "/thisis atestblock#016712", excep = True)
apiObj.run(path, "thisisat/ae/stblock#016712", block, excep = True)
apiObj.run("/ddd/hd*/hdhd", excep = True)
apiObj.run("/dd d/hd/hdhd", excep = True)
apiObj.run("/ddd/hd/hd;hd", excep = True)


f.write("\n***********************insertBlock API tests***************************")

apiObj = DbsUnitTestApi(api.insertLumiSection, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertLumiSection API tests***************************")
lumiNumber1 = 111 + int(time.time()%10000)
lumiNumber2 = 112 + int(time.time()%10000)
lumiNumber3 = 113 + int(time.time()%10000)

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

lumi = DbsLumiSection (startEventNumber=100)
apiObj.run(lumi, excep = True)

lumi = DbsLumiSection (startEventNumber='10 0')
apiObj.run(lumi, excep = True)

lumi = DbsLumiSection (LumiSectionNumber=lumiNumber3,
			StartEventNumber=100,
			EndEventNumber=200,
			LumiStartTime='noti me',
			RunNumber=runNumber1,
			)
apiObj.run(lumi, excep = True)


lumi = DbsLumiSection (LumiSectionNumber=lumiNumber3,
			StartEventNumber=100,
			EndEventNumber=200,
			LumiStartTime='not* me',
			RunNumber=runNumber1,
			)
apiObj.run(lumi, excep = True)

lumi = DbsLumiSection (LumiSectionNumber=lumiNumber3,
			StartEventNumber=100,
			EndEventNumber=200,
			LumiStartTime='no;me',
			RunNumber=runNumber1,
			)
apiObj.run(lumi, excep = True)

f.write("\n***********************insertLumiSection API tests***************************")

apiObj = DbsUnitTestApi(api.insertFiles, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertFiles API tests***************************")
lfn1 = '1111-0909-9767-8764' + mytime
lfn2 = '1111-0909-9767-876411' + mytime
file1= DbsFile (
		Checksum= '999',
		LogicalFileName= lfn1,
		#QueryableMetadata= 'This is a test file',
		NumberOfEvents= 10000,
		FileSize= 12340,
		Status= 'VALID',
		ValidationStatus = 'VALID',
		FileType= 'EDM',
		LumiList= [lumi1, lumi2],
		TierList= tierList,
		AlgoList = [algo1, algo2],
		)

file2= DbsFile (
		Checksum= '999',
		LogicalFileName= lfn2,
		#QueryableMetadata= 'This is a test file',
		NumberOfEvents= 10000,
		FileSize= 12340,
		Status= 'VALID',
		ValidationStatus = 'VALID',
		FileType= 'EDM',
		LumiList= [lumi1, lumi2],
		TierList= tierList,
		AlgoList = [algo1, algo2],
		)

apiObj.run(proc1 ,[file1,file2], block1,  excep = False)

file3 = DbsFile (LogicalFileName= '1111-0909-9767-8764222' + mytime,
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID',
		FileSize= 12340)
apiObj.run(proc1 ,[file3], block1,  excep = True)

file = DbsFile (LogicalFileName= '1111*-0909-9767-8764222' + mytime,
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID',
		FileSize= 12340)
apiObj.run(proc1 ,[file], block1,  excep = True)

file = DbsFile (LogicalFileName= '1111;-0909-9767-8764222' + mytime,
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID', 
		FileSize= 12340)
apiObj.run(proc1 ,[file], block1,  excep = True)

file = DbsFile (LogicalFileName= '1111-0909-9767-876411111' + mytime,
		NumberOfEvents= 10000,
                Status= 'VALID',
		Checksum= '999',
		FileSize= 12340)
proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcessxxxxxxxxxxxxx" + mytime,
                Status= 'VALID',
		TierList=tierList)

apiObj.run(proc ,[file], block1,  excep = True)

block = DbsFileBlock (Name = "/" + mytime + "xxxxxxxxxxxxxxxxthis/isatestblock#016712")
apiObj.run(proc1 ,[file], block,  excep = True)

file = DbsFile (LogicalFileName= '1111-0909-9767-876411111' + mytime,
		ParentList = [lfn1,lfn2],
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID',
		FileSize= 12340,
		TierList=tierList
		)
apiObj.run(proc1 ,[file], block1,  excep = True)

file = DbsFile (LogicalFileName= '1111-0909-9767-87641234545' + mytime,
		ParentList = [lfn1,'doesnotexists'],
		NumberOfEvents= 10000,
		Checksum= '999',
                Status= 'VALID',
		FileSize= 12340,
		TierList=tierList)
apiObj.run(proc1 ,[file], block1,  excep = True)

f.write("\n***********************insertFiles API tests***************************")

apiObj = DbsUnitTestApi(api.updateFileStatus, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************updateFileStatus API tests***************************")

apiObj.run(lfn1, "VALID", excep = False)
apiObj.run(lfn1, "DOESNOTEXIST", excep = True)
apiObj.run("DOESNOTEXIST", "VALID", excep = True)
apiObj.run("DO;ESNOTEXIST", "VALID", excep = True)
apiObj.run("DO/ESNOTEXIST", "VALID", excep = True)
apiObj.run(lfn1, "VA;LID", excep = True)
apiObj.run(lfn1, "VA/LID", excep = True)


f.write("\n\n***********************updateFileStatus API tests***************************")



adef = DbsAnalysisDatasetDefinition(Name="TestAnalysisDSDef_005" + mytime,
		ProcessedDatasetPath=path,
		FileList=[file1['LogicalFileName'], file2['LogicalFileName']],
		AlgoList = [algo1, algo2],
		TierList= tierList,
		AnalysisDSList=[],
		LumiRangeList=[('1', '4444'), ('5000', '90000')],
		RunRangeList=[('0', '5000'), ('6000', '99999')],
		UserCut="get all blah blah from x=1, y=6, z=j, lumi=all",
		Description="This is a test Analysis Dataset" + mytime,
		)
		    
apiObj = DbsUnitTestApi(api.createAnalysisDatasetDefinition, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************createAnalysisDatasetDefinition API tests***************************")

apiObj.run(adef, excep = False)
apiObj.run(adef, excep = True)

adef1 = DbsAnalysisDatasetDefinition(Name="TestAnalysisDSDef_006" + mytime,
		ProcessedDatasetPath=path,
		Description="This is a test Analysis Dataset" + mytime,
		)

apiObj.run(adef1, excep = False)

adef1 = DbsAnalysisDatasetDefinition(Name="TestAnalysisDSDef_007" + mytime)
apiObj.run(adef1, excep = True)

adef1 = DbsAnalysisDatasetDefinition(Name="TestAnalysisDSDe  f_006" + mytime)
apiObj.run(adef1, excep = True)


f.write("\n***********************createAnalysisDatasetDefinition API tests***************************")


apiObj = DbsUnitTestApi(api.createAnalysisDataset, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************createAnalysisDataset API tests***************************")


ads = DbsAnalysisDataset(
                            Name='TestAnalysisDataset005' + mytime,
                            Annotation='testdataset' +mytime,
                            Type='TEST',
                            Status='NEW',
                            PhysicsGroup='BPositive'
                           )

apiObj.run(ads, adef['Name'] , excep = False)
apiObj.run(ads, adef['Name'] , excep = False)

ads1 = DbsAnalysisDataset(Name='TestAnalysisDataset0056' + mytime,
                            Annotation='testdataset' +mytime,
                            PhysicsGroup='BPositive')
apiObj.run(ads1, adef['Name'] , excep = False)

ads1 = DbsAnalysisDataset(Name='TestAnalysisDaaaataset0056' + mytime,
                            PhysicsGroup='BPositive')
apiObj.run(ads1, adef['Name'] , excep = True)

ads1 = DbsAnalysisDataset(Name='TestAnalysiqqqqsDataset0056' + mytime,
                            Annotation='testdataset' +mytime,)
apiObj.run(ads1, adef['Name'] , excep = True)

ads1 = DbsAnalysisDataset(Name='TestAnalysisDataset0  056' + mytime,
                            Annotation='testdataset' +mytime,
                            PhysicsGroup='BPositive' )
apiObj.run(ads1, adef['Name'] , excep = True)

apiObj.run(adef['Name'] , excep = True)
apiObj.run(ads1, 'Should_not_exists' , excep = True)

f.write("\n***********************createAnalysisDataset API tests***************************")


apiObj = DbsUnitTestApi(api.addReplicaToBlock, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************addReplicaToBlock API tests***************************")

apiObj.run(block1, "MySE1", excep = False)
apiObj.run(block1, "MySE2", excep = False)
apiObj.run(block1, "My;SE", excep = True)
apiObj.run("/does/not/eist#1234", "MySE1", excep = True)

f.write("\n\n***********************addReplicaToBlock API tests***************************")

apiObj = DbsUnitTestApi(api.deleteReplicaFromBlock, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************deleteReplicaFromBlock API tests***************************")

apiObj.run(block1, "MySE1", excep = False)
apiObj.run(block1, "My;SE", excep = True)
apiObj.run(block1, "DOESNOTEXIST", excep = True)
apiObj.run("/does/not/eist#1234", "MySE1", excep = True)

f.write("\n\n***********************deleteReplicaFromBlock API tests***************************")


apiObj = DbsUnitTestApi(api.renameSE, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************renameSE API tests***************************")

apiObj.run("MySE2", "MySE2New" + mytime, excep = False)
apiObj.run("DOESNOTEXIST", "MySE", excep = False)
apiObj.run("abcd;edf", "ANTHING", excep = False)
apiObj.run("/does/no", "MySE1", excep = False)

f.write("\n\n***********************renameSE API tests***************************")


apiObj = DbsUnitTestApi(api.closeBlock, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************closeBlock API tests***************************")

apiObj.run(block1, excep = False)
apiObj.run("DOESNOTEXIST",  excep = True)
apiObj.run("abcd;edf", excep = True)
apiObj.run("/does/no/exit#1234",  excep = True)

f.write("\n\n***********************closeBlock API tests***************************")

# Store ONE path that could be used by next LIST test cases
pathfile=open('pathfile', 'w')
pathfile.write(path)
pathfile.close()
blockfile=open('blockfile', 'w')
blockfile.write(blockName)
blockfile.close()
lfnfile=open('lfnfile', 'w')
lfnfile.write(lfn2)
lfnfile.close()

###########################

f.close()
sys.exit(0)

