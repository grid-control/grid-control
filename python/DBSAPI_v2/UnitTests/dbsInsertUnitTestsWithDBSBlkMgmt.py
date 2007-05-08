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

f = open("result.txt", "a+")

apiObj = DbsUnitTestApi(api.insertPrimaryDataset, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertPrimaryDataset API tests***************************")

primary = 'TestPrimary' + mytime

pri1 = DbsPrimaryDataset (Name = primary, Type="TEST")
apiObj.run(pri1, excep = False)

apiObj.run(pri1, excep = True)

pri = DbsPrimaryDataset ()
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Test Het", Type="TEST")
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Test;Het", Type="TEST")
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Test*Het", Type="TEST")
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = "Ta/estHet", Type="TEST")
apiObj.run(pri, excep = True)

pri = DbsPrimaryDataset (Name = primary, Type="TEST")
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
apiObj.run(algo1, excep = True)


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


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVersion01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Version="V001", 
							Type="test", 
                                                        Annotation="This is test",
                                                        Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
			                              )
	)
apiObj.run(algo, excep = True)


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
apiObj.run(algo, excep = True)


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
apiObj.run(algo, excep = True)


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "Tea/stVe/rsion01" + mytime, 
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


algo = DbsAlgorithm (ExecutableName="TestExe01", 
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


algo = DbsAlgorithm (ExecutableName="TestExe01", 
		ApplicationVersion= "TestVezzzn01" + mytime, 
		ApplicationFamily="AppFamily01", 
		ParameterSetID=DbsQueryableParameterSet(Hash="001234565798685", 
							Name="MyFir/stP/aram01", 
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
tierName1 = "HIT" + mytime
tierName2 = "SIM" + mytime
apiObj.run(tierName1, excep = False)
apiObj.run(tierName1, excep = False)
apiObj.run(tierName2, excep = False)
apiObj.run("", excep = True)
apiObj.run(tierName1 + "sjhd*lk", excep = True)
apiObj.run(tierName1 + "sjhd;lk", excep = True)
apiObj.run(tierName1 + "sjhd lk", excep = True)
apiObj.run(tierName1, "",  excep = True)

f.write("\n***********************insertTier API tests***************************")

f.write("\n\n***********************insertProcessedDataset API tests***************************")
tierList = [tierName1, tierName2]

apiObj = DbsUnitTestApi(api.insertProcessedDataset,f)
apiObj.setVerboseLevel(opts.verbose)
proc1 = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcessed" + mytime,
		PhysicsGroup="BPositive",
		Status="VALID",
		TierList=tierList,
		AlgoList=[algo1, algo2])
apiObj.run(proc1, excep = False)
apiObj.run(proc1, excep = True)


proc2 = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcessed1" + mytime)
apiObj.run(proc2, excep = False)

proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Ta/estHet", Type="TEST"),
		Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)


proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Ta estHet", Type="TEST"),
		Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Ta*estHet", Type="TEST"),
		Name="TestProcessed2" + mytime)
apiObj.run(proc, excep = True)

proc = DbsProcessedDataset(PrimaryDataset=DbsPrimaryDataset (Name = "Taes;tHet", Type="TEST"),
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


apiObj = DbsUnitTestApi(api.insertBlock, f)
apiObj.setVerboseLevel(opts.verbose)
f.write("\n\n***********************insertBlock API tests***************************")
path = "/" + str(proc1['PrimaryDataset']['Name']) + "/" + str(proc1['TierList'][0]) + "/" + str(proc1['Name'])
blockName =  "/"+ mytime + "this/isatestblock#016712"
blockName1 =  "/"+ mytime + "this/isatestskljblock#016712"
blockName2 =  "/"+ mytime + "thislkss/isatestskljblock#016712"
block = DbsFileBlock (Path = path)
apiObj.run(path, excep = False)

block1 = DbsFileBlock (Name = blockName)
apiObj.run(path, blockName, excep = False)

block = DbsFileBlock (Name= blockName1)
apiObj.run(path, block,  excep = False)

block = DbsFileBlock (Name= blockName2)
apiObj.run(proc1, block,  excep = False)

apiObj.run(path, blockName,  excep = True)
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
apiObj.run(run1, excep = True)

run2 = DbsRun (RunNumber=runNumber2)
apiObj.run(run2, excep = True)

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
		Status= 'VALID',
		ValidationStatus = 'VALID',
		FileType= 'EVD',
		LumiList= [lumi1, lumi2],
		TierList= tierList,
		)

apiObj.run(proc1 ,[file1,file2], None,  excep = False)
#apiObj.run(proc1 ,[file1,file2], block1,  excep = False)
apiObj.run(proc1 ,[file1,file2], None,  excep = True)
#apiObj.run(proc1 ,[file1,file2], block1,  excep = True)

file3 = DbsFile (LogicalFileName= '1111-0909-9767-8764222' + mytime,
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID',
		FileSize= 12340)
#apiObj.run(proc1 ,[file3], block1,  excep = False)
apiObj.run(proc1 ,[file3], None,  excep = False)

file = DbsFile (LogicalFileName= '1111*-0909-9767-8764222' + mytime,
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID',
		FileSize= 12340)
#apiObj.run(proc1 ,[file], block1,  excep = True)
apiObj.run(proc1 ,[file], None,  excep = True)

file = DbsFile (LogicalFileName= '1111;-0909-9767-8764222' + mytime,
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID', 
		FileSize= 12340)

#apiObj.run(proc1 ,[file], block1,  excep = True)
apiObj.run(proc1 ,[file], None,  excep = True)

file = DbsFile (LogicalFileName= '1111-0909-9767-876411111' + mytime,
		NumberOfEvents= 10000,
                Status= 'VALID',
		Checksum= '999',
		FileSize= 12340)
proc = DbsProcessedDataset(PrimaryDataset=pri1,
		Name="TestProcessxxxxxxxxxxxxx" + mytime,
                Status= 'VALID',
		TierList=tierList)


#apiObj.run(proc ,[file], block1,  excep = True)
apiObj.run(proc ,[file], None,  excep = True)

"""
Changing block name may not work, need to investigate that
block = DbsFileBlock (Name = "/" + mytime + "xxxxxxxxxxxxxxxxthis/isatestblock#016712")

apiObj.run(proc1 ,[file], block,  excep = True)
#apiObj.run(proc1 ,[file], block,  excep = True)

file = DbsFile (LogicalFileName= '1111-0909-9767-876411111' + mytime,
		ParentList = [lfn1,lfn2],
		Checksum= '999',
		NumberOfEvents= 10000,
                Status= 'VALID',
		FileSize= 12340)
apiObj.run(proc1 ,[file], block1,  excep = False)

file = DbsFile (LogicalFileName= '1111-0909-9767-87641234545' + mytime,
		ParentList = [lfn1,'doesnotexists'],
		NumberOfEvents= 10000,
		Checksum= '999',
                Status= 'VALID',
		FileSize= 12340)
apiObj.run(proc1 ,[file], block1,  excep = True)
"""

f.write("\n***********************insertFiles API tests***************************")


# Store ONE path that could be used by next LIST test cases
pathfile=open('pathfile', 'w')
pathfile.write(path)
pathfile.close()
blockfile=open('blockfile', 'w')
blockfile.write(blockName)
blockfile.close()
###########################

f.close()
sys.exit(0)

