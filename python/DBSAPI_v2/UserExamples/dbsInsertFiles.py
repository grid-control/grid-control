#!/usr/bin/env python
#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
#
# Unit tests for the DBS CGI implementation.

import sys
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsAlgorithm import DbsAlgorithm
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsLumiSection import DbsLumiSection
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsOptions import DbsOptionParser

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

#args={}
#args['url']='http://cmssrv17.fnal.gov:8989/DBS/servlet/DBSServlet' 
#args['version']='v00_00_05'
#args['level']='CRITICAL'
#args['level']='ERROR'
#api = DbsApi(args)

algo = DbsAlgorithm (
         ExecutableName="TestExe01",
         ApplicationVersion= "TestVersion01",
         ApplicationFamily="AppFamily01",
         ParameterSetID=DbsQueryableParameterSet(
           Hash="001234565798685",
           )
         )
primary = DbsPrimaryDataset (Name = "test_primary_001")
proc = DbsProcessedDataset (
        PrimaryDataset=primary, 
        Name="TestProcessedDS001", 
        PhysicsGroup="BPositive",
        Status="Valid",
        TierList=['SIM', 'GEN'],
        AlgoList=[algo],
        )

lumi1 = DbsLumiSection (
         LumiSectionNumber=1222,
         StartEventNumber=100,
         EndEventNumber=200,
         LumiStartTime='notime',
         LumiEndTime='neverending',
         RunNumber=1,
         )
lumi2 = DbsLumiSection (
         LumiSectionNumber=1333,
         StartEventNumber=100,
         EndEventNumber=200,
         LumiStartTime='notime',
         LumiEndTime='neverending',
         RunNumber=1,
         )

lumi3 = DbsLumiSection (
         #LumiSectionNumber=1333,
         #StartEventNumber=100,
         #EndEventNumber=200,
         #LumiStartTime='notime',
         #LumiEndTime='neverending',
         RunNumber=1,
         )

myfile1= DbsFile (
        Checksum= '999',
        LogicalFileName= 'NEW_TEST0001',
        #QueryableMetadata= 'This is a test file',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
	ValidationStatus = 'VALID',
        FileType= 'EDM',
        Dataset= proc,
        #Block= isDictType,
        AlgoList = [algo],
        LumiList= [lumi1, lumi2],
        TierList= ['SIM', 'GEN'],
        #ParentList = ['NEW_TEST0003']  
         )

myfile2= DbsFile (
        Checksum= '000',
        LogicalFileName= 'NEW_TEST0002',
        #QueryableMetadata= 'This is a test file',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
	ValidationStatus = 'VALID',
        FileType= 'EDM',
        Dataset= proc,
        #Block= isDictType,
        LumiList= [lumi1, lumi2],
        TierList= ['SIM', 'GEN'],
        AlgoList = [algo],
        BranchList=['testbranch01', 'testbranch02'],
        #ParentList = ['NEW_TEST0004']  
         )
        

myfile11= DbsFile (
        Checksum= '999',
        LogicalFileName= 'NEW_TEST0004',
        #QueryableMetadata= 'This is a test file',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
        ValidationStatus = 'VALID',
        FileType= 'EDM',
        Dataset= proc,
        #Block= isDictType,
        AlgoList = [algo],
        LumiList= [lumi1, lumi2],
        TierList= ['SIM', 'GEN'],
        ParentList = ['NEW_TEST0001']  
         )

myfile22= DbsFile (
        Checksum= '000',
        LogicalFileName= 'NEW_TEST0005',
        #QueryableMetadata= 'This is a test file',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
        ValidationStatus = 'VALID',
        FileType= 'EDM',
        Dataset= proc,
        #Block= isDictType,
        LumiList= [lumi1, lumi2],
        TierList= ['SIM', 'GEN'],
        AlgoList = [algo],
        BranchList=['testbranch01', 'testbranch02'],
        ParentList = ['NEW_TEST0002']  
         )


# A file with RunsList and NOT lumi list
myfile3= DbsFile (
        Checksum= '000',
        LogicalFileName= 'NEW_TEST003',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
        ValidationStatus = 'VALID',
        FileType= 'EDM',
        Dataset= proc,
        TierList= ['SIM', 'GEN'],
        AlgoList = [algo],
	RunsList = [1],
         )
 
# Need to provide Block name if YOU want to control Block management (The block named must pre-exist), if NOT then DBS will throw this file in
# Open Block for this Dataset, and will do the Block management too.
# Make a choice
                   
block = DbsFileBlock (
         StorageElement=['test1', 'test3'],
	 Name="/test_primary_001/TestProcessedDS001/GEN-SIM#12345"
         )

print "BUG to be fixed in server, cannot handle QueryableMetadata"
print "For now we don't have BLOCK Management on Server side so User need to providea BLOCK"
print "In future it will be an optional parameter"
 
print "Inserting files in processDS %s" % proc

try:
    # A file with RunsList and NOT lumi list

    #Insert in a Block	
    #api.insertFiles (proc, [myfile1, myfile2, myfile11, myfile22])
    api.insertFiles (proc, [myfile1, myfile2, myfile11, myfile22], block)
    #DBS Creates the Block and add file that has ONLYU run, No Lumi
    api.insertFiles (proc, [myfile3] )

    print "Result: %s" % myfile3

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


print "Done"
