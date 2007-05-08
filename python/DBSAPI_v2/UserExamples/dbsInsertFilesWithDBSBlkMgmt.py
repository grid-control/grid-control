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

algo = DbsAlgorithm (
         ExecutableName="TestExe01",
         ApplicationVersion= "TestVersion01",
         ApplicationFamily="AppFamily01",
         ParameterSetID=DbsQueryableParameterSet(
           Hash="001234565798685",
           Name="MyFirstParam01",
           Version="V001",
           Type="test",
           Annotation="This is test",
           Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
           )
         )

primary = DbsPrimaryDataset (Name = "test_primary_001")
proc = DbsProcessedDataset (
        PrimaryDataset=primary, 
        Name="TestProcessedDS003", 
        PhysicsGroup="BPositive",
        Status="Valid",
        TierList=['SIM', 'RECO'],
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



myfile1= DbsFile (
        Checksum= '999',
        LogicalFileName= 'aaa1122-0909-9767-8764aaa',
        #QueryableMetadata= 'This is a test file',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
	ValidationStatus = 'VALID',
        FileType= 'EVD',
        Dataset= proc,
        #Block= isDictType,
        LumiList= [lumi1, lumi2],
        TierList= ['SIM', 'RECO'],
         )

myfile2= DbsFile (
        Checksum= '000',
        LogicalFileName= 'aaaa2233-0909-9767-8764aaa',
        #QueryableMetadata= 'This is a test file',
        NumberOfEvents= 10000,
        FileSize= 12340,
        Status= 'VALID',
	ValidationStatus = 'VALID',
        FileType= 'EVD',
        Dataset= proc,
        #Block= isDictType,
        #LumiList= [lumi1, lumi2],
        TierList= ['SIM', 'RECO'],
        AlgoList = [algo],
        #ParentList = ['lfn01', 'lfn02']  
         )
                            
block = DbsFileBlock (
         #Name="/test_primary_anzar_001/TestProcessedDS002#879143ef-b527-44cb-867d-fff54f5730db"
         Name="/this/hahah#12345"
         )

print "BUG to be fixed in server, cannot handle QueryableMetadata"
print "For now we don't have BLOCK Management on Server side so User need to providea BLOCK"
print "In future it will be an optional parameter"
 
print "Inserting files in processDS %s" % proc

try:
    api.insertFiles (proc, [myfile1, myfile2], None)
    #api.insertFiles (proc, [myfile1, myfile2], block)
    print "Result: %s" % proc

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


print "Done"

