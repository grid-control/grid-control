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
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsOptions import DbsOptionParser

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

#algo = {'ExecutableName': 'TestExe01', 'ApplicationVersion': 'TestVersion011164750596.79', 'ParameterSetID': {'Content': 'int a= {}, b={c=1, d=33}, f={}, x, y, x', 'Version': 'V001', 'Hash': '001234565798685', 'Name': 'MyFirstParam01', 'Type': 'test', 'Annotation': 'This is test'}, 'ApplicationFamily': 'AppFamily01'}

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

#primary = DbsPrimaryDataset (Name = "TestPrimary1164750596.79")
primary = DbsPrimaryDataset (Name = "test_primary_001")
proc = DbsProcessedDataset (
                            PrimaryDataset=primary, 
                            Name="TestProcessedDS001", 
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['GEN', 'SIM'],
                            AlgoList=[algo],
                            RunsList=[1],   # Provide a Run Number List that goes with this ProcDS                   
                            )
                             
print "Creating a processed dataset %s" % proc

try:
    api.insertProcessedDataset (proc)
    print "Result: %s" % proc

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

