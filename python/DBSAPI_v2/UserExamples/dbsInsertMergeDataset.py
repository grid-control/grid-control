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
from DBSAPI.dbsOptions import DbsOptionParser
from DBSAPI.dbsAlgorithm import DbsAlgorithm
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset

try:
   optManager  = DbsOptionParser()
   (opts,args) = optManager.getOpt()
   api = DbsApi(opts.__dict__)

   import pdb
 
   ##Primary Dataset
   primary = DbsPrimaryDataset (Name = "test_primary_001", Type="TEST")
   api.insertPrimaryDataset(primary)

   #Algorithm used by Parent and Child Datasets for our test
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

   api.insertAlgorithm (algo)

   # Parent Dataset 
   parent_procds = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS001-Parent",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['GEN', 'SIM'],
                            AlgoList=[algo],
                            )

   api.insertProcessedDataset(parent_procds) 

   # Lets say child dataset will have two runs
   api.insertRun (DbsRun (
         RunNumber=1,
         NumberOfEvents= 100,
         NumberOfLumiSections= 10,
         TotalLuminosity= 1111,
         StoreNumber= 1234,
         StartOfRun= 'now',
         EndOfRun= 'never',
         ) )

   api.insertRun (  DbsRun (
         RunNumber=2,
         NumberOfEvents= 200,
         NumberOfLumiSections= 20,
         TotalLuminosity= 2222,
         StoreNumber= 5678,
         StartOfRun= 'now',
         EndOfRun= 'never',
         ) )

   # Child Dataset
   child_procds = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS001-Child",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['GEN', 'SIM'],
                            AlgoList=[algo],
                            RunsList=[1, 2],   # Provide a Run Number List that goes with this ProcDS                   
                            ParentList=[parent_procds]  #parent_procds as its parent
                            ) 

   api.insertProcessedDataset(child_procds)

   # Lets us create Merged Dataset for Child Dataset
   merge_algo = DbsAlgorithm (
                    ExecutableName="EdmFastMerge",
                    ApplicationVersion= "v101",
                    ApplicationFamily="Merge",
                    )
   merged_ds = api.insertMergedDataset(child_procds, "ThisISMergedDataset001", merge_algo)

   # Now we should be able to
   # See that there is a Processed Dataset with Name: ThisISMergedDataset001
   print "\nThere is a Processed Dataset with Name: ThisISMergedDataset001 ???"
   print api.listProcessedDatasets("test_primary_001", "*", "ThisISMergedDataset001")
   
   # It has Runs from child_procds
   print "\nIt has Runs (1,2) from child_procds ???"
   print api.listRuns(merged_ds)

   # And its Parent is parent_procds
   print "\nAnd its Parent is parent_procds"
   print api.listDatasetParents(merged_ds)
	
except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

