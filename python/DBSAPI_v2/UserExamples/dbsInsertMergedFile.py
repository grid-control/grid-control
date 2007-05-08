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

try:
  merge_algo = DbsAlgorithm (
                    ExecutableName="EdmFastMerge",
                    ApplicationVersion= "v101",
                    ApplicationFamily="Merge",
                    )
  path = "/test_primary_001/TestProcessedDS001/SIM"
  merge_proc = api.insertMergedDataset(path, "ThisISMergedDataset001", merge_algo)

  # File will go into THIS Block
  block = DbsFileBlock (
         StorageElement=['test1', 'test3'],
         Name="/test_primary_001/TestProcessedDS001/SIM#12345"
         )

  merged_file = DbsFile (
        Checksum= '00000',
        LogicalFileName= 'MERGEDFILE_001',
        NumberOfEvents= 10000,
        FileSize= 1000000,
        Status= 'VALID',
	ValidationStatus = 'VALID',
        FileType= 'EVD',
        Dataset= merge_proc,
        Block= block,
        AlgoList = [merge_algo],
         )

  #api.insertFiles (proc, [myfile1], block)

  parentList = ['NEW_TEST0001', 'NEW_TEST0002'] # The parent Un-Merged files
  api.insertMergedFile(parentList, merged_file)
  print "Result: %s" % merged_file

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


print "Done"

