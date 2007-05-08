#!/usr/bin/env python
#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
#
# Unit tests for the DBS CGI implementation.
#
#   Mind that examples here are more elaborate and can be compacted


import sys
import random
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsOptions import DbsOptionParser


optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

primary = DbsPrimaryDataset (Name = "test_primary_001")

proc = DbsProcessedDataset (
         PrimaryDataset=primary,
         Name="TestProcessedDS001",
         TierList=['GEN', 'SIM'],
	 #Path='/test_primary_001/TestProcessedDS001/GEN'
         )

block = DbsFileBlock (
         Name="/test_primary_001/TestProcessedDS001/GEN-SIM#12345"
         #Name="/test_primary_001/TestProcessedDS001/SIM#12345"
         )

print "Creating block %s" % block

try:
    print api.insertBlock (proc, block)
    #print api.insertBlock (proc)
    #print api.insertBlock ("/test_primary_001/TestProcessedDS001/SIM", block=None, storage_element=["thisIsMyOnlySE"])
    #print api.insertBlock ("/test_primary_001/TestProcessedDS001/SIM")
    print api.insertBlock ("/test_primary_001/TestProcessedDS001/GEN", "/test_primary_001/TestProcessedDS001/GEN#12345" , ['se1', 'se2', 'se3'])

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

