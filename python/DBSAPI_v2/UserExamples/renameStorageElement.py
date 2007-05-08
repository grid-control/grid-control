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


#block = DbsFileBlock (
#         Name="/TestPrimary1164751189.48/HIT1164751189.48/TestProcessed1164751189.48"
#         )

print "Inserting SE in a block /test_primary_001/TestProcessedDS001/SIM#12345"

try:
	api.renameSE ( 'seaa1', "seNewNameWow")
except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


print "Done"

