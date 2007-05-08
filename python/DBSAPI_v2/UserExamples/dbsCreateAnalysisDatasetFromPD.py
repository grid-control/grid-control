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
from DBSAPI.dbsAnalysisDataset import DbsAnalysisDataset
#from dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsOptions import DbsOptionParser


optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)


#path can also be specified as a Processed Dataset Object itself (dbsProcessedDataset)
path="/test_primary_anzar_001/SIM/TestProcessedDS002/"

analysis=DbsAnalysisDataset(
                            Name='TestAnalysisDataset001',
                            Annotation='This is a test analysis dataset',
                            Type='HeckIKnowTheType',
                            Status='VALID',
                            PhysicsGroup='BPositive'
                           ) 

print "Creating An Analysis Dataset  %s for Path %s" % (analysis['Name'], path)

try:
    api.createAnalysisDatasetFromPD ("/test_primary_anzar_001/SIM/TestProcessedDS002/", analysis)
    print "Result: %s" % analysis
except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

