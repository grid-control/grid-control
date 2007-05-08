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
from DBSAPI.dbsAnalysisDatasetDefinition import DbsAnalysisDatasetDefinition
from DBSAPI.dbsAnalysisDataset import DbsAnalysisDataset


optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)



analysis=DbsAnalysisDataset(
                            Name='TestAnalysisDataset001',
                            Annotation='testdataset',
                            Type='TEST',
                            Status='NEW',
                            PhysicsGroup='BPositive'
                           )

try:
    #api.insertFiles (proc, [myfile1], block)
    api.createAnalysisDataset(analysis, "TestAnalysisDSDef_001")
    print "DONE", analysis

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


print "Done"

