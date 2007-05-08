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

try:
  optManager  = DbsOptionParser()
  (opts,args) = optManager.getOpt()
  #if len(args) < 1:
  #   print "You must provide a log file name "
  #   sys.exit()
  #logfile=args[0]
  api = DbsApi(opts.__dict__)
  
  try:
   #Must run dbsInsertFiles from UserExamples first !!!!!!!!!
   #print len(api.listFiles("/test_primary_anzar_001/SIM/TestProcessedDS001", "", "")) 
   #print len(api.listFiles("/DBSStressTestPrimaryDataset/DBSStressTestHIT/DBSStressTestProcessedDS", "", "")) 
   #print len(api.listFiles("/CSA06-083-os-Wenu/SIM/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877644-merged")) 
   #print len(api.listFiles(path="/StressTestPrimary260c8c7b-4b28-4654-a8db-6ffd977046b50/StressTestProcessed260c8c7b-4b28-4654-a8db-6ffd977046b50/GEN-SIM"))
   print len(api.listFiles(path="/StressTestPrimary3aa90e71-7e7e-4e4f-b3a6-f9c99fd704710/StressTestProcessed3aa90e71-7e7e-4e4f-b3a6-f9c99fd704710/GEN-SIM"))

  except DbsDatabaseError,e:
   print e
  
except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()
print "Done"

