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
  api = DbsApi(opts.__dict__)
  
  try:
   path = "/TestPrimary_002_20070205_11h52m29s/SIM_20070205_11h52m29s/TestProcessed_20070205_11h52m29s"
   token = path.split("/")
   proc = api.listProcessedDatasets(token[1], token[2], token[3])[0]
   print "proc fetched from DBS %s" %proc
   proc['Name'] = "TestProcessed_20070205_11h52m29s_MERGED"
   proc['ParentList'] = [path]
   
   print "proc modified to be inserted in DBS %s" %proc
   api.insertProcessedDataset (proc)
   print "Result: %s" % proc
	
  except DbsDatabaseError,e:
   print e
  
except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

