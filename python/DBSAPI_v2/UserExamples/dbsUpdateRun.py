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
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsOptions import DbsOptionParser

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

                            
run = DbsRun (
         RunNumber=1,
         NumberOfEvents= 101,
         #NumberOfLumiSections= 20,
         #TotalLuminosity= 2222,
         #EndOfRun= 'RightNow',
         )
 
print "Creating a run"

try:
    api.updateRun (run)

    print "Result: %s" % run

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


print "Done"

