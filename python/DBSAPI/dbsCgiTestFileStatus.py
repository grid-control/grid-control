#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from dbsCgiApi import DbsCgiApi, DbsCgiObjectExists
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/in.sh"

#DEFAULT_URL = "exec:../CGIServer/prodquery"

try:
  args = {}
  if len(sys.argv) == 2: args['instance'] = sys.argv[1]
  api = DbsCgiApi(DEFAULT_URL, args)

  print "Setting file status as invalid"
  api.setFileStatus ("tmpLFN4", "invalid")
 
  print "Setting file status as valid"
  #api.setFileStatus ("tmpLFN6", "valid")
  #api.setFileStatus ("tmpLFN6", None)
  #api.setFileAvailable ("tmpLFN6")
  #api.setFileUnavailable ("6B9EC-5026-DB11-9898-003048713B63.root")
  #api.setFileAvailable ("/store/unmerged/PreProd/2006/8/2/ProdAgentDevTest1/GEN-SIM-DIGI/0000/9CE6B9EC-5026-DB11-9898-003048713B63.root")

except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
