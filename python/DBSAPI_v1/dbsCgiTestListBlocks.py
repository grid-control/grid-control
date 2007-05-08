#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from dbsCgiApi import DbsCgiApi
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"
# DEFAULT_URL = "exec:../../Servers/CGIServer/dbsxml"

try:
  args = {}
  if len(sys.argv) == 2: args['instance'] = sys.argv[1]
  api = DbsCgiApi(DEFAULT_URL, args)

  dataset = "/CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302"
  print api.listBlocks(dataset,"yes")
  print "\n\n"
  print api.listBlocks(dataset)

except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
