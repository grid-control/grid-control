#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from dbsCgiApi import DbsCgiApi, DbsCgiDatabaseError
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier
DEFAULT_HOST = "cmssrv18.fnal.gov"
DEFAULT_PORT = 8443
DEFAULT_BASE = "/DBS/servlet/DBSServlet"
#DEFAULT_URL = "http://venom.fnal.gov:8080/DBS/servlet/DBSServlet"
#DEFAULT_URL = "http://venom.fnal.gov:8080/servlets-examples/servlet/RequestHeaderExample"
#DEFAULT_URL = "http://venom.fnal.gov:8080/servlets-examples/servlet/HeaderExample"
DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
#DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/dbsxml"
#DEFAULT_URL = "exec:../CGIServer/prodquery"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/catout.sh"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/abc.sh"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/run.sh"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/in.sh"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/err.sh"

try:
  args = {}
  if len(sys.argv) == 2: args['instance'] = sys.argv[1]
  print args
  api = DbsCgiApi(DEFAULT_URL, args)
  #api.setLogLevel(DBS_LOG_LEVEL_ALL_)
  # api.setDebug(1)
   
 # Datasets we play with
  datasetPattern = "/CMSSW_0_6_0_pre7/*/*"
  
  try:
   # List some datasets
   print ""
   print "Listing datasets %s" % datasetPattern
   datasets = api.listDatasetsFromApp (datasetPattern)
   for dataset in datasets:
     print "  %s" % dataset
  except DbsCgiDatabaseError,e:
   print e

except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
