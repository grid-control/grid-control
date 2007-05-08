#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from DBSAPIOLD.dbsCgiApi import DbsCgiApi, DbsCgiDatabaseError
from DBSAPIOLD.dbsException import DbsException
from DBSAPIOLD.dbsApi import DbsApi, DbsApiException, InvalidDataTier
DEFAULT_HOST = "cmssrv18.fnal.gov"
DEFAULT_PORT = 8443
DEFAULT_BASE = "/DBS/servlet/DBSServlet"
#DEFAULT_URL = "http://venom.fnal.gov:8080/DBS/servlet/DBSServlet"
#DEFAULT_URL = "http://venom.fnal.gov:8080/servlets-examples/servlet/RequestHeaderExample"
#DEFAULT_URL = "http://venom.fnal.gov:8080/servlets-examples/servlet/HeaderExample"
DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"
#DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/dbsxml"
#DEFAULT_URL = "exec:../../Servers/CGIServer/prodquerytest2"
#DEFAULT_URL = "http://lxgate40.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery"
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
  datasetPattern = "/*/*/*"
  
  """
  try:
   # List some datasets
   print ""
   print "Listing datasets %s" % datasetPattern
   datasets = api.listProcessedDatasets (datasetPattern)
   for dataset in datasets:
     print "  %s" % dataset
  except DbsCgiDatabaseError,e:
   print e
  """

  mydataset = "/CSA06-083-os-Wenu/SIM/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877644-merged"
  try:
   # List some datasets
   print len(api.getDatasetFileBlocks(mydataset)) 
   #for block in api.getDatasetFileBlocks(mydataset):
   #  print "  %s" % block
  except DbsCgiDatabaseError,e:
   print e


  otherDatasetPath = "/PreProdR2Pion10GeV/SIM/GEN-SIM-DIGI"
  otherDatasetPath = "/CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged"
  otherDatasetPath = "/CSA06-082-os-TTbar/SIM/CMSSW_0_8_2-GEN-SIM-DIGI-1155826011-merged"
   
except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"

