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
  
  try:
   # List all parameter sets
   print ""
   print "Listing parameter sets"
   for pset in api.listParameterSets("**"):
     print "  %s" % pset
  except DbsCgiDatabaseError,e:
   print e
  
  try:
   # List all applications
   print ""
   print "Listing applications"
   for app in api.listApplications():
     print "  %s" % app
   # List all application configurations
   print ""
   print "Listing application configurations"

   
   for appc in api.listApplicationConfigs():
     print "  %s" % appc
   
  except DbsCgiDatabaseError,e:
   print e
  
  try:
   # List all primary datasets
   print ""
   print "Listing primary datasets t*"
   for p in api.listPrimaryDatasets("*"):
     print "  %s" % p
  except DbsCgiDatabaseError,e:
   print e
  # Datasets we play with
  datasetPattern = "/*/*/*"
  
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
  try:
   # Get dataset provenance. It returns list of dataset parents.
   print ""
   tiers = [ "Hit" ]
   datasetPath = datasets[1]
   print "Provenance for: %s (dataTiers: %s)" % (datasetPath, tiers)
   for parent in api.getDatasetProvenance(datasetPath, tiers):
     print "  %s" % parent
  except DbsCgiDatabaseError,e:
   print e
  
  try:
   print ""
   tiers = [ "Digi", "Hit" ]
   otherDatasetPath = datasets[2]
   print "Provenance for: %s (dataTiers: %s)" % (otherDatasetPath, tiers)
   for parent in api.getDatasetProvenance(otherDatasetPath, tiers):
     print "  %s" % parent
  except DbsCgiDatabaseError,e:
   print e
  """
  
  otherDatasetPath = "/PreProdR2Pion10GeV/SIM/GEN-SIM-DIGI"
  otherDatasetPath = "/CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged"
  otherDatasetPath = "/CSA06-082-os-TTbar/SIM/CMSSW_0_8_2-GEN-SIM-DIGI-1155826011-merged"
  try:
   # Get dataset contents, returning a list of blocks with event collections
   print ""
   print "Dataset contents for: %s" % otherDatasetPath
   for block in api.getDatasetContents(otherDatasetPath):
     print block.get('blockName')
     #print block['guid']
     #print "  File block name/id: %s/%d, %d event collections}" % \
     #  (block.get('blockName'), block.get('objectId'), len(block.get('eventCollectionList')) )
     #for ev in block.get('eventCollectionList') :
     #  print "evc ", ev
  except DbsCgiDatabaseError,e:
   print e
   
  #otherDatasetPath = "/PreProdR2Pion10GeV/SIM/GEN-SIM-DIGI"
  #otherDatasetPath = "/test_primary_anzar/DST/test_process_anzar"
  try:
   # Get dataset contents as a list of blocks with files
   print ""
   #print "Dataset files for: %s" % datasets[0]
   for block in api.getDatasetFileBlocks (otherDatasetPath):
     print "  File block name/id: %s/%d, %d files}" % \
       (block.get('blockName'), block.get('objectId'), len(block.get('fileList')) )
     for ev in block.get('eventCollectionList') :
       print "evc ", ev
  except DbsCgiDatabaseError,e:
   print e
   
except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
