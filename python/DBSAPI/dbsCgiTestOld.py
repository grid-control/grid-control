#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import os, sys
from dbsCgiApi import DbsCgiApi
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier, DBS_LOG_LEVEL_ALL_

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/dbsxml"
# DEFAULT_URL = "exec:../../Servers/CGIServer/dbsxml"

try:
  args = {}
  if len(sys.argv) == 2: args['instance'] = sys.argv[1]
  api = DbsCgiApi(DEFAULT_URL, args)
  api.setLogLevel(DBS_LOG_LEVEL_ALL_)
  # api.setDebug(1)

  # Datasets we play with
  datasetPattern = "/*/*/eg_2x1033PU761_TkMu_2_g133_OSC"
  datasetPath = "/eg03_jets_1e_pt2550/Digi/eg_2x1033PU761_TkMu_2_g133_OSC"
  otherDatasetPath = "/bt03_B0sJPsiX/Hit/bt_Hit245_2_g133"

  # List some datasets
  print ""
  print "Listing datasets %s" % datasetPattern
  datasets = api.listDatasets (datasetPattern)
  for dataset in datasets:
    print "  %s" % dataset

  # Get dataset provenance. It returns list of dataset parents.
  print ""
  tiers = [ "Hit" ]
  print "Provenance for: %s (dataTiers: %s)" % (datasetPath, tiers)
  for parent in api.getDatasetProvenance(datasetPath, tiers):
    print "  %s" % parent

  print ""
  tiers = [ "Digi", "Hit" ]
  print "Provenance for: %s (dataTiers: %s)" % (otherDatasetPath, tiers)
  for parent in api.getDatasetProvenance(otherDatasetPath, tiers):
    print "  %s" % parent

  # Get dataset contents, returning a list of blocks with event collections
  print ""
  print "Dataset contents for: %s" % otherDatasetPath
  for block in api.getDatasetContents(otherDatasetPath):
    print "  File block name/id: %s/%d, %d event collections}" % \
      (block.getBlockName(), block.getObjectId(), len(block.getEventCollectionList()))

  # Get dataset contents as a list of blocks with files
  print ""
  # print "Dataset files for: %s" % datasets[0]
  # for block in api.getDatasetFileBlocks (datasets[0]):
  print "Dataset files for: %s" % datasetPath
  for block in api.getDatasetFileBlocks (datasetPath):
    print "  File block name/id: %s/%d, %d files}" % \
      (block.getBlockName(), block.getObjectId(), len(block.getFileList()))

except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
