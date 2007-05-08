#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys, time
from dbsCgiApi import DbsCgiApi, DbsCgiObjectExists
from dbsException import DbsException
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsEventCollection import DbsEventCollection
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsProcessedDataset import DbsProcessedDataset
from dbsProcessing import DbsProcessing
from dbsApi import DbsApi, DbsApiException, InvalidDataTier, DBS_LOG_LEVEL_ALL_

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
# DEFAULT_URL = "exec:../../Servers/CGIServer/prodquery"

try:
  nop = 1
  if (len(sys.argv) > 2): nop = int(sys.argv[2])

  nevc = 1000
  if (len(sys.argv) > 3): nevc = int(sys.argv[3])

  api = DbsCgiApi(DEFAULT_URL, { 'instance' : sys.argv[1] })
  api.setLogLevel(DBS_LOG_LEVEL_ALL_)
  # api.setDebug(1)

  # Make a new test dataset, insert group of event collections
  basename = "test_big_%f" % time.time()
  primary = DbsPrimaryDataset (datasetName = basename)
  processing = DbsProcessing (primaryDataset = primary,
		      	      processingName = "test_process",
			      applicationConfig = {
			        'application' : { 'executable' : 'testexe',
				 		  'version' : 'testversion',
						  'family' : 'testfamily' },
				'parameterSet' : { 'hash' : basename,
				 		   'content' : basename }})
  block = DbsFileBlock (processing = processing)
  gen = DbsProcessedDataset (primaryDataset=primary,
                             datasetName="test_process",
                             dataTier="GEN")
  all = []
  for i in range(nop):
    files = []
    evcs = []
    all.append ((files, evcs))
    for j in range(nevc/nop):
      f = DbsFile (logicalFileName="%s.%d.%d" % (basename, i, j),
                   fileSize=1, checkSum="cksum:1", fileType="EVD")
      e = DbsEventCollection (collectionName="%s.%d.%d.GEN" % (basename, i, j),
			      numberOfEvents=1, fileList=[f])
      files.append (f)
      evcs.append (e)

  print "Creating primary dataset %s" % primary
  api.createPrimaryDataset (primary)

  print "Creating processing %s" % processing
  api.createProcessing (processing)

  print "Creating file block %s" % block
  api.createFileBlock (block)

  print "Creating processed dataset %s" % gen
  api.createProcessedDataset (gen)

  for (files, evcs) in all:
    print "Inserting %d files into %s" % (len(files), block)
    api.insertFiles (block, files)

    print "Inserting %d evcs into %s" % (len(evcs), gen)
    api.insertEventCollections (gen, evcs)

except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
