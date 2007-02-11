#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from dbsCgiApi import DbsCgiApi, DbsCgiObjectExists
from dbsException import DbsException
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsEventCollection import DbsEventCollection
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsProcessedDataset import DbsProcessedDataset
from dbsProcessing import DbsProcessing
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

#DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"
#DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
#DEFAULT_URL = "exec:/home/sekhri/cgi/java/test/in.sh"

#DEFAULT_URL = "exec:../CGIServer/prodquery"

try:
  args = {}
  if len(sys.argv) == 2: args['instance'] = sys.argv[1]
  api = DbsCgiApi(DEFAULT_URL, args)

  #api.setLogLevel(DBS_LOG_LEVEL_ALL_)
  # api.setDebug(1)

  # Attempt to create a primary dataset
  
  print ""
  primary = DbsPrimaryDataset (datasetName = "test_primary_anzar")
  print "Creating primary dataset %s" % primary
  
  try:
    api.createPrimaryDataset (primary)
    print "Result: %s" % primary
  except DbsCgiObjectExists, ex:
    print "Object existed already, passing"
  
  
  # Attempt to create a processing
  print ""
  processing = DbsProcessing (primaryDataset = primary,
		      	      processingName = "test_process_anzar",
			      applicationConfig = {
			        'application' : { 'executable' : 'testexe',
				 		  'version' : 'test',
						  'family' : 'test' },
				'parameterSet' : { 'hash' : 'test',
				 		   'content' : 'test' }})
  print "Creating processing %s" % processing
  try:
    api.createProcessing (processing)
    print "Result: %s" % processing
  except DbsCgiObjectExists, ex:
    print "Object existed already, passing"


  # Attempt to create a file block
  print ""
  block = DbsFileBlock (processing = processing)
  print "Creating file block %s" % block
  try:
    api.createFileBlock (block)
    print "Result: %s" % block
    print "Block creation commented out!"
  except DbsCgiObjectExists, ex:
    print "Object existed already, passing"

  # Attempt to create hits, digi and dst datasets
  print ""

  hits = DbsProcessedDataset (primaryDataset=primary,
		              datasetName="test_process_anzar",
			      dataTier="Hit")
  digi = DbsProcessedDataset (primaryDataset=primary,
		              datasetName="test_process_anzar",
			      dataTier="Digi")
  dst = DbsProcessedDataset (primaryDataset=primary,
		             datasetName="test_process_anzar",
			     dataTier="DST")
  print "Creating datasets\n %s\n %s\n %s" % (hits, digi, dst)
  try:
    pass
    api.createProcessedDataset (hits)
    api.createProcessedDataset (digi)
    api.createProcessedDataset (dst)
  except DbsCgiObjectExists, ex:
    print "Object existed already, passing"
  
  # Attempt to insert a file
  print "trying to create file object"
  file = DbsFile (logicalFileName="LFN1xyz", fileSize=long(1),
		  checkSum="cksum:1", fileType="EVD")
  print "Inserting file %s" % file
  try:
    pass
    api.insertFiles (block, [ file ])
  except DbsCgiObjectExists, ex:
    print "Object existed already, passing"
  
  # Attempt to insert hit and digi event collections
  print "creating ev object"
  #ech = DbsEventCollection (collectionName="HC2", numberOfEvents=long(1), collectionStatus='NEW', fileList=[file])
  ech = DbsEventCollection (collectionName="HC2", numberOfEvents=long(1),  fileList=[file])
  ecd = DbsEventCollection (collectionName="DC2", numberOfEvents=long(1), fileList=[file],
			    parentageList=[ { 'parent' : ech, 'type' : 'Hit' } ])
  ecs = DbsEventCollection (collectionName="SC2", numberOfEvents=long(1), fileList=[file],
		      	    parentageList=[ { 'parent' : ecd, 'type' : 'Digi' } ])
  print "Inserting event collections\n %s\n %s\n %s" % (ech, ecd, ecs)
  try:
    api.insertEventCollections (hits, [ ech ])
    api.insertEventCollections (digi, [ ecd ])
    api.insertEventCollections (dst, [ ecs ])
  except DbsCgiObjectExists, ex:
    print "Object existed already, passing"
  
except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
