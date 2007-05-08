#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from dbsCgiApi import DbsCgiApi
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"

try:
  args = {}
  if len(sys.argv) == 2: args['instance'] = sys.argv[1]
  api = DbsCgiApi(DEFAULT_URL, args)
  # api.setDebug(1)

  # List all datasets and count of files in all their blocks
  print "Listing datasets blocks/files"
  # For MCLocal_1/Writer
  dataset = "/test_primary_anzar/Hit/test_process_anzar"
  print api.getLFNs ( "/test_primary_anzar/test_process_anzar#2eaa3188-9f81-490c-9f20-e10ac49ef785", dataset)
  print api.getLFNs ( "/test_primary_anzar/test_process_anzar#2eaa3188-9f81-490c-9f20-e10ac49ef785", "")
  print api.getLFNs ( "/test_primary_anzar/test_process_anzar#2eaa3188-9f81-490c-9f20-e10ac49ef785")
  print api.getLFNs ( "/test_primary_anzar/test_process_anzar#2eaa3188-9f81-490c-9f20-e10ac49ef785"," ")

  # For DevMC/Writer
  """
  dataset = "/CSA06-083-os-EWKSoup/DIGI/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged"
  print api.getLFNs ( "/CSA06-083-os-EWKSoup/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged#502ac997-9a64-4bb3-8279-3c6328f35f54", dataset)
  print api.getLFNs ( "/CSA06-083-os-EWKSoup/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged#502ac997-9a64-4bb3-8279-3c6328f35f54")
  """
except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
