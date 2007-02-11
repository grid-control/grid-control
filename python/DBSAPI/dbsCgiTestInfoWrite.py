#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.

import sys
from dbsCgiApi import DbsCgiApi, DbsCgiDatabaseError
from dbsException import DbsException
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

#DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
#DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/dbsxml"
#DEFAULT_URL = "exec:../../Servers/CGIServer/prodquery"
DEFAULT_URL = "exec:../../Servers/CGIServer/prodquerytest2"

try:
  args = {}
  args1 = {}
  if (len(sys.argv) == 3) | (len(sys.argv) == 2) : args['instance'] = sys.argv[1]
  api = DbsCgiApi(DEFAULT_URL, args)
  if len(sys.argv) == 3: 
	  args1['instance'] = sys.argv[2]
  else :
	  args1['instance'] = sys.argv[1]
  api1 = DbsCgiApi(DEFAULT_URL, args1)
  #api.setLogLevel(DBS_LOG_LEVEL_ALL_)
  # api.setDebug(1)
  try:
   pathList = [ 
                "/CSA06-081-os-minbias/GEN/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged" ,
                "/CSA06-081-os-minbias/SIM/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged" ,
                "/CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged" ,
        ] # for MCLocal_3/Writer

   pathList = [ "/CSA06-082-os-ZMuMu/GEN/CMSSW_0_8_2-GEN-SIM-DIGI-1155826058-merged",
                "/CSA06-082-os-ZMuMu/SIM/CMSSW_0_8_2-GEN-SIM-DIGI-1155826058-merged",
                "/CSA06-082-os-ZMuMu/DIGI/CMSSW_0_8_2-GEN-SIM-DIGI-1155826058-merged"
   ]

   pathList = [ "/CSA06-083-os-SoftMuon/GEN/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877643-merged",
                "/CSA06-083-os-SoftMuon/SIM/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877643-merged",
                "/CSA06-083-os-SoftMuon/DIGI/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877643-merged",
                "/CSA06-083-os-SoftMuon/HLT/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877643-merged"
   ]
   pathList = [ "/CSA06-083-os-ExoticSoup/GEN/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877641-merged",
                "/CSA06-083-os-ExoticSoup/SIM/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877641-merged",
                "/CSA06-083-os-ExoticSoup/DIGI/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877641-merged",
                "/CSA06-083-os-ExoticSoup/HLT/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877641-merged"
   ]

   pathList = [ "/CSA06-083-os-EWKSoup/GEN/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged",
                "/CSA06-083-os-EWKSoup/SIM/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged",
                "/CSA06-083-os-EWKSoup/DIGI/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged",
                "/CSA06-083-os-EWKSoup/HLT/CMSSW_0_8_3-GEN-SIM-DIGI-HLT-1156877645-merged"
   ]

   pathList = [ "/CSA06-082-os-TTbar/GEN/CMSSW_0_8_2-GEN-SIM-DIGI-1155826011-merged",
                "/CSA06-082-os-TTbar/SIM/CMSSW_0_8_2-GEN-SIM-DIGI-1155826011-merged",
                "/CSA06-082-os-TTbar/DIGI/CMSSW_0_8_2-GEN-SIM-DIGI-1155826011-merged"
   ]



   for path in pathList:
     print path
     name = args['instance'].replace('/','_') + "_" + args1['instance'].replace('/', '_') + path.replace('/', '_')
     f = open(name +".xml");
     xmlinput = f.read()
     f.close()
     flog =  open(name +".log", "w");
     flog.write(api1.insertDatasetInfo(xmlinput))
     flog.close()  
  except DbsCgiDatabaseError,e:
   print e
 
except InvalidDataTier, ex:
  print "Caught InvalidDataTier API exception: %s" % (ex.getErrorMessage())
except DbsApiException, ex:
  print "Caught API exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
except DbsException, ex:
  print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())

print "Done"
