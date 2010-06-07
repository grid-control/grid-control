
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListDatasetFiles(self, datasetPath):
   """
	Checks to see if input datasetPath satisfies the PATH naming convention, otherwise
	its is ASSUMED that provided datasetPath is an AnalysisDataset Name
   """
   inpath=datasetPath[1:]
   if inpath.endswith('/'):
      inpath=inpath[:-1]
   pathl = inpath.split('/')


   allowedRetriveValue = [
                    #'all',
                    #'retrive_invalid_files', 
                    #'retrive_status',
                    #'retrive_type',
                    #'retrive_block',
                    #'retrive_date',
                    #'retrive_person',
                    #'retrive_parent',
                    #'retrive_child',
                    #'retrive_algo',
                    #'retrive_tier',
                    'retrive_lumi',
                    'retrive_run',
                    #'retrive_branch',
                    ]


   if len(pathl) == 3: # Most most probably this is Processed Dataset Path
   	return self.listFiles(path=datasetPath)
   else:
	return self.listFiles(analysisDataset=datasetPath, retriveList=allowedRetriveValue)
	


