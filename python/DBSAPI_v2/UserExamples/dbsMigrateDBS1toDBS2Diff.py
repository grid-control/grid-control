#!/usr/bin/env python
#
#
#

import os, re, string, xml.sax, xml.sax.handler, sys
from xml.sax.saxutils import escape
from cStringIO import StringIO

# DBS specific modules
from DBSAPI.dbsApi import DbsApi 
from DBSAPIOLD.dbsCgiApi import DbsCgiApi

from DBSAPIOLD.dbsException import DbsException
import DBSAPIOLD

from DBSAPI.dbsHttpService import DbsHttpService
from DBSAPI.dbsExecService import DbsExecService

from DBSAPI.dbsException import DbsException
from DBSAPI.dbsApiException import *

from DBSAPI.dbsBaseObject import *
from DBSAPI.dbsConfig import DbsConfig
from DBSAPI.dbsOptions import DbsOptionParser

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)
DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"
args = {}
args['instance'] = 'MCGlobal/Writer'

datasetDBS2 = api.listProcessedDatasets()
datasetDBS2Paths = []
for i in datasetDBS2:
	for j in  i['PathList']:
		datasetDBS2Paths.append(j)

datasetPattern = "/*/*/*"
cgiApi = DbsCgiApi(DEFAULT_URL, args)
datasetDBS1 = cgiApi.listProcessedDatasets ("/*/*/*")
out = open("DatasetToTransferFromDBS1ToDBS2.txt", "w")
for i in datasetDBS1:
	pathDBS1 = i['datasetPathName']
	tmpPath = pathDBS1.split('/')
	path = '/' + tmpPath[1] + '/' + tmpPath[3] + '/' + tmpPath[2]
	if(path in datasetDBS2Paths):
		print pathDBS1 + " Already exists in DBS2"
	else:
		out.write(pathDBS1 + "\n")
		print pathDBS1 + " Marked for migration"
out.close()
print "Done"

