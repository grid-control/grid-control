from __future__ import generators
import sys, os
from grid_control import DataDiscovery, RuntimeError, utils, DatasetError

import DBSAPI_v2.dbsApi
from DBSAPI_v2.dbsApiException import *
from DBSAPI_v2.dbsOptions import DbsOptionParser

class DBSApiv2(DataDiscovery):
	def __init__(self, datasetPath):
		self.datasetPath = datasetPath
		
		self.args = {}
		self.args['url']     = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
		self.args['version'] = "v00_00_06"
		self.args['level']   = "CRITICAL"






	def run(self):
		api = DBSAPI_v2.dbsApi.DbsApi(self.args)
		listBlockInfo  = api.listBlocks(self.datasetPath)

#		print listBlockInfo
#		if len(listBlockInfo) != 1:
#			raise DatasetError('More than one block for dataset found')


		self.datasetBlockInfo = {}
#		self.datasetBlockInfo['NumberOfEvents'] = listBlockInfo[0]['NumberOfEvents']
#		self.datasetBlockInfo['NumberOfFiles'] = listBlockInfo[0]['NumberOfFiles']
		self.datasetBlockInfo['StorageElementList'] = []
		for sename in listBlockInfo[0]['StorageElementList']:
			self.datasetBlockInfo['StorageElementList'].append(sename['Name'])

	

		filelistInfo = api.listFiles(self.datasetPath)
                self.filelist = [];
		for entry in filelistInfo:
			self.filelist.append({'lfn'    :entry['LogicalFileName'],
					      'status' :entry['Status'],
					      'events' :entry['NumberOfEvents']
					      })



