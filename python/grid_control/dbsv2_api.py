from __future__ import generators
import sys, os, string, re, copy

from grid_control import DataDiscovery, RuntimeError, utils, DatasetError

import DBSAPI_v2.dbsApi
from DBSAPI_v2.dbsApiException import *
from DBSAPI_v2.dbsOptions import DbsOptionParser

class DBSApiv2(DataDiscovery):
	def __init__(self, datasetExpr):
		datasetExprList = string.split(datasetExpr,"#")
		if len(datasetExprList) > 2:
			raise ConfigError('dataset must have the format <dataset>#block or <dataset>')
		
		self.datasetPath  = datasetExprList[0]
		if len(datasetExprList) == 2:
			self.datasetBlock = datasetExprList[1]
		else:
			self.datasetBlock = "all"
		


		self.args = {}
		self.args['url']     = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
		self.args['version'] = "v00_00_06"
		self.args['level']   = "CRITICAL"



	def _getBlocks(self):
		api = DBSAPI_v2.dbsApi.DbsApi(self.args)
		listBlockInfo  = api.listBlocks(self.datasetPath)
		filelistInfo = api.listFiles(self.datasetPath)

		blocks = [];

		for block in listBlockInfo:
			if (self.datasetBlock == "all") or (string.split(block['Name'],"#")[1] ==  self.datasetBlock) :
				blockInfo = {}
				blockInfo['NumberOfEvents'] =  block['NumberOfEvents']
				blockInfo['NumberOfFiles'] = block['NumberOfFiles']	
				blockInfo['BlockName'] = string.split(block['Name'],"#")[1]

				blockInfo['StorageElementList'] = []
				for sename in block['StorageElementList']:
					blockInfo['StorageElementList'].append(sename['Name'])	


				blockInfo['FileList'] = []

				for entry in filelistInfo:
					if self.datasetPath+"#"+blockInfo['BlockName'] == entry['Block']['Name']:
						blockInfo['FileList'].append({'lfn'    :entry['LogicalFileName'],
									      'status' :entry['Status'],
									      'events' :entry['NumberOfEvents']
									      })
						
								


				blocks.append(copy.deepcopy(blockInfo))
				

		if len(blocks) == 0:
			raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)


		return blocks




