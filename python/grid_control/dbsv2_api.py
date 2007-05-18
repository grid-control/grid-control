from __future__ import generators
import sys, os, string, re

from grid_control import DataDiscovery, RuntimeError, utils, DatasetError

import DBSAPI_v2.dbsApi
from DBSAPI_v2.dbsApiException import *
from DBSAPI_v2.dbsOptions import DbsOptionParser

class DBSApiv2(DataDiscovery):
	def __init__(self, datasetExpr):
		datasetExprList = string.split(datasetExpr,"#")
		if len(datasetExprList) != 2:
			raise ConfigError('dataset must have the format <dataset>#block or <dataset>#OnlySE:<SE-Name>')
		
		self.datasetPath  = datasetExprList[0]
		self.datasetBlock = datasetExprList[1]
		


		self.args = {}
		self.args['url']     = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
		self.args['version'] = "v00_00_06"
		self.args['level']   = "CRITICAL"



	def run(self):
		api = DBSAPI_v2.dbsApi.DbsApi(self.args)
		listBlockInfo  = api.listBlocks(self.datasetPath)



		testSE = re.compile("OnlySE:")
		selectionType = ""
		if testSE.search(self.datasetBlock):
			selectionType = "SeBased"
		else:
			selectionType = "BlockBased"


		self.datasetBlockInfo = {}
		self.datasetBlockInfo['NumberOfEvents'] = 0
		self.datasetBlockInfo['NumberOfFiles'] = 0
		self.datasetBlockInfo['StorageElementList'] = []



		if selectionType == "SeBased":
			targetSE = testSE.sub("",self.datasetBlock)
			self.datasetBlock = []
			for block in listBlockInfo:
				for sename in block['StorageElementList']:
					if targetSE == sename['Name']:
						self.datasetBlock.append(block['Name'])
						break
			if len(self.datasetBlock) > 0:
				self.datasetBlockInfo['StorageElementList'].append(targetSE)
			else:
				raise DatasetError('No block found in dbs for %s .' % targetSE)	

		else:
			self.datasetBlock = ["%s#%s" % (self.datasetPath,self.datasetBlock)]
			
		

			
		for block in listBlockInfo:
			if self.datasetBlock.count(block['Name']) > 0:
				self.datasetBlockInfo['NumberOfEvents'] += block['NumberOfEvents']
				self.datasetBlockInfo['NumberOfFiles'] += block['NumberOfFiles']
				if selectionType == "BlockBased":
					for sename in block['StorageElementList']:
						self.datasetBlockInfo['StorageElementList'].append(sename['Name'])	


		
		


		if self.datasetBlockInfo.has_key('NumberOfFiles')== False:
			raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)




		NumOfEventsFromFiles = 0;
		NumOfFilesfromFiles  = 0;
		filelistInfo = api.listFiles(self.datasetPath)
                self.filelist = [];
		for entry in filelistInfo:
			if self.datasetBlock.count(entry['Block']['Name']) > 0:
				self.filelist.append({'lfn'    :entry['LogicalFileName'],
						      'status' :entry['Status'],
						      'events' :entry['NumberOfEvents']
						      })
				NumOfEventsFromFiles+=entry['NumberOfEvents']
				NumOfFilesfromFiles+=1
				

		if NumOfEventsFromFiles != self.datasetBlockInfo['NumberOfEvents'] :
			raise DatasetError('Number of events from block info and file info differ.')
		
		if NumOfFilesfromFiles != self.datasetBlockInfo['NumberOfFiles'] :
			raise DatasetError('Number of files from block info and number of files differ.')


