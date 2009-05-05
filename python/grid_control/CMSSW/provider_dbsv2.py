from __future__ import generators
import sys, os, string, re, copy
from grid_control import RuntimeError, utils, DatasetError
from provider_base import DataProvider

import DBSAPI_v2.dbsApi
from DBSAPI_v2.dbsApiException import *
from DBSAPI_v2.dbsOptions import DbsOptionParser

class DBSApiv2(DataProvider):
	def __init__(self, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, datasetExpr, datasetNick, datasetID)

		datasetExprList = datasetExpr.split("#")
		if len(datasetExprList) > 2:
			raise ConfigError('dataset must have the format <dataset>#block or <dataset>')
		self.datasetPath  = datasetExprList[0]
		if len(datasetExprList) == 2:
			self.datasetBlock = datasetExprList[1]
		else:
			self.datasetBlock = "all"

		self.args = {}
		self.args['url']     = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
		self.args['version'] = "DBS_1_0_7"
		self.args['level']   = "CRITICAL"


	def getBlocksInternal(self):
		api = DBSAPI_v2.dbsApi.DbsApi(self.args)
		try:
			listBlockInfo = api.listBlocks(self.datasetPath)
			listFileInfo = api.listFiles(self.datasetPath)
		except DbsException, ex:
			raise DatasetError("DBS exception\n%s: %s" % (ex.getClassName(), ex.getErrorMessage()))

		if len(listBlockInfo) == 0:
			raise DatasetError('Dataset %s has no registered blocks in dbs.' % self.datasetPath)
		if len(listFileInfo) == 0:
			raise DatasetError('Dataset %s has no registered files in dbs.' % self.datasetPath)

		def blockFilter(block):
			if (self.datasetBlock == "all"):
				return True
			if (string.split(block['Name'],"#")[1] == self.datasetBlock) :
				return True
			return False

		result = []
		for block in filter(blockFilter, listBlockInfo):
			blockInfo = dict()
			blockInfo[DataProvider.NEvents] = block['NumberOfEvents']
			blockInfo[DataProvider.Dataset] = string.split(block['Name'],"#")[0]
			blockInfo[DataProvider.BlockName] = string.split(block['Name'],"#")[1]

			blockInfo[DataProvider.SEList] = []
			for seName in block['StorageElementList']:
				blockInfo[DataProvider.SEList].append(seName['Name'])

			blockInfo[DataProvider.FileList] = []
			for entry in listFileInfo:
				if block['Name'] == entry['Block']['Name']:
					blockInfo[DataProvider.FileList].append({
						DataProvider.lfn     : entry['LogicalFileName'],
						DataProvider.NEvents : entry['NumberOfEvents']
					})

			if len(blockInfo[DataProvider.FileList]) != block['NumberOfFiles']:
				print('Inconsistency in dbs block %s: Number of files doesn\'t match (b:%d != f:%d)'
					% (block['Name'], block['NumberOfFiles'], len(blockInfo[DataProvider.FileList])))

			result.append(blockInfo)

		if len(result) == 0:
			raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)
		return result
