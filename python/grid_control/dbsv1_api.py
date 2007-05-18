import sys, os, string, re
from grid_control import DataDiscovery, RuntimeError, DatasetError, utils

__all__ = ['DBSApiv1']

# import stuff from DLSAPI, DBSAPI temporarily, which are incorrectly packaged
for dir in ('DLSAPI_v1', 'DBSAPI_v1'):
	sys.path.append(os.path.join(utils.getRoot(), 'python', dir))

import dlsApi
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
import dlsClient
import dbsCgiApi


class DBSApiv1(DataDiscovery):
	def __init__(self, datasetExpr):
		self.dlsIface = dlsClient.DLS_TYPE_LFC
		self.dlsEndpoint = 'prod-lfc-cms-central.cern.ch/grid/cms/DLS/LFC'
		self.dbsUrl = 'http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery'
		self.dbsInstance = 'MCGlobal/Writer'


		datasetExprList = string.split(datasetExpr,"#")
		if len(datasetExprList) != 2:
			raise ConfigError('dataset must have the format <dataset>#block or <dataset>#OnlySE:<SE-Name>')
		
		self.datasetPath  = datasetExprList[0]
		self.datasetBlock = datasetExprList[1]


	def run(self):
		dbs = dbsCgiApi.DbsCgiApi(self.dbsUrl, { 'instance': self.dbsInstance })
		dls = dlsClient.getDlsApi(self.dlsIface, self.dlsEndpoint)


		testSE = re.compile("OnlySE:")
		selectionType = ""
		if testSE.search(self.datasetBlock):
			selectionType = "SeBased"
		else:
			selectionType = "BlockBased"



		datasets = dbs.listProcessedDatasets(self.datasetPath)
		if len(datasets) == 0:
			raise RuntimeError('No dataset found')
		elif len(datasets) > 1:
			raise RuntimeError('Ambiguous dataset name')




		datasetPath = datasets[0].get('datasetPathName')
      		contents = dbs.getDatasetContents(datasetPath)


		if selectionType == "SeBased":
			targetSE = testSE.sub("",self.datasetBlock)


		self.collections = []

		self.datasetBlockInfo = {}
		self.datasetBlockInfo['NumberOfEvents'] = 0
		self.datasetBlockInfo['NumberOfFiles'] = 0
		self.datasetBlockInfo['StorageElementList'] = []


		for fileBlock in contents:
			blockName = fileBlock.get('blockName')
			if selectionType == "BlockBased":
				if string.split(blockName,"#")[1] == self.datasetBlock:
					for entry in dls.getLocations([ blockName ]):
						self.datasetBlockInfo['StorageElementList'].extend(map(lambda x: str(x.host), entry.locations))

					self.collections.extend(fileBlock.get('eventCollectionList'))

			elif selectionType == "SeBased":
				for entry in dls.getLocations([ blockName ]):
					if map(lambda x: str(x.host), entry.locations).count(targetSE) > 0:
						self.collections.extend(fileBlock.get('eventCollectionList'))
					self.datasetBlockInfo['StorageElementList']=[targetSE]
			else:
				raise DatasetError('selectionType undefined')


				
		if len(self.collections) == 0:
			raise DatasetError('Block %s not found in dbs.' % self.datasetExpr)

		self.filelist = [];
                for entry in self.collections:
			if len(entry['fileList']) != 1:
				raise DatasetError('More than one entry found for fileList')
			else :
				fileAtt=entry['fileList'][0]

			self.filelist.append({'lfn'    :fileAtt['logicalFileName'],
					      'status' :fileAtt['fileStatus'],
					      'events' :entry['numberOfEvents']
					      })

			self.datasetBlockInfo['NumberOfEvents']+=entry['numberOfEvents']
			self.datasetBlockInfo['NumberOfFiles']+=1


