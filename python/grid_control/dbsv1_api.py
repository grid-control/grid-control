import sys, os
from grid_control import DataDiscovery, RuntimeError, utils

__all__ = ['DBSApiv1']

# import stuff from DLSAPI, DBSAPI temporarily, which are incorrectly packaged
for dir in ('DLSAPI_v1', 'DBSAPI_v1'):
	sys.path.append(os.path.join(utils.getRoot(), 'python', dir))

import dlsApi
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
import dlsClient
import dbsCgiApi


class DBSApiv1(DataDiscovery):
	def __init__(self, datasetPath):
		self.dlsIface = dlsClient.DLS_TYPE_LFC
		self.dlsEndpoint = 'prod-lfc-cms-central.cern.ch/grid/cms/DLS/LFC'
		self.dbsUrl = 'http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery'
		self.dbsInstance = 'MCGlobal/Writer'
		self.datasetPath = datasetPath






	def run(self):
		dbs = dbsCgiApi.DbsCgiApi(self.dbsUrl, { 'instance': self.dbsInstance })
		dls = dlsClient.getDlsApi(self.dlsIface, self.dlsEndpoint)

		datasets = dbs.listProcessedDatasets(self.datasetPath)
		if len(datasets) == 0:
			raise RuntimeError('No dataset found')
		elif len(datasets) > 1:
			raise RuntimeError('Ambiguous dataset name')

		datasetPath = datasets[0].get('datasetPathName')

		contents = dbs.getDatasetContents(datasetPath)

		self.collections = []





		self.datasetBlockInfo = {}
		self.datasetBlockInfo['NumberOfEvents'] = None
		self.datasetBlockInfo['NumberOfFiles'] = None
		self.datasetBlockInfo['StorageElementList'] = []


		for fileBlock in contents:
			blockName = fileBlock.get('blockName')
			for entry in dls.getLocations([ blockName ]):
				self.datasetBlockInfo['StorageElementList'].extend(map(lambda x: str(x.host), entry.locations))

			self.collections.extend(fileBlock.get('eventCollectionList'))


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


		


