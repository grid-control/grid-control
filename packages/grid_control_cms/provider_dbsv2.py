from grid_control import QM, utils, DatasetError, RethrowError, datasets
from grid_control.datasets import DataProvider
from provider_cms import CMSProvider
from webservice_api import *

def createDBSAPI(url):
	import DBSAPI.dbsApi, sys, os
	sys.path.append(os.path.dirname(__file__))
	if url == '':
		url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
	elif (not 'http://' in url) and (not 'https://' in url):
		url = 'http://cmsdbsprod.cern.ch/%s/servlet/DBSServlet' % url
	return DBSAPI.dbsApi.DbsApi({'version': 'DBS_2_0_6', 'level': 'CRITICAL', 'url': url})


# required format: <dataset path>[@<instance>][#<block>]
class DBSApiv2(CMSProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		CMSProvider.__init__(self, config, section, datasetExpr, datasetNick, datasetID)
		self.phedex = config.getBool(section, 'use phedex', True) and (self.url == '')


	def getCMSDatasetsImpl(self, datasetPath):
		pd, sd, dt = (self.datasetPath.lstrip("/") + "/*/*/*").split("/")[:3]
		return map(lambda x: x.get("PathList", [])[-1], self.api.listProcessedDatasets(pd, dt, sd))


	def getCMSBlocksImpl(self, datasetPath, getSites):
		blockInfo = self.api.listBlocks(datasetPath, nosite = not getSites)
		return map(lambda x: (x['Name'], map(lambda s: s.get('Name'), x.get('StorageElementList', []))), blockInfo)


	def getCMSFilesImpl(self, blockPath, onlyValid, queryLumi):
		if blockPath not in self.fiCache:
			self.fiCache[blockPath] = []
			query = ['retrive_status'] + QM(queryLumi, ['retrive_lumi'], [])
			for fi in self.api.listFiles(blockPath.split('#')[0], retriveList = query):
				lumiList = map(lambda li: (int(li['RunNumber']), [int(li['LumiSectionNumber'])]), fi['LumiList'])
				tmp = ({DataProvider.URL: fi['LogicalFileName'], DataProvider.NEntries: int(fi['NumberOfEvents'])}, lumiList)
				self.fiCache.setdefault(fi['Block']['Name'], []).append(tmp)
		return self.fiCache[blockPath]


	def getBlocksInternal(self):
		(self.api, self.fiCache) = (createDBSAPI(self.url), {})
		return self.getGCBlocks(usePhedex = self.phedex)
