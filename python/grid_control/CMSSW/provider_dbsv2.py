from grid_control import utils, DatasetError, datasets
from grid_control.datasets import DataProvider
from lumi_tools import *

def createDBSAPI(url):
	import DBSAPI.dbsApi
	if url == '':
		url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
	elif (not 'http://' in url) and (not 'https://' in url):
		url = 'http://cmsdbsprod.cern.ch/%s/servlet/DBSServlet' % url
	return DBSAPI.dbsApi.DbsApi({'version': 'DBS_2_0_6', 'level': 'CRITICAL', 'url': url})


# required format: <dataset path>[@<instance>][#<block>][%<run-lumis>]
class DBSApiv2(DataProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, section, datasetExpr, datasetNick, datasetID)
		DataProvider.providers.update({'DBSApiv2': 'dbs'})
		if config.getBool(section, 'dbs blacklist T1', True):
			T1SEs = ["-srmcms.pic.es", "-ccsrm.in2p3.fr", "-storm-fe-cms.cr.cnaf.infn.it",
				"-srm-cms.gridpp.rl.ac.uk", "-srm.grid.sinica.edu.tw", "-srm2.grid.sinica.edu.tw"]
			self.sitefilter.extend(T1SEs)

		self.url = config.get(section, 'dbs instance', '')
		(self.datasetPath, datasetUrl, self.datasetBlock) = utils.optSplit(datasetExpr, '@#')
		if not self.datasetBlock:
			self.datasetBlock = 'all'
		if datasetUrl != '':
			self.url = datasetUrl
		# This works in tandem with cmssw.py !
		self.selectedLumis = parseLumiFilter(config.get(section, 'lumi filter', ''))
		if self.selectedLumis:
			utils.vprint("The following runs and lumi sections are selected:", -1, once = True)
			for line in map(lambda x: str.join(', ', x), utils.lenSplit(formatLumi(self.selectedLumis), 65)):
				utils.vprint("\t%s" % line, -1, once = True)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 2 * 60 * 60 # 2 hour delay minimum


	# Check if splitter is valid
	def checkSplitter(self, splitter):
		if self.selectedLumis and splitter == 'EventBoundarySplitter':
			utils.vprint('Active lumi section filter forced selection of HybridSplitter', -1, once = True)
			return 'HybridSplitter'
		return splitter


	def getBlocksInternal(self):
		import DBSAPI.dbsApiException
		api = createDBSAPI(self.url)
		try:
			listBlockInfo = api.listBlocks(self.datasetPath)
			if self.selectedLumis:
				listFileInfo = api.listFiles(self.datasetPath, retriveList=['retrive_lumi'])
			else:
				listFileInfo = api.listFiles(self.datasetPath)
		except DBSAPI.dbsApiException.DbsException, ex:
			raise DatasetError('DBS exception\n%s: %s' % (ex.getClassName(), ex.getErrorMessage()))

		if len(listBlockInfo) == 0:
			raise DatasetError('Dataset %s has no registered blocks in dbs.' % self.datasetPath)
		if len(listFileInfo) == 0:
			raise DatasetError('Dataset %s has no registered files in dbs.' % self.datasetPath)

		def blockFilter(block):
			if self.datasetBlock == 'all':
				return True
			if str.split(block['Name'], '#')[1] == self.datasetBlock:
				return True
			return False

		def lumiFilter(lumilist):
			if self.selectedLumis == None:
				return True
			for lumi in lumilist:
				if selectLumi((lumi["RunNumber"], lumi["LumiSectionNumber"]), self.selectedLumis):
					return True
			return False

		result = []
		for block in filter(blockFilter, listBlockInfo):
			blockInfo = dict()
			blockInfo[DataProvider.Dataset] = str.split(block['Name'], '#')[0]
			blockInfo[DataProvider.BlockName] = str.split(block['Name'], '#')[1]

			blockInfo[DataProvider.SEList] = []
			for seName in block['StorageElementList']:
				blockInfo[DataProvider.SEList].append(seName['Name'])

			dropped = 0
			blockInfo[DataProvider.FileList] = []
			for entry in listFileInfo:
				if block['Name'] == entry['Block']['Name']:
					if lumiFilter(entry['LumiList']):
						blockInfo[DataProvider.FileList].append({
							DataProvider.lfn     : entry['LogicalFileName'],
							DataProvider.NEvents : entry['NumberOfEvents']
						})
					else:
						dropped += 1

			recordedFiles = len(blockInfo[DataProvider.FileList]) + dropped
			if recordedFiles != block['NumberOfFiles']:
				print('Inconsistency in dbs block %s: Number of files doesn\'t match (b:%d != f:%d)'
					% (block['Name'], block['NumberOfFiles'], recordedFiles))
			if dropped == 0:
				blockInfo[DataProvider.NEvents] = block['NumberOfEvents']
			if len(blockInfo[DataProvider.FileList]) > 0:
				result.append(blockInfo)

		if len(result) == 0:
			if self.selectedLumis:
				print "Dataset %s does not contain the requested run/lumi sections!" % self.datasetPath
			else:
				raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)
		return result
