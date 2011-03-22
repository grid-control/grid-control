from grid_control import QM, utils, DatasetError, RethrowError, datasets
from grid_control.datasets import DataProvider, HybridSplitter, DataSplitter
from lumi_tools import *
from python_compat import *

def createDBSAPI(url):
	import DBSAPI.dbsApi, sys, os
	sys.path.append(os.path.dirname(__file__))
	if url == '':
		url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
	elif (not 'http://' in url) and (not 'https://' in url):
		url = 'http://cmsdbsprod.cern.ch/%s/servlet/DBSServlet' % url
	return DBSAPI.dbsApi.DbsApi({'version': 'DBS_2_0_6', 'level': 'CRITICAL', 'url': url})


# required format: <dataset path>[@<instance>][#<block>]
class DBSApiv2(DataProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, section, datasetExpr, datasetNick, datasetID)
		# PhEDex blacklist: '-T1_DE_KIT', '-T1_US_FNAL' allow user jobs
		phedexBL = ['-T0_CH_CERN', '-T1_CH_CERN', '-T1_ES_PIC', '-T1_FR_CCIN2P3', '-T1_IT_CNAF', '-T1_TW_ASGC', '-T1_UK_RAL', '-T3_US_FNALLPC']
		self.phedexBL = self.setup(config.getList, section, 'phedex sites', phedexBL)
		self.onlyComplete = self.setup(config.getBool, section, 'phedex only complete', True)

		(self.datasetPath, self.url, self.datasetBlock) = utils.optSplit(datasetExpr, '@#')
		self.url = QM(self.url, self.url, self.setup(config.get, section, 'dbs instance', ''))
		self.datasetBlock = QM(self.datasetBlock, self.datasetBlock, 'all')
		self.phedex = self.setup(config.getBool, section, 'use phedex', self.url == '')

		# This works in tandem with active job module (cmssy.py supports only [section] lumi filter!)
		self.selectedLumis = parseLumiFilter(self.setup(config.get, section, 'lumi filter', ''))
		if self.selectedLumis:
			utils.vprint('The following runs and lumi sections are selected:', -1, once = True)
			utils.vprint(utils.wrapList(formatLumi(self.selectedLumis), 65, ',\n\t'), -1, once = True)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 2 * 60 * 60 # 2 hour delay minimum


	# Check if splitterClass is valid
	def checkSplitter(self, splitterClass):
		if self.selectedLumis and (DataSplitter.Skipped in splitterClass.neededVars()):
			utils.vprint('Active lumi section filter forced selection of HybridSplitter', -1, once = True)
			return HybridSplitter
		return splitterClass


	def getBlocksInternal(self):
		import urllib2, DBSAPI.dbsApi
		api = createDBSAPI(self.url)
		def getWithPhedex(listBlockInfo, seList):
			# Get dataset list from PhEDex (concurrent with listFiles)
			phedexArgFmt = lambda x: ('block=%s' % x['Name']).replace('/', '%2F').replace('#', '%23')
			phedexArg = str.join('&', map(phedexArgFmt, listBlockInfo))
			phedexData = urllib2.urlopen('https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas', phedexArg).read()
			if str(phedexData).lower().find('error') != -1:
				raise DatasetError("Phedex error '%s'" % phedexData)
			phedexDict = eval(compile(phedexData.replace('null','None'), '<string>', 'eval'))['phedex']['block']
			for phedexBlock in phedexDict:
				phedexSelector = lambda x: (x['complete'] == 'y') or not self.onlyComplete
				phedexSites = dict(map(lambda x: (x['node'], x['se']), filter(phedexSelector, phedexBlock['replica'])))
				phedexSitesOK = utils.doBlackWhiteList(phedexSites.keys(), self.phedexBL)
				seList[phedexBlock['name']] = map(lambda x: phedexSites[x], phedexSitesOK)
		try:
			toProcess = [self.datasetPath]
			if '*' in self.datasetPath:
				pd, sd, dt = (self.datasetPath.lstrip("/") + "/*/*/*").split("/")[:3]
				toProcess = map(lambda x: x.get("PathList", [])[-1], api.listProcessedDatasets(pd, dt, sd))
				utils.vprint("DBS dataset wildcard selected:\n\t%s\n" % str.join("\n\t", toProcess), -1)
			# Start thread to retrieve list of files
			(listBlockInfo, listFileInfo, seList) = ([], [], {})
			def listFileInfoThread(self, api, path, result):
				result.extend(api.listFiles(path, retriveList=QM(self.selectedLumis, ['retrive_lumi'], [])))
			for datasetPath in toProcess:
				thisBlockInfo = api.listBlocks(datasetPath, nosite = self.phedex)
				tFile = utils.gcStartThread("Retrieval of file infos for %s" % datasetPath,
					listFileInfoThread, self, api, datasetPath, listFileInfo)
				if self.phedex:
					getWithPhedex(thisBlockInfo, seList)
				tFile.join()
				listBlockInfo.extend(thisBlockInfo)
		except:
			raise RethrowError('DBS exception')

		if len(listBlockInfo) == 0:
			raise DatasetError('Dataset %s has no registered blocks in dbs.' % self.datasetPath)
		if len(listFileInfo) == 0:
			raise DatasetError('Dataset %s has no registered files in dbs.' % self.datasetPath)

		def blockFilter(block):
			return (self.datasetBlock == 'all') or (str.split(block['Name'], '#')[1] == self.datasetBlock)

		def lumiFilter(lumilist):
			if self.selectedLumis:
				for lumi in lumilist:
					if selectLumi((lumi['RunNumber'], lumi['LumiSectionNumber']), self.selectedLumis):
						return True
			return self.selectedLumis == None

		result = []
		for block in filter(blockFilter, listBlockInfo):
			blockInfo = dict()
			blockInfo[DataProvider.Dataset] = str.split(block['Name'], '#')[0]
			blockInfo[DataProvider.BlockName] = str.split(block['Name'], '#')[1]
			if self.phedex:
				blockInfo[DataProvider.SEList] = seList.get(block['Name'], [])
			else:
				blockInfo[DataProvider.SEList] = map(lambda x: x['Name'], block['StorageElementList'])

			dropped = 0
			blockInfo[DataProvider.FileList] = []
			if self.selectedLumis:
				blockInfo[DataProvider.Metadata] = ['Runs']
			for entry in listFileInfo:
				if block['Name'] == entry['Block']['Name']:
					if lumiFilter(entry['LumiList']):
						blockInfo[DataProvider.FileList].append({
							DataProvider.lfn      : entry['LogicalFileName'],
							DataProvider.NEvents  : entry['NumberOfEvents'],
							DataProvider.Metadata : [list(set(map(lambda x: int(x['RunNumber']), entry['LumiList'])))]
						})
					else:
						dropped += 1

			recordedFiles = len(blockInfo[DataProvider.FileList]) + dropped
			if recordedFiles != block['NumberOfFiles']:
				utils.eprint('Inconsistency in dbs block %s: Number of files doesn\'t match (b:%d != f:%d)'
					% (block['Name'], block['NumberOfFiles'], recordedFiles))
			if dropped == 0:
				blockInfo[DataProvider.NEvents] = block['NumberOfEvents']
			if len(blockInfo[DataProvider.FileList]) > 0:
				result.append(blockInfo)

		if len(result) == 0:
			if self.selectedLumis:
				utils.eprint('Dataset %s does not contain the requested run/lumi sections!' % self.datasetPath)
			else:
				raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)
		return result
