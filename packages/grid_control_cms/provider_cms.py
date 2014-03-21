from grid_control import QM, utils, datasets, DatasetError, ConfigError
from grid_control.datasets import DataProvider, HybridSplitter, DataSplitter
from python_compat import set, sorted
from lumi_tools import *
from webservice_api import *

# required format: <dataset path>[@<instance>][#<block>]
class CMSProvider(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		# PhEDex blacklist: 'T1_DE_KIT', 'T1_US_FNAL' and '*_Disk' allow user jobs - other T1's dont!
		self.phedexBL = config.getList('phedex sites', ['-T3_US_FNALLPC'])
		self.phedexWL = config.getList('phedex t1 accept', ['T1_DE_KIT', 'T1_US_FNAL'])
		self.phedexT1 = config.get('phedex t1 mode', 'disk').lower()
		self.onlyComplete = config.getBool('only complete sites', True)
		self.locationFormat = config.get('location format', 'hostname').lower() # hostname or sitedb
		if self.locationFormat not in ['hostname', 'sitedb', 'both']:
			raise ConfigError('Invalid location format: %s' % self.locationFormat)

		(self.datasetPath, self.url, self.datasetBlock) = utils.optSplit(datasetExpr, '@#')
		self.url = QM(self.url, self.url, config.get('dbs instance', ''))
		self.datasetBlock = QM(self.datasetBlock, self.datasetBlock, 'all')
		self.includeLumi = config.getBool('keep lumi metadata', False)
		self.onlyValid = config.getBool('only valid', True)
		self.checkUnique = config.getBool('check unique', True)

		# This works in tandem with active task module (cmssy.py supports only [section] lumi filter!)
		self.selectedLumis = parseLumiFilter(config.get('lumi filter', ''))
		if self.selectedLumis:
			utils.vprint('Runs/lumi section filter enabled! (%d entries)' % len(self.selectedLumis), -1, once = True)
			utils.vprint('\tThe following runs and lumi sections are selected:', 1, once = True)
			utils.vprint('\t' + utils.wrapList(formatLumi(self.selectedLumis), 65, ',\n\t'), 1, once = True)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 2 * 60 * 60 # 2 hour delay minimum


	# Check if splitterClass is valid
	def checkSplitter(self, splitterClass):
		if self.selectedLumis and (DataSplitter.Skipped in splitterClass.neededVars()):
			utils.vprint('Active lumi section filter forced selection of HybridSplitter', -1, once = True)
			return HybridSplitter
		return splitterClass


	def blockFilter(self, blockname):
		return (self.datasetBlock == 'all') or (str.split(blockname, '#')[1] == self.datasetBlock)


	def lumiFilter(self, lumilist, runkey, lumikey):
		if self.selectedLumis:
			for lumi in lumilist:
				if selectLumi((lumi[runkey], lumi[lumikey]), self.selectedLumis):
					return True
		return self.selectedLumis == None


	def nodeFilter(self, nameSiteDB, complete):
		# Remove T0 and T1 by default
		result = not (nameSiteDB.startswith('T0_') or nameSiteDB.startswith('T1_'))
		# check if listed on the accepted list
		if self.phedexT1 in ['accept', 'disk']:
			result = result or (len(utils.filterBlackWhite([nameSiteDB], self.phedexWL)) != 0)
		if self.phedexT1 == 'disk':
			result = result or nameSiteDB.lower().endswith('_disk')
		# apply phedex blacklist
		result = result and (len(utils.filterBlackWhite([nameSiteDB], self.phedexBL)) != 0)
		# check for completeness at the site
		result = result and (complete or not self.onlyComplete)
		return result


	# Get dataset se list from PhEDex (perhaps concurrent with listFiles)
	def getPhedexSEList(self, blockPath, dictSE):
		dictSE[blockPath] = []
		url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas'
		for phedexBlock in readJSON(url, {'block': blockPath})['phedex']['block']:
			for replica in phedexBlock['replica']:
				if self.nodeFilter(replica['node'], replica['complete'] == 'y'):
					location = None
					if self.locationFormat == 'hostname':
						location = replica.get('se')
					elif self.locationFormat == 'sitedb':
						location = replica.get('node')
					elif self.locationFormat == 'both' and (replica.get('node') or replica.get('se')):
						location = '%s/%s' % (replica.get('node'), replica.get('se'))
					if location:
						dictSE[blockPath].append(location)
					else:
						utils.vprint('Warning: Dataset block %s replica at %s / %s is skipped!' %
							(blockPath, replica.get('node'), replica.get('se')) , -1)


	def getCMSDatasets(self):
		result = [self.datasetPath]
		if '*' in self.datasetPath:
			result = list(self.getCMSDatasetsImpl(self.datasetPath))
			if len(result) == 0:
				raise DatasetError('No datasets selected by DBS wildcard %s !' % self.datasetPath)
			utils.vprint('DBS dataset wildcard selected:\n\t%s\n' % str.join('\n\t', result), -1)
		return result # List of resolved datasetPaths


	def getCMSBlocks(self, datasetPath, getSites):
		result = self.getCMSBlocksImpl(datasetPath, getSites)
		result = filter(lambda b: self.blockFilter(b[0]), result)
		if len(result) == 0:
			raise DatasetError('Dataset %s does not contain any selected blocks!' % datasetPath)
		return result # List of (blockname, selist) tuples


	def getCMSFiles(self, blockPath):
		lumiDict = {}
		if self.selectedLumis: # Central lumi query
			lumiDict = self.getCMSLumisImpl(blockPath)
			lumiDict = QM(lumiDict, lumiDict, {})
		for (fileInfo, listLumi) in self.getCMSFilesImpl(blockPath, self.onlyValid, self.selectedLumis):
			if self.selectedLumis:
				if not listLumi:
					listLumi = lumiDict.get(fileInfo[DataProvider.URL], [])
				def acceptLumi():
					for (run, lumiList) in listLumi:
						for lumi in lumiList:
							if selectLumi((run, lumi), self.selectedLumis):
								return True
				if not acceptLumi():
					continue
				if self.includeLumi:
					(listLumiExt_Run, listLumiExt_Lumi) = ([], [])
					for (run, lumi_list) in sorted(listLumi):
						for lumi in lumi_list:
							listLumiExt_Run.append(run)
							listLumiExt_Lumi.append(lumi)
					fileInfo[DataProvider.Metadata] = [listLumiExt_Run, listLumiExt_Lumi]
				else:
					fileInfo[DataProvider.Metadata] = [list(sorted(set(map(lambda (run, lumi_list): run, listLumi))))]
			yield fileInfo


	def getCMSLumisImpl(self, blockPath):
		return None


	def getGCBlocks(self, usePhedex):
		blockCache = []
		for datasetPath in self.getCMSDatasets():
			counter = 0
			for (blockPath, listSE) in self.getCMSBlocks(datasetPath, getSites = not usePhedex):
				if blockPath in blockCache:
					raise DatasetError('CMS source provided duplicate blocks! %s' % blockPath)
				blockCache.append(blockPath)
				result = {}
				result[DataProvider.Dataset] = blockPath.split('#')[0]
				result[DataProvider.BlockName] = blockPath.split('#')[1]

				if usePhedex: # Start parallel phedex query
					dictSE = {}
					tPhedex = utils.gcStartThread("Query phedex site info for %s" % blockPath, self.getPhedexSEList, blockPath, dictSE)

				if self.selectedLumis:
					result[DataProvider.Metadata] = ['Runs']
					if self.includeLumi:
						result[DataProvider.Metadata].append('Lumi')
				result[DataProvider.FileList] = list(self.getCMSFiles(blockPath))
				if self.checkUnique:
					uniqueURLs = set(map(lambda x: x[DataProvider.URL], result[DataProvider.FileList]))
					if len(result[DataProvider.FileList]) != len(uniqueURLs):
						utils.vprint('Warning: The webservice returned %d duplicated files in dataset block %s! Continuing with unique files...' %
							(len(result[DataProvider.FileList]) - len(uniqueURLs)), -1)
					uniqueFIs = []
					for fi in result[DataProvider.FileList]:
						if fi[DataProvider.URL] in uniqueURLs:
							uniqueURLs.remove(fi[DataProvider.URL])
							uniqueFIs.append(fi)
					result[DataProvider.FileList] = uniqueFIs

				if usePhedex:
					tPhedex.join()
					listSE = dictSE.get(blockPath)
				result[DataProvider.Locations] = listSE

				if len(result[DataProvider.FileList]):
					counter += 1
					yield result

			if (counter == 0) and self.selectedLumis:
				raise DatasetError('Dataset %s does not contain the requested run/lumi sections!' % datasetPath)
			elif counter == 0:
				raise DatasetError('Dataset %s does not contain any valid blocks!' % datasetPath)
