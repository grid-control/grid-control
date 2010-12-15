import os, fnmatch, operator
from grid_control import QM, utils, ConfigError, storage, JobSelector, AbstractObject, Config
from provider_base import DataProvider
from python_compat import *
from scanner_basic import *

class ScanProviderBase(DataProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, section, '', datasetNick, datasetID)
		self.nameDS = self.setup(config.get, section, 'dataset name pattern', '', noVar = False)
		self.nameB = self.setup(config.get, section, 'block name pattern', '', noVar = False)
		self.kUserDS = self.setup(config.getList, section, 'dataset hash keys', [])
		self.kUserB = self.setup(config.getList, section, 'block hash keys', [])
		self.kGuardDS = self.setup(config.getList, section, 'dataset guard override', [])
		self.kGuardB = self.setup(config.getList, section, 'block guard override', [])
		scanList = self.setup(config.getList, section, 'scanner', datasetExpr)
		self.scanner = map(lambda cls: InfoScanner.open(cls, self.setup, config, section), scanList)


	def collectFiles(self):
		def recurse(collectorList, args):
			if len(collectorList):
				for data in recurse(collectorList[:-1], args):
					for (path, metadata, nEvents, seList, objStore) in collectorList[-1](*data):
						yield (path, dict(metadata), nEvents, seList, objStore)
			else:
				yield args
		return recurse(map(lambda x: x.getEntriesVerbose, self.scanner), (None, {}, None, None, {}))


	def generateKey(self, keys, base, path, metadata, events, seList, objStore):
		return md5(repr(base) + repr(seList) + repr(map(lambda k: metadata.get(k, None), keys))).hexdigest()


	def generateDatasetName(self, key, data):
		data['DS_KEY'] = key
		if 'SE_OUTPUT_BASE' in data:
			return utils.replaceDict(QM(self.nameDS, self.nameDS, '/PRIVATE/@SE_OUTPUT_BASE@'), data)
		return utils.replaceDict(QM(self.nameDS, self.nameDS, '/PRIVATE/Dataset_%s' % key), data)


	def generateBlockName(self, key, data):
		data['BLOCK_KEY'] = key
		return utils.replaceDict(QM(self.nameB, self.nameB, key[:8]), data)


	def getBlocksInternal(self):
		# Split files into blocks/datasets via key functions and determine metadata intersection
		(protoBlocks, commonDS, commonB) = ({}, {}, {})
		def getActiveKeys(kUser, kGuard, gIdx):
			return kUser + QM(kGuard, kGuard, reduce(operator.add, map(lambda x: x.getGuards()[gIdx], self.scanner)))
		keysDS = getActiveKeys(self.kUserDS, self.kGuardDS, 0)
		keysB = getActiveKeys(self.kUserB, self.kGuardB, 1)
		for fileInfo in self.collectFiles():
			hashDS = self.generateKey(keysDS, None, *fileInfo)
			hashB = self.generateKey(keysB, hashDS, *fileInfo)
			protoBlocks.setdefault(hashDS, {}).setdefault(hashB, []).append(fileInfo)
			infoSet = set(utils.filterDict(fileInfo[1], vF = lambda v: isinstance(v, str)).items())
			commonDS.setdefault(hashDS, set(infoSet)).intersection_update(infoSet)
			commonB.setdefault(hashDS, {}).setdefault(hashB, set(infoSet)).intersection_update(infoSet)

		# Generate names for blocks/datasets using common metadata
		(hashNameDictDS, hashNameDictB) = ({}, {})
		for hashDS in protoBlocks:
			hashNameDictDS[hashDS] = self.generateDatasetName(hashDS, dict(commonDS[hashDS]))
			for hashB in protoBlocks[hashDS]:
				hashNameDictB[hashB] = (hashDS, self.generateBlockName(hashB, dict(commonB[hashDS][hashB])))

		# Find name <-> key collisions
		def findCollision(tName, nameDict, varDict, hashKeys, keyFmt = lambda x: x):
			targetNames = nameDict.values()
			for name in list(set(targetNames)):
				targetNames.remove(name)
			if len(targetNames):
				ask = True
				for name in targetNames:
					utils.eprint("Multiple %s keys are mapped to the same %s name '%s'!" % (tName, tName, keyFmt(name)))
					for key in nameDict:
						if nameDict[key] == name:
							utils.eprint('\t%s hash %s using:' % (tName, keyFmt(key)))
							for x in filter(lambda (k,v): k in hashKeys, varDict[keyFmt(key)]):
								utils.eprint('\t\t%s = %s' % x)
					if ask and not utils.getUserBool('Do you want to continue?', False):
						exit(0)
					ask = False
		findCollision('dataset', hashNameDictDS, commonDS, keysDS)
		findCollision('block', hashNameDictB, commonB, keysB, lambda x: x[1])

		# Return named dataset
		for hashDS in protoBlocks:
			for hashB in protoBlocks[hashDS]:
				seList = list(set(map(lambda x: x[3], protoBlocks[hashDS][hashB])))
				seList = QM(seList == [None], None, seList)
				metaKeys = protoBlocks[hashDS][hashB][0][1].keys()
				fnProps = lambda (path, metadata, events, seList, objStore): {
					DataProvider.lfn: path, DataProvider.NEvents: events,
					DataProvider.Metadata: map(lambda x: metadata.get(x), metaKeys)}
				yield {
					DataProvider.Dataset: hashNameDictDS[hashDS],
					DataProvider.BlockName: hashNameDictB[hashB][1],
					DataProvider.SEList: seList,
					DataProvider.Metadata: metaKeys,
					DataProvider.FileList: map(fnProps, protoBlocks[hashDS][hashB])
				}


# Get dataset information from storage url
# required format: <storage url>
class ScanProvider(ScanProviderBase):
	DataProvider.providers.update({'ScanProvider': 'gc'})
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		config.set(section, 'source directory', datasetExpr)
		defScanner = ['FilesFromLS', 'MatchOnFilename', 'MatchDelimeter', 'DetermineEvents']
		ScanProviderBase.__init__(self, config, section, defScanner, datasetNick, datasetID)


# Get dataset information just from grid-control instance
# required format: <path to config file / workdir> [%<job selector]
class GCProvider(ScanProviderBase):
	DataProvider.providers.update({'GCProvider': 'gc'})
	stageDir = {}
	stageFile = {None: ['MatchOnFilename', 'MatchDelimeter']}

	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		if os.path.isdir(datasetExpr):
			GCProvider.stageDir[None] = ['OutputDirsFromWork']
			config.set(section, 'source directory', datasetExpr)
			datasetExpr = os.path.join(datasetExpr, 'work.conf')
		else:
			GCProvider.stageDir[None] = ['OutputDirsFromConfig', 'MetadataFromModule']
			config.set(section, 'source config', datasetExpr)
		extConfig = Config(datasetExpr)
		extModule = extConfig.get('global', 'module')
		if 'ParaMod' in extModule:
			extModule = extConfig.get('ParaMod', 'module')
		sGet = lambda scannerDict: scannerDict.get(None) + scannerDict.get(extModule, [])
		sList = sGet(GCProvider.stageDir) + ['FilesFromJobInfo'] + sGet(GCProvider.stageFile) + ['DetermineEvents']
		ScanProviderBase.__init__(self, config, section, sList, datasetNick, datasetID)
