# | Copyright 2010-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys
from grid_control import utils
from grid_control.config import createConfig
from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.utils.gc_itertools import lchain
from python_compat import identity, ifilter, imap, lmap, lsmap, md5_hex, set

class ScanProviderBase(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		DataProvider.__init__(self, config, '', datasetNick, datasetID)
		def DSB(cFun, n, *args, **kargs):
			return (cFun('dataset %s' % n, *args, **kargs), cFun('block %s' % n, *args, **kargs))
		(self.nameDS, self.nameB) = DSB(config.get, 'name pattern', '')
		(self.kUserDS, self.kUserB) = DSB(config.getList, 'hash keys', [])
		(self.kGuardDS, self.kGuardB) = DSB(config.getList, 'guard override', [])
		self.kSelectDS = config.getList('dataset key select', [])
		scanList = config.getList('scanner', datasetExpr) + ['NullScanner']
		self.scanner = lmap(lambda cls: InfoScanner.createInstance(cls, config), scanList)


	def _collectFiles(self):
		def recurse(level, collectorList, args):
			if collectorList:
				for data in recurse(level - 1, collectorList[:-1], args):
					for (path, metadata, nEvents, seList, objStore) in collectorList[-1](level, *data):
						yield (path, dict(metadata), nEvents, seList, objStore)
			else:
				yield args
		return recurse(len(self.scanner), lmap(lambda x: x.getEntriesVerbose, self.scanner), (None, {}, None, None, {}))


	def _generateKey(self, keys, base, path, metadata, events, seList, objStore):
		return md5_hex(repr(base) + repr(seList) + repr(lmap(metadata.get, keys)))


	def _generateDatasetName(self, key, data):
		if 'SE_OUTPUT_BASE' in data:
			return utils.replaceDict(self.nameDS or '/PRIVATE/@SE_OUTPUT_BASE@', data)
		return utils.replaceDict(self.nameDS or ('/PRIVATE/Dataset_%s' % key), data)


	def _generateBlockName(self, key, data):
		return utils.replaceDict(self.nameB or key[:8], data)


	def _getFilteredVarDict(self, varDict, varDictKeyComponents, hashKeys):
		tmp = varDict
		for key_component in varDictKeyComponents:
			tmp = tmp[key_component]
		result = {}
		for key, value in ifilter(lambda k_v: k_v[0] in hashKeys, tmp.items()):
			result[key] = value
		return result


	# Find name <-> key collisions
	def _findCollision(self, tName, nameDict, varDict, hashKeys, keyFmt, nameFmt = identity):
		dupesDict = {}
		for (key, name) in nameDict.items():
			dupesDict.setdefault(nameFmt(name), []).append(keyFmt(name, key))
		ask = True
		for name, key_list in dupesDict.items():
			if len(key_list) > 1:
				self._log.critical('Multiple %s keys are mapped to the name %s!', tName, repr(name))
				for key in key_list:
					self._log.critical('\t%s hash %s using:', tName, str.join('#', key))
					for var, value in self._getFilteredVarDict(varDict, key, hashKeys).items():
						self._log.critical('\t\t%s = %s', var, value)
				if ask and not utils.getUserBool('Do you want to continue?', False):
					sys.exit(os.EX_OK)
				ask = False


	def _buildBlocks(self, protoBlocks, hashNameDictDS, hashNameDictB):
		# Return named dataset
		for hashDS in protoBlocks:
			for hashB in protoBlocks[hashDS]:
				blockSEList = None
				for seList in ifilter(lambda s: s is not None, imap(lambda x: x[3], protoBlocks[hashDS][hashB])):
					blockSEList = blockSEList or []
					blockSEList.extend(seList)
				if blockSEList is not None:
					blockSEList = list(set(blockSEList))
				metaKeys = protoBlocks[hashDS][hashB][0][1].keys()
				def fnProps(path, metadata, events, seList, objStore):
					return {DataProvider.URL: path, DataProvider.NEntries: events,
						DataProvider.Metadata: lmap(metadata.get, metaKeys)}
				yield {
					DataProvider.Dataset: hashNameDictDS[hashDS],
					DataProvider.BlockName: hashNameDictB[hashB][1],
					DataProvider.Locations: blockSEList,
					DataProvider.Metadata: list(metaKeys),
					DataProvider.FileList: lsmap(fnProps, protoBlocks[hashDS][hashB])
				}


	def getBlocksInternal(self):
		# Split files into blocks/datasets via key functions and determine metadata intersection
		(protoBlocks, commonDS, commonB) = ({}, {}, {})
		def getActiveKeys(kUser, kGuard, gIdx):
			return kUser + (kGuard or lchain(imap(lambda x: x.getGuards()[gIdx], self.scanner)))
		keysDS = getActiveKeys(self.kUserDS, self.kGuardDS, 0)
		keysB = getActiveKeys(self.kUserB, self.kGuardB, 1)
		for fileInfo in self._collectFiles():
			hashDS = self._generateKey(keysDS, None, *fileInfo)
			hashB = self._generateKey(keysB, hashDS, *fileInfo)
			if self.kSelectDS and (hashDS not in self.kSelectDS):
				continue
			fileInfo[1].update({'DS_KEY': hashDS, 'BLOCK_KEY': hashB})
			protoBlocks.setdefault(hashDS, {}).setdefault(hashB, []).append(fileInfo)
			utils.intersectDict(commonDS.setdefault(hashDS, dict(fileInfo[1])), fileInfo[1])
			utils.intersectDict(commonB.setdefault(hashDS, {}).setdefault(hashB, dict(fileInfo[1])), fileInfo[1])

		# Generate names for blocks/datasets using common metadata
		(hashNameDictDS, hashNameDictB) = ({}, {})
		for hashDS in protoBlocks:
			hashNameDictDS[hashDS] = self._generateDatasetName(hashDS, commonDS[hashDS])
			for hashB in protoBlocks[hashDS]:
				hashNameDictB[hashB] = (hashDS, self._generateBlockName(hashB, commonB[hashDS][hashB]))

		self._findCollision('dataset', hashNameDictDS, commonDS, keysDS, lambda name, key: [key])
		self._findCollision('block', hashNameDictB, commonB, keysDS + keysB, lambda name, key: [name[0], key], lambda name: name[1])

		for block in self._buildBlocks(protoBlocks, hashNameDictDS, hashNameDictB):
			yield block


# Get dataset information from storage url
# required format: <storage url>
class ScanProvider(ScanProviderBase):
	alias = ['scan']

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		ds_config = config.changeView(viewClass = 'TaggedConfigView', addNames = [md5_hex(datasetExpr)])
		if '*' in os.path.basename(datasetExpr):
			ds_config.set('source directory', os.path.dirname(datasetExpr))
			ds_config.set('filename filter', datasetExpr)
		else:
			ds_config.set('source directory', datasetExpr)
		defScanner = ['FilesFromLS', 'MatchOnFilename', 'MatchDelimeter', 'DetermineEvents', 'AddFilePrefix']
		ScanProviderBase.__init__(self, ds_config, defScanner, datasetNick, datasetID)


# Get dataset information just from grid-control instance
# required format: <path to config file / workdir> [%<job selector]
class GCProvider(ScanProviderBase):
	alias = ['gc']
	stageDir = {}
	stageFile = {None: ['MatchOnFilename', 'MatchDelimeter']}

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		ds_config = config.changeView(viewClass = 'TaggedConfigView', addNames = [md5_hex(datasetExpr)])
		if os.path.isdir(datasetExpr):
			GCProvider.stageDir[None] = ['OutputDirsFromWork']
			ds_config.set('source directory', datasetExpr)
			datasetExpr = os.path.join(datasetExpr, 'work.conf')
		else:
			GCProvider.stageDir[None] = ['OutputDirsFromConfig', 'MetadataFromTask']
			datasetExpr, selector = utils.optSplit(datasetExpr, '%')
			ds_config.set('source config', datasetExpr)
			ds_config.set('source job selector', selector)
		ext_config = createConfig(datasetExpr)
		ext_task_name = ext_config.changeView(setSections = ['global']).get(['task', 'module'])
		if 'ParaMod' in ext_task_name: # handle old config files
			ext_task_name = ext_config.changeView(setSections = ['ParaMod']).get('module')
		sGet = lambda scannerDict: scannerDict.get(None) + scannerDict.get(ext_task_name, [])
		sList = sGet(GCProvider.stageDir) + ['JobInfoFromOutputDir', 'FilesFromJobInfo'] + sGet(GCProvider.stageFile) + ['DetermineEvents', 'AddFilePrefix']
		ScanProviderBase.__init__(self, ds_config, sList, datasetNick, datasetID)
