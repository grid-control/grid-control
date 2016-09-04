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
from grid_control.config import appendOption, create_config
from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.utils.data_structures import UniqueList
from hpfwk import Plugin, PluginError
from python_compat import identity, ifilter, imap, itemgetter, lchain, lmap, lsmap, md5_hex, sorted

class ScanProviderBase(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick, sList):
		DataProvider.__init__(self, config, datasetExpr, datasetNick)
		(self._ds_select, self._ds_name, self._ds_keys_user, self._ds_keys_guard) = self._setup(config, 'dataset')
		(self._b_select, self._b_name, self._b_keys_user, self._b_keys_guard) = self._setup(config, 'block')
		scanList = config.getList('scanner', sList) + ['NullScanner']
		self._scanner = lmap(lambda cls: InfoScanner.createInstance(cls, config), scanList)


	def _setup(self, config, prefix):
		select = config.getList(appendOption(prefix, 'key select'), [])
		name = config.get(appendOption(prefix, 'name pattern'), '')
		kuser = config.getList(appendOption(prefix, 'hash keys'), [])
		kguard = config.getList(appendOption(prefix, 'guard override'), [])
		return (select, name, kuser, kguard)


	def _collectFiles(self):
		def recurse(level, collectorList, args):
			if collectorList:
				for data in recurse(level - 1, collectorList[:-1], args):
					for (path, metadata, nEvents, seList, objStore) in collectorList[-1](level, *data):
						yield (path, dict(metadata), nEvents, seList, objStore)
			else:
				yield args
		return recurse(len(self._scanner), lmap(lambda x: x.getEntriesVerbose, self._scanner), (None, {}, None, None, {}))


	def _generateKey(self, keys, base, path, metadata, events, seList, objStore):
		return md5_hex(repr(base) + repr(lmap(metadata.get, keys)))


	def _generateDatasetName(self, key, data):
		if 'SE_OUTPUT_BASE' in data:
			return utils.replaceDict(self._ds_name or '/PRIVATE/@SE_OUTPUT_BASE@', data)
		return utils.replaceDict(self._ds_name or ('/PRIVATE/Dataset_%s' % key), data)


	def _generateBlockName(self, key, data):
		return utils.replaceDict(self._b_name or key[:8], data)


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
		for name, key_list in sorted(dupesDict.items()):
			if len(key_list) > 1:
				self._log.warn('Multiple %s keys are mapped to the name %s!', tName, repr(name))
				for key in sorted(key_list):
					self._log.warn('\t%s hash %s using:', tName, str.join('#', key))
					for var, value in self._getFilteredVarDict(varDict, key, hashKeys).items():
						self._log.warn('\t\t%s = %s', var, value)
				if ask and not utils.getUserBool('Do you want to continue?', False):
					sys.exit(os.EX_OK)
				ask = False


	def _buildBlocks(self, protoBlocks, hashNameDictDS, hashNameDictB):
		# Return named dataset
		for hashDS in sorted(protoBlocks):
			for hashB in sorted(protoBlocks[hashDS]):
				blockSEList = None
				for seList in ifilter(lambda s: s is not None, imap(lambda x: x[3], protoBlocks[hashDS][hashB])):
					blockSEList = blockSEList or []
					blockSEList.extend(seList)
				if blockSEList is not None:
					blockSEList = list(UniqueList(blockSEList))
				metaKeys = protoBlocks[hashDS][hashB][0][1].keys()
				def fnProps(path, metadata, events, seList, objStore):
					if events is None:
						events = -1
					return {DataProvider.URL: path, DataProvider.NEntries: events,
						DataProvider.Metadata: lmap(metadata.get, metaKeys)}
				yield {
					DataProvider.Dataset: hashNameDictDS[hashDS],
					DataProvider.BlockName: hashNameDictB[hashB][1],
					DataProvider.Locations: blockSEList,
					DataProvider.Metadata: list(metaKeys),
					DataProvider.FileList: lsmap(fnProps, protoBlocks[hashDS][hashB])
				}


	def _getBlocksInternal(self):
		# Split files into blocks/datasets via key functions and determine metadata intersection
		(protoBlocks, commonDS, commonB) = ({}, {}, {})
		def getActiveKeys(kUser, kGuard, gIdx):
			return kUser + (kGuard or lchain(imap(lambda x: x.getGuards()[gIdx], self._scanner)))
		keysDS = getActiveKeys(self._ds_keys_user, self._ds_keys_guard, 0)
		keysB = getActiveKeys(self._b_keys_user, self._b_keys_guard, 1)
		for fileInfo in ifilter(itemgetter(0), self._collectFiles()):
			hashDS = self._generateKey(keysDS, md5_hex(repr(self._datasetExpr)) + md5_hex(repr(self._datasetNick)), *fileInfo)
			hashB = self._generateKey(keysB, hashDS + md5_hex(repr(fileInfo[3])), *fileInfo) # [3] == SE list
			if not self._ds_select or (hashDS in self._ds_select):
				if not self._b_select or (hashB in self._b_select):
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

	def __init__(self, config, datasetExpr, datasetNick = None):
		ds_config = config.changeView(viewClass = 'TaggedConfigView', addNames = [md5_hex(datasetExpr)])
		basename = os.path.basename(datasetExpr)
		firstScanner = 'FilesFromLS'
		if '*' in basename:
			ds_config.set('source directory', datasetExpr.replace(basename, ''))
			ds_config.set('filename filter', basename)
		elif not datasetExpr.endswith('.dbs'):
			ds_config.set('source directory', datasetExpr)
		else:
			ds_config.set('source dataset path', datasetExpr)
			ds_config.set('filename filter', '')
			firstScanner = 'FilesFromDataProvider'
		defScanner = [firstScanner, 'MatchOnFilename', 'MatchDelimeter', 'DetermineEvents', 'AddFilePrefix']
		ScanProviderBase.__init__(self, ds_config, datasetExpr, datasetNick, defScanner)


# This class is used to disentangle the TaskModule and GCProvider class - without any direct dependencies / imports
class GCProviderSetup(Plugin):
	alias = ['GCProviderSetup_TaskModule']
	scan_pipeline = ['JobInfoFromOutputDir', 'FilesFromJobInfo', 'MatchOnFilename', 'MatchDelimeter', 'DetermineEvents', 'AddFilePrefix']


# Get dataset information just from grid-control instance
# required format: <path to config file / workdir> [%<job selector]
class GCProvider(ScanProviderBase):
	alias = ['gc']

	def __init__(self, config, datasetExpr, datasetNick = None):
		ds_config = config.changeView(viewClass = 'TaggedConfigView', addNames = [md5_hex(datasetExpr)])
		if os.path.isdir(datasetExpr):
			scan_pipeline = ['OutputDirsFromWork']
			ds_config.set('source directory', datasetExpr)
			datasetExpr = os.path.join(datasetExpr, 'work.conf')
		else:
			scan_pipeline = ['OutputDirsFromConfig', 'MetadataFromTask']
			datasetExpr, selector = utils.optSplit(datasetExpr, '%')
			ds_config.set('source config', datasetExpr)
			ds_config.set('source job selector', selector)
		ext_config = create_config(datasetExpr)
		ext_task_name = ext_config.changeView(setSections = ['global']).get(['module', 'task'])
		if 'ParaMod' in ext_task_name: # handle old config files
			ext_task_name = ext_config.changeView(setSections = ['ParaMod']).get('module')
		ext_task_cls = Plugin.getClass(ext_task_name)
		for ext_task_cls in Plugin.getClass(ext_task_name).iterClassBases():
			try:
				scan_holder = GCProviderSetup.getClass('GCProviderSetup_' + ext_task_cls.__name__)
			except PluginError:
				continue
			scan_pipeline += scan_holder.scan_pipeline
			break
		ScanProviderBase.__init__(self, ds_config, datasetExpr, datasetNick, scan_pipeline)
