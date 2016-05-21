# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import os, copy, logging
from grid_control import utils
from grid_control.config import createConfig, triggerResync
from grid_control.datasets.dproc_base import DataProcessor
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, InstanceFactory, NestedException
from python_compat import StringBuffer, identity, ifilter, imap, irange, json, lmap, lrange, md5_hex, sort_inplace, sorted

class DatasetError(NestedException):
	pass

class DataProvider(ConfigurablePlugin):
	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('user.dataprovider')
		(self._datasetExpr, self._datasetNick, self._datasetID) = (datasetExpr, datasetNick, datasetID)
		(self._cache_block, self._cache_dataset, self._passthrough) = (None, None, False)

		self._stats = DataProcessor.createInstance('StatsDataProcessor', config)
		self._nickProducer = config.getPlugin('nickname source', 'SimpleNickNameProducer', cls = DataProcessor)
		self._datasetProcessor = config.getCompositePlugin('dataset processor',
			'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' +
			'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor',
			'MultiDataProcessor', cls = DataProcessor, onChange = triggerResync(['datasets', 'parameters']))


	def bind(cls, value, **kwargs):
		config = kwargs.pop('config')
		defaultProvider = config.get('dataset provider', 'ListProvider')

		for idx, entry in enumerate(ifilter(str.strip, value.splitlines())):
			(nickname, provider, dataset) = ('', defaultProvider, None)
			temp = lmap(str.strip, entry.split(':', 2))
			if len(temp) == 3:
				(nickname, provider, dataset) = temp
				if dataset.startswith('/'):
					dataset = '/' + dataset.lstrip('/')
			elif len(temp) == 2:
				(nickname, dataset) = temp
			elif len(temp) == 1:
				dataset = temp[0]

			clsNew = cls.getClass(provider)
			bindValue = str.join(':', [nickname, provider, dataset])
			yield InstanceFactory(bindValue, clsNew, config, dataset, nickname, idx)
	bind = classmethod(bind)


	def getDatasetExpr(self):
		return self._datasetExpr


	def setPassthrough(self):
		self._passthrough = True


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 60 # 1 minute delay minimum


	# Check if splitter is valid
	def checkSplitter(self, splitter):
		return splitter


	# Default implementation via getBlocks
	def getDatasets(self):
		if self._cache_dataset is None:
			self._cache_dataset = []
			for block in self.getBlocks():
				if block[DataProvider.Dataset] not in self._cache_dataset:
					self._cache_dataset.append(block[DataProvider.Dataset])
		return self._cache_dataset


	# Cached access to list of block dicts, does also the validation checks
	def getBlocks(self, silent = True):
		def prepareBlocks():
			# Validation, Filtering & Naming:
			for block in self.getBlocksInternal():
				assert(block[DataProvider.Dataset])
				block.setdefault(DataProvider.BlockName, '0')
				block.setdefault(DataProvider.Provider, self.__class__.__name__)
				block.setdefault(DataProvider.Locations, None)
				if self._datasetID:
					block[DataProvider.DatasetID] = self._datasetID
				events = sum(imap(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
				block.setdefault(DataProvider.NEntries, events)
				if self._datasetNick:
					block[DataProvider.Nickname] = self._datasetNick
				elif self._nickProducer:
					block = self._nickProducer.processBlock(block)
					if not block:
						raise DatasetError('Nickname producer failed!')
				yield block

		if self._cache_block is None:
			log = utils.ActivityLog('Retrieving %s' % self._datasetExpr)
			try:
				if self._passthrough:
					self._cache_block = list(self._stats.process(prepareBlocks()))
				else:
					self._cache_block = list(self._stats.process(self._datasetProcessor.process(prepareBlocks())))
			except Exception:
				raise DatasetError('Unable to retrieve dataset %s' % repr(self._datasetExpr))
			statString = ' * Dataset '
			if self._datasetNick:
				statString += repr(self._datasetNick)
			elif self._datasetExpr:
				statString += repr(self._datasetExpr)
			log.finish()
			statString += '\tcontains %d block(s) with %s' % self._stats.getStats()
			if not silent:
				self._log.info(statString)
		return self._cache_block


	# List of block dicts with format
	# { NEntries: 123, Dataset: '/path/to/data', Block: 'abcd-1234', Locations: ['site1','site2'],
	#   Filelist: [{URL: '/path/to/file1', NEntries: 100}, {URL: '/path/to/file2', NEntries: 23}]}
	def getBlocksInternal(self):
		raise AbstractError


	def clearCache(self):
		self._cache_block = None
		self._cache_dataset = None


	def classifyMetadataKeys(block):
		def metadataHash(fi, idx):
			return md5_hex(repr(fi[DataProvider.Metadata][idx]))
		cMetadataIdx = lrange(len(block[DataProvider.Metadata]))
		cMetadataHash = lmap(lambda idx: metadataHash(block[DataProvider.FileList][0], idx), cMetadataIdx)
		for fi in block[DataProvider.FileList]: # Identify common metadata
			for idx in cMetadataIdx:
				if metadataHash(fi, idx) != cMetadataHash[idx]:
					cMetadataIdx.remove(idx)
		def filterC(common):
			idxList = ifilter(lambda idx: (idx in cMetadataIdx) == common, irange(len(block[DataProvider.Metadata])))
			return sorted(idxList, key = lambda idx: block[DataProvider.Metadata][idx])
		return (filterC(True), filterC(False))
	classifyMetadataKeys = staticmethod(classifyMetadataKeys)


	# Save dataset information in 'ini'-style => 10x faster to r/w than cPickle
	def saveToStream(stream, dataBlocks, stripMetadata = False):
		writer = StringBuffer()
		for block in dataBlocks:
			writer.write('[%s#%s]\n' % (block[DataProvider.Dataset], block[DataProvider.BlockName]))
			if DataProvider.Nickname in block:
				writer.write('nickname = %s\n' % block[DataProvider.Nickname])
			if DataProvider.DatasetID in block:
				writer.write('id = %d\n' % block[DataProvider.DatasetID])
			if DataProvider.NEntries in block:
				writer.write('events = %d\n' % block[DataProvider.NEntries])
			if block.get(DataProvider.Locations) is not None:
				writer.write('se list = %s\n' % str.join(',', block[DataProvider.Locations]))
			cPrefix = os.path.commonprefix(lmap(lambda x: x[DataProvider.URL], block[DataProvider.FileList]))
			cPrefix = str.join('/', cPrefix.split('/')[:-1])
			if len(cPrefix) > 6:
				writer.write('prefix = %s\n' % cPrefix)
				formatter = lambda x: x.replace(cPrefix + '/', '')
			else:
				formatter = identity

			writeMetadata = (DataProvider.Metadata in block) and not stripMetadata
			if writeMetadata:
				(idxListBlock, idxListFile) = DataProvider.classifyMetadataKeys(block)
				def getMetadata(fi, idxList):
					idxList = ifilter(lambda idx: idx < len(fi[DataProvider.Metadata]), idxList)
					return json.dumps(lmap(lambda idx: fi[DataProvider.Metadata][idx], idxList))
				writer.write('metadata = %s\n' % json.dumps(lmap(lambda idx: block[DataProvider.Metadata][idx], idxListBlock + idxListFile)))
				if idxListBlock:
					writer.write('metadata common = %s\n' % getMetadata(block[DataProvider.FileList][0], idxListBlock))
			for fi in block[DataProvider.FileList]:
				writer.write('%s = %d' % (formatter(fi[DataProvider.URL]), fi[DataProvider.NEntries]))
				if writeMetadata and idxListFile:
					writer.write(' %s' % getMetadata(fi, idxListFile))
				writer.write('\n')
			writer.write('\n')
		stream.write(writer.getvalue())
	saveToStream = staticmethod(saveToStream)


	def saveToFile(path, dataBlocks, stripMetadata = False):
		fp = open(path, 'w')
		try:
			DataProvider.saveToStream(fp, dataBlocks, stripMetadata)
		finally:
			fp.close()
	saveToFile = staticmethod(saveToFile)


	# Load dataset information using ListProvider
	def loadFromFile(path):
		return DataProvider.createInstance('ListProvider', createConfig(useDefaultFiles = False,
			configDict = {'dataset': {'dataset processor': 'NullDataProcessor'}}), path)
	loadFromFile = staticmethod(loadFromFile)


	# Returns changes between two sets of blocks in terms of added, missing and changed blocks
	# Only the affected files are returned in the block file list
	def resyncSources(oldBlocks, newBlocks):
		# Compare different blocks according to their name - NOT full content
		def keyBlock(x):
			return (x[DataProvider.Dataset], x[DataProvider.BlockName])
		sort_inplace(oldBlocks, key = keyBlock)
		sort_inplace(newBlocks, key = keyBlock)

		def onMatchingBlock(blocksAdded, blocksMissing, blocksMatching, oldBlock, newBlock):
			# Compare different files according to their name - NOT full content
			def keyFiles(x):
				return x[DataProvider.URL]
			sort_inplace(oldBlock[DataProvider.FileList], key = keyFiles)
			sort_inplace(newBlock[DataProvider.FileList], key = keyFiles)

			def onMatchingFile(filesAdded, filesMissing, filesMatched, oldFile, newFile):
				filesMatched.append((oldFile, newFile))

			(filesAdded, filesMissing, filesMatched) = \
				utils.DiffLists(oldBlock[DataProvider.FileList], newBlock[DataProvider.FileList], keyFiles, onMatchingFile, isSorted = True)
			if filesAdded: # Create new block for added files in an existing block
				tmpBlock = copy.copy(newBlock)
				tmpBlock[DataProvider.FileList] = filesAdded
				tmpBlock[DataProvider.NEntries] = sum(imap(lambda x: x[DataProvider.NEntries], filesAdded))
				blocksAdded.append(tmpBlock)
			blocksMatching.append((oldBlock, newBlock, filesMissing, filesMatched))

		return utils.DiffLists(oldBlocks, newBlocks, keyBlock, onMatchingBlock, isSorted = True)
	resyncSources = staticmethod(resyncSources)

# To uncover errors, the enums of DataProvider / DataSplitter do *NOT* match type wise
makeEnum(['NEntries', 'BlockName', 'Dataset', 'Locations', 'URL', 'FileList',
	'Nickname', 'DatasetID', 'Metadata', 'Provider', 'ResyncInfo'], DataProvider)
