#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, copy, logging
from grid_control import utils
from grid_control.config import createConfigFactory
from grid_control.datasets.dproc_base import DataProcessor
from hpfwk import AbstractError, InstanceFactory, NestedException, Plugin
from python_compat import StringBuffer

class DatasetError(NestedException):
	pass

class DataProvider(Plugin):
	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		(self._datasetExpr, self._datasetNick, self._datasetID) = (datasetExpr, datasetNick, datasetID)
		(self._cache, self._passthrough) = (None, False)

		self._stats = DataProcessor.getInstance('StatsDataProcessor', config)
		nickProducerClass = config.getPlugin('nickname source', 'SimpleNickNameProducer', cls = DataProcessor)
		self._nickProducer = nickProducerClass.getInstance()
		self._datasetProcessor = config.getCompositePlugin('dataset processor',
			'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' +
			'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor',
			'MultiDataProcessor', cls = DataProcessor).getInstance()


	def bind(cls, value, modulePaths = [], config = None, **kwargs):
		defaultProvider = config.get('dataset provider', 'ListProvider')

		for idx, entry in enumerate(filter(str.strip, value.splitlines())):
			(nickname, provider, dataset) = ('', defaultProvider, None)
			temp = map(str.strip, entry.split(':', 2))
			if len(temp) == 3:
				(nickname, provider, dataset) = temp
				if dataset.startswith('/'):
					dataset = '/' + dataset.lstrip('/')
			elif len(temp) == 2:
				(nickname, dataset) = temp
			elif len(temp) == 1:
				dataset = temp[0]

			clsNew = cls.getClass(provider, modulePaths)
			bindValue = str.join(':', [nickname, provider, dataset])
			yield InstanceFactory(bindValue, clsNew, config, dataset, nickname, idx)
	bind = classmethod(bind)


	def setPassthrough(self):
		self._passthrough = True
		self._nickProducer = None


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 60 # 1 minute delay minimum


	# Check if splitter is valid
	def checkSplitter(self, splitter):
		return splitter


	# Cached access to list of block dicts, does also the validation checks
	def getBlocks(self):
		def prepareBlocks():
			# Validation, Filtering & Naming:
			for block in self.getBlocksInternal():
				block.setdefault(DataProvider.BlockName, '0')
				block.setdefault(DataProvider.Provider, self.__class__.__name__)
				block.setdefault(DataProvider.Locations, None)
				if self._datasetID:
					block[DataProvider.DatasetID] = self._datasetID
				events = sum(map(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
				block.setdefault(DataProvider.NEntries, events)
				if self._datasetNick:
					block[DataProvider.Nickname] = self._datasetNick
				elif self._nickProducer:
					block = self._nickProducer.processBlock(block)
					if not block:
						raise DatasetError('Nickname producer failed!')
				yield block

		if self._cache == None:
			log = utils.ActivityLog('Retrieving %s' % self._datasetExpr)
			if self._passthrough:
				self._cache = list(self._stats.process(prepareBlocks()))
			else:
				self._cache = list(self._stats.process(self._datasetProcessor.process(prepareBlocks())))
			if self._datasetNick:
				statString = '%s: ' % self._datasetNick
			else:
				statString = '%s: ' % self._datasetExpr
			del log
			statString += 'Running over %s distributed over %d blocks.' % self._stats.getStats()
			logging.getLogger('user').info(statString)
		return self._cache


	# List of block dicts with format
	# { NEntries: 123, Dataset: '/path/to/data', Block: 'abcd-1234', Locations: ['site1','site2'],
	#   Filelist: [{URL: '/path/to/file1', NEntries: 100}, {URL: '/path/to/file2', NEntries: 23}]}
	def getBlocksInternal(self):
		raise AbstractError


	def clearCache(self):
		self._cache = None


	# Save dataset information in 'ini'-style => 10x faster to r/w than cPickle
	def saveStateRaw(stream, dataBlocks, stripMetadata = False):
		writer = StringBuffer()
		for block in dataBlocks:
			writer.write('[%s#%s]\n' % (block[DataProvider.Dataset], block[DataProvider.BlockName]))
			if DataProvider.Nickname in block:
				writer.write('nickname = %s\n' % block[DataProvider.Nickname])
			if DataProvider.DatasetID in block:
				writer.write('id = %d\n' % block[DataProvider.DatasetID])
			if DataProvider.NEntries in block:
				writer.write('events = %d\n' % block[DataProvider.NEntries])
			if block.get(DataProvider.Locations) != None:
				writer.write('se list = %s\n' % str.join(',', block[DataProvider.Locations]))
			cPrefix = os.path.commonprefix(map(lambda x: x[DataProvider.URL], block[DataProvider.FileList]))
			cPrefix = str.join('/', cPrefix.split('/')[:-1])
			if len(cPrefix) > 6:
				writer.write('prefix = %s\n' % cPrefix)
				formatter = lambda x: x.replace(cPrefix + '/', '')
			else:
				formatter = lambda x: x

			writeMetadata = (DataProvider.Metadata in block) and not stripMetadata
			if writeMetadata:
				getMetadata = lambda fi, idxList: map(lambda idx: fi[DataProvider.Metadata][idx], idxList)
				metadataHash = lambda fi, idx: utils.md5(repr(fi[DataProvider.Metadata][idx])).digest()
				cMetadataIdx = range(len(block[DataProvider.Metadata]))
				cMetadataHash = map(lambda idx: metadataHash(block[DataProvider.FileList][0], idx), cMetadataIdx)
				for fi in block[DataProvider.FileList]: # Identify common metadata
					for idx in filter(lambda idx: metadataHash(fi, idx) != cMetadataHash[idx], cMetadataIdx):
						cMetadataIdx.remove(idx)
				def filterC(common):
					idxList = filter(lambda idx: (idx in cMetadataIdx) == common, range(len(block[DataProvider.Metadata])))
					return utils.sorted(idxList, key = lambda idx: block[DataProvider.Metadata][idx])
				writer.write('metadata = %s\n' % map(lambda idx: block[DataProvider.Metadata][idx], filterC(True) + filterC(False)))
				if cMetadataIdx:
					writer.write('metadata common = %s\n' % getMetadata(block[DataProvider.FileList][0], filterC(True)))
					writeMetadata = len(cMetadataIdx) != len(block[DataProvider.Metadata])
			for fi in block[DataProvider.FileList]:
				writer.write('%s = %d' % (formatter(fi[DataProvider.URL]), fi[DataProvider.NEntries]))
				if writeMetadata:
					writer.write(' %s' % getMetadata(fi, filterC(False)))
				writer.write('\n')
			writer.write('\n')
		stream.write(writer.getvalue())
	saveStateRaw = staticmethod(saveStateRaw)


	def saveState(self, path, dataBlocks = None, stripMetadata = False):
		if dataBlocks == None:
			dataBlocks = self.getBlocks()
		DataProvider.saveStateRaw(open(path, 'wb'), dataBlocks, stripMetadata)


	# Load dataset information using ListProvider
	def loadState(path):
		config = createConfigFactory(useDefaultFiles = False, configDict = {'dataset': {
			'nickname check consistency': 'False', 'nickname check collision': 'False'}}).getConfig()
		return DataProvider.getInstance('ListProvider', config, path)
	loadState = staticmethod(loadState)


	# Returns changes between two sets of blocks in terms of added, missing and changed blocks
	# Only the affected files are returned in the block file list
	def resyncSources(oldBlocks, newBlocks):
		# Compare different blocks according to their name - NOT full content
		def cmpBlock(x, y):
			if x[DataProvider.Dataset] == y[DataProvider.Dataset]:
				return cmp(x[DataProvider.BlockName], y[DataProvider.BlockName])
			return cmp(x[DataProvider.Dataset], y[DataProvider.Dataset])
		oldBlocks.sort(cmpBlock)
		newBlocks.sort(cmpBlock)

		def onMatchingBlock(blocksAdded, blocksMissing, blocksMatching, oldBlock, newBlock):
			# Compare different files according to their name - NOT full content
			def cmpFiles(x, y):
				return cmp(x[DataProvider.URL], y[DataProvider.URL])
			oldBlock[DataProvider.FileList].sort(cmpFiles)
			newBlock[DataProvider.FileList].sort(cmpFiles)

			def onMatchingFile(filesAdded, filesMissing, filesMatched, oldFile, newFile):
				filesMatched.append((oldFile, newFile))

			(filesAdded, filesMissing, filesMatched) = \
				utils.DiffLists(oldBlock[DataProvider.FileList], newBlock[DataProvider.FileList], cmpFiles, onMatchingFile, isSorted = True)
			if filesAdded: # Create new block for added files in an existing block
				tmpBlock = copy.copy(newBlock)
				tmpBlock[DataProvider.FileList] = filesAdded
				tmpBlock[DataProvider.NEntries] = sum(map(lambda x: x[DataProvider.NEntries], filesAdded))
				blocksAdded.append(tmpBlock)
			blocksMatching.append((oldBlock, newBlock, filesMissing, filesMatched))

		return utils.DiffLists(oldBlocks, newBlocks, cmpBlock, onMatchingBlock, isSorted = True)
	resyncSources = staticmethod(resyncSources)

# To uncover errors, the enums of DataProvider / DataSplitter do *NOT* match type wise
utils.makeEnum(['NEntries', 'BlockName', 'Dataset', 'Locations', 'URL', 'FileList',
	'Nickname', 'DatasetID', 'Metadata', 'Provider', 'ResyncInfo'], DataProvider)
