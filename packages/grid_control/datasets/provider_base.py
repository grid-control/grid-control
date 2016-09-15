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
from grid_control.config import create_config, triggerResync
from grid_control.datasets.dproc_base import DataProcessor, NullDataProcessor
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, InstanceFactory, NestedException
from python_compat import StringBuffer, identity, ifilter, imap, irange, json, lmap, lrange, md5_hex, set, sort_inplace, sorted

class DatasetError(NestedException):
	pass


class DataProvider(ConfigurablePlugin):
	def __init__(self, config, datasource_name, dataset_expr, dataset_nick = None, dataset_proc = None):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('%s.provider' % datasource_name)
		(self._datasource_name, self._dataset_expr, self._dataset_nick) = (datasource_name, dataset_expr, dataset_nick)
		(self._cache_block, self._cache_dataset) = (None, None)
		self._dataset_query_interval = config.getTime('%s default query interval' % datasource_name, 60, onChange = None)

		triggerDataResync = triggerResync(['datasets', 'parameters'])
		self._stats = dataset_proc or DataProcessor.createInstance('SimpleStatsDataProcessor', config, datasource_name,
			triggerDataResync, self._log, ' * Dataset %s:\n\tcontains ' % repr(dataset_nick or dataset_expr))
		self._nick_producer = config.getPlugin(['nickname source', '%s nickname source' % datasource_name], 'SimpleNickNameProducer',
			cls = DataProcessor, pargs = (datasource_name, triggerDataResync), onChange = triggerDataResync)
		self._dataset_processor = dataset_proc or config.getCompositePlugin('%s processor' % datasource_name,
			'NickNameConsistencyProcessor EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' +
			'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor', 'MultiDataProcessor',
			cls = DataProcessor, pargs = (datasource_name, triggerDataResync), onChange = triggerDataResync)


	def bind(cls, value, **kwargs):
		config = kwargs.pop('config')
		datasource_name = kwargs.pop('datasource_name', 'dataset')
		defaultProvider = config.get('%s provider' % datasource_name, 'ListProvider')

		instance_args = []
		for entry in ifilter(str.strip, value.splitlines()):
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
			instance_args.append([bindValue, clsNew, config, datasource_name, dataset, nickname])
		for instance_arg in instance_args:
			if len(instance_args) > 1:
				instance_arg.append(NullDataProcessor()) # setting dataset_proc
			yield InstanceFactory(*instance_arg)
	bind = classmethod(bind)


	def getBlocksFromExpr(cls, config, dataset_expr):
		for dp_factory in DataProvider.bind(dataset_expr, config = config):
			dproc = dp_factory.getBoundInstance()
			for block in dproc.get_blocks_raw():
				yield block
	getBlocksFromExpr = classmethod(getBlocksFromExpr)


	def bName(cls, block):
		if block.get(DataProvider.BlockName, '') in ['', '0']:
			return block[DataProvider.Dataset]
		return block[DataProvider.Dataset] + '#' + block[DataProvider.BlockName]
	bName = classmethod(bName)


	def _raise_on_abort(self):
		if utils.abort():
			raise DatasetError('Received abort request during retrieval of %r' % self.get_dataset_expr())


	def get_dataset_expr(self):
		return self._dataset_expr


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return self._dataset_query_interval


	# Check if splitter is valid
	def checkSplitter(self, splitter):
		return splitter


	# Default implementation via getBlocks
	def getDatasets(self):
		if self._cache_dataset is None:
			self._cache_dataset = set()
			for block in self.getBlocks(show_stats = True):
				self._cache_dataset.add(block[DataProvider.Dataset])
				if utils.abort():
					raise DatasetError('Received abort request during dataset name retrieval!')
		return list(self._cache_dataset)


	# Cached access to list of block dicts, does also the validation checks
	def getBlocks(self, show_stats):
		statsProcessor = NullDataProcessor()
		if show_stats:
			statsProcessor = self._stats
		if self._cache_block is None:
			try:
				self._cache_block = list(statsProcessor.process(self._dataset_processor.process(self.get_blocks_raw())))
			except Exception:
				raise DatasetError('Unable to run dataset %s through processing pipeline!' % repr(self._dataset_expr))
		return self._cache_block


	# List of block dicts with format
	# { NEntries: 123, Dataset: '/path/to/data', Block: 'abcd-1234', Locations: ['site1','site2'],
	#   Filelist: [{URL: '/path/to/file1', NEntries: 100}, {URL: '/path/to/file2', NEntries: 23}]}
	def _getBlocksInternal(self):
		raise AbstractError


	def get_blocks_raw(self):
		activity = Activity('Retrieving %s' % self._dataset_expr)
		try:
			# Validation, Naming:
			for block in self._getBlocksInternal():
				assert(block[DataProvider.Dataset])
				block.setdefault(DataProvider.BlockName, '0')
				block.setdefault(DataProvider.Provider, self.__class__.__name__)
				block.setdefault(DataProvider.Locations, None)
				events = sum(imap(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
				block.setdefault(DataProvider.NEntries, events)
				if self._dataset_nick:
					block[DataProvider.Nickname] = self._dataset_nick
				elif self._nick_producer:
					block = self._nick_producer.process_block(block)
					if not block:
						raise DatasetError('Nickname producer failed!')
				yield block
		except Exception:
			raise DatasetError('Unable to retrieve dataset %s' % repr(self._dataset_expr))
		activity.finish()


	def clearCache(self):
		self._cache_block = None
		self._cache_dataset = None


	def _classify_metadata_name_list(block):
		def metadataHash(fi, idx):
			if idx < len(fi[DataProvider.Metadata]):
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
	_classify_metadata_name_list = staticmethod(_classify_metadata_name_list)


	def get_hash(self):
		buffer = StringBuffer()
		for _ in DataProvider.saveToStream(buffer, self._dataset_processor.process(self.get_blocks_raw())):
			pass
		return md5_hex(buffer.getvalue())


	# Save dataset information in 'ini'-style => 10x faster to r/w than cPickle
	def saveToStream(stream, dataBlocks, stripMetadata = False):
		writer = StringBuffer()
		write_separator = False
		for block in dataBlocks:
			if write_separator:
				writer.write('\n')
			writer.write('[%s]\n' % DataProvider.bName(block))
			if DataProvider.Nickname in block:
				writer.write('nickname = %s\n' % block[DataProvider.Nickname])
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
				(idxListBlock, idxListFile) = DataProvider._classify_metadata_name_list(block)
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
			stream.write(writer.getvalue())
			writer.seek(0)
			writer.truncate(0)
			write_separator = True
			yield block
	saveToStream = staticmethod(saveToStream)


	def saveToFile(path, dataBlocks, stripMetadata = False):
		if os.path.dirname(path):
			utils.ensureDirExists(os.path.dirname(path), 'dataset cache directory')
		fp = open(path, 'w')
		try:
			for _ in DataProvider.saveToStream(fp, dataBlocks, stripMetadata):
				pass
		finally:
			fp.close()
	saveToFile = staticmethod(saveToFile)


	# Load dataset information using ListProvider
	def loadFromFile(path):
		return DataProvider.createInstance('ListProvider', create_config(
			configDict = {'dataset': {'dataset processor': 'NullDataProcessor'}}), 'dataset', path)
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
	'Nickname', 'Metadata', 'Provider', 'ResyncInfo'], DataProvider)
