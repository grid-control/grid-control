import os, gzip, cStringIO, copy, random
from grid_control import utils, AbstractObject, AbstractError, ConfigError

class NickNameProducer(AbstractObject):
	def __init__(self, config):
		pass

	def getName(self, oldnick, dataset):
		return oldnick
NickNameProducer.dynamicLoaderPath()


class SimpleNickNameProducer(NickNameProducer):
	def getName(self, oldnick, dataset):
		if oldnick == '':
			try:
				return dataset.split('/')[1]
			except:
				pass
		return oldnick


class DataProvider(AbstractObject):
	# To uncover errors, the enums of DataProvider / DataSplitter do *NOT* match
	dataInfos = ['NEvents', 'BlockName', 'Dataset', 'SEList', 'lfn', 'FileList', 'Nickname', 'DatasetID', 'Metadata']
	for id, dataInfo in enumerate(dataInfos):
		locals()[dataInfo] = id

	def __init__(self, config, section, datasetExpr, datasetNick, datasetID):
		(self._datasetExpr, self._datasetNick, self._datasetID) = (datasetExpr, datasetNick, datasetID)
		self._cache = None
		self.sitefilter = map(str.strip, self.setup(config.get, 'dataset', 'sites', '').split())
		self.emptyBlock = self.setup(config.getBool, 'dataset', 'remove empty blocks', True)
		self.emptyFiles = self.setup(config.getBool, 'dataset', 'remove empty files', True)
		self.limitEvents = self.setup(config.getInt, 'dataset', 'limit events', -1)


	def setup(self, func, section, item, default = None):
		value = func(section, item, default)
		if self._datasetNick:
			value = func('dataset %s' % self._datasetNick, item, value)
		return value


	# Parse dataset format [NICK : [PROVIDER : [(/)*]]] DATASET
	def parseDatasetExpr(config, expression, defaultProvider):
		(nickname, provider, dataset) = ('', defaultProvider, None)
		temp = map(str.strip, expression.split(':', 2))
		providerMap = dict(map(lambda (x, y): (y, x), DataProvider.providers.items()))

		if len(temp) == 3:
			(nickname, provider, dataset) = temp
			if dataset[0] == '/':
				dataset = '/' + dataset.lstrip('/')
			provider = providerMap.get(provider.lower(), provider)
		elif len(temp) == 2:
			(nickname, dataset) = temp
		elif len(temp) == 1:
			dataset = temp[0]

		nickProducer = config.get('dataset', 'nickname source', 'SimpleNickNameProducer')
		nickname = NickNameProducer.open(nickProducer, config).getName(nickname, dataset)
		return (nickname, provider, dataset)
	parseDatasetExpr = staticmethod(parseDatasetExpr)


	# Create a new DataProvider instance
	def create(config, section, dataset, defaultProvider):
		if '\n' in dataset:
			return DataProvider.open('DataMultiplexer', config, section, dataset, defaultProvider)
		else:
			(dsNick, dsProv, dsExpr) = DataProvider.parseDatasetExpr(config, dataset, defaultProvider)
			return DataProvider.open(dsProv, config, section, dsExpr, dsNick, 0)
	create = staticmethod(create)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 60 # 1 minute delay minimum


	# Check if splitter is valid
	def checkSplitter(self, splitter):
		return splitter


	# Cached access to list of block dicts, does also the validation checks
	def getBlocks(self):
		if self._cache == None:
			if self._datasetExpr != None:
				log = utils.ActivityLog('Retrieving %s' % self._datasetExpr)
			self._cache = list(self.getBlocksInternal())

			allEvents = 0
			# Validation, Filtering & Naming:
			for block in self._cache:
				if self._datasetID:
					block[DataProvider.DatasetID] = self._datasetID
				if self._datasetNick:
					block[DataProvider.Nickname] = self._datasetNick

				events = 0
				for file in block[DataProvider.FileList]:
					events += file[DataProvider.NEvents]
				if (self.limitEvents > 0) and (allEvents + events > self.limitEvents):
					block[DataProvider.NEvents] = 0
					block[DataProvider.FileList] = []
					events = 0
				allEvents += events
				if DataProvider.NEvents not in block:
					block[DataProvider.NEvents] = events
				if events != block[DataProvider.NEvents]:
					utils.eprint('WARNING: Inconsistency in block %s#%s: Number of events doesn\'t match (b:%d != f:%d)'
						% (block[DataProvider.Dataset], block[DataProvider.BlockName], block[DataProvider.NEvents], events))

				# Filter empty files
				if self.emptyFiles:
					block[DataProvider.FileList] = filter(lambda x: x[DataProvider.NEvents] != 0, block[DataProvider.FileList])

				# Filter dataset sites
				if block[DataProvider.SEList] != None:
					sites = utils.doBlackWhiteList(block[DataProvider.SEList], self.sitefilter)
					if len(sites) == 0 and len(block[DataProvider.FileList]) != 0:
						utils.eprint('WARNING: Block %s#%s is not available at any site!'
							% (block[DataProvider.Dataset], block[DataProvider.BlockName]))
					block[DataProvider.SEList] = sites

			# Filter empty blocks
			if self.emptyBlock:
				self._cache = filter(lambda x: x[DataProvider.NEvents] != 0, self._cache)

			if self._datasetNick:
				utils.vprint('%s:' % self._datasetNick, newline = False)
			elif self.__class__.__name__ == 'DataMultiplexer':
				utils.vprint('Summary:', newline = False)
			utils.vprint('Running over %d events split into %d blocks.' % (allEvents, len(self._cache)))
		return self._cache


	# List of block dicts with format
	# { NEvents: 123, Dataset: '/path/to/data', Block: 'abcd-1234', SEList: ['site1','site2'],
	#   Filelist: [{lfn: '/path/to/file1', NEvents: 100}, {lfn: '/path/to/file2', NEvents: 23}]}
	def getBlocksInternal(self):
		raise AbstractError


	# Print information about datasets
	def printDataset(self, level = 2):
		utils.vprint('Provided datasets:', level)
		idDict = {DataProvider.DatasetID: 0, DataProvider.Dataset: None, DataProvider.Nickname: ''}
		for block in self.getBlocks():
			utils.vprint('ID / Dataset / Nick : %s / %s / %s' % map(lambda k,d: block.get(k, d), idDict.items()), level)
			utils.vprint('BlockName : %s' % block[DataProvider.BlockName], level)
			utils.vprint('#Events   : %s' % block[DataProvider.NEvents], level)
			utils.vprint('SE List   : %s' % block[DataProvider.SEList], level)
			utils.vprint('Files     : ', level)
			for fi in block[DataProvider.FileList]:
				utils.vprint('%s (Events: %d)' % (fi[DataProvider.lfn], fi[DataProvider.NEvents]), level)
			utils.vprint(level = level)


	# Save dataset information in 'ini'-style => 10x faster to r/w than cPickle
	def saveState(self, path, filename = 'datacache.dat', dataBlocks = None):
		writer = cStringIO.StringIO()
		if dataBlocks == None:
			dataBlocks = self.getBlocks()
		for block in dataBlocks:
			writer.write('[%s#%s]\n' % (block[DataProvider.Dataset], block[DataProvider.BlockName]))
			if DataProvider.Nickname in block:
				writer.write('nickname = %s\n' % block[DataProvider.Nickname])
			if DataProvider.DatasetID in block:
				writer.write('id = %d\n' % block[DataProvider.DatasetID])
			writer.write('events = %d\n' % block[DataProvider.NEvents])
			if block[DataProvider.SEList] != None:
				writer.write('se list = %s\n' % str.join(',', block[DataProvider.SEList]))
			writeMetadata = DataProvider.Metadata in block
			if writeMetadata:
				writer.write('metadata = %s\n' % block[DataProvider.Metadata])

			commonprefix = os.path.commonprefix(map(lambda x: x[DataProvider.lfn], block[DataProvider.FileList]))
			commonprefix = str.join('/', commonprefix.split('/')[:-1])
			if len(commonprefix) > 6:
				writer.write('prefix = %s\n' % commonprefix)
				formatter = lambda x: x.replace(commonprefix + '/', '')
			else:
				formatter = lambda x: x

			for fi in block[DataProvider.FileList]:
				data = [str(fi[DataProvider.NEvents])]
				if writeMetadata:
					data.append(repr(fi[DataProvider.Metadata]))
				writer.write('%s = %s\n' % (formatter(fi[DataProvider.lfn]), str.join(' ', data)))
			writer.write('\n')
		open(os.path.join(path, filename), 'wb').write(writer.getvalue())


	# Load dataset information using ListProvider
	def loadState(config, path, filename = 'datacache.dat'):
		# None, None = Don't override NickName and ID
		return DataProvider.open('ListProvider', config, '', os.path.join(path, filename), None, None)
	loadState = staticmethod(loadState)


	# Returns changes between two sets of blocks in terms of added, missing and changed blocks
	# Only the affected files are returned in the block file list
	def resyncSources(oldBlocks, newBlocks):
		# Compare different blocks according to their name - NOT full content
		def cmpBlock(x, y):
			if x[DataProvider.Dataset] == y[DataProvider.Dataset]:
				return cmp(x[DataProvider.BlockName], y[DataProvider.BlockName])
			return cmp(x[DataProvider.Dataset], y[DataProvider.Dataset])

		# Compare different blocks according to their content
		# Returns changes in terms of added, missing and changed files
		def changedBlock(blocksAdded, blocksMissing, blocksChanged, oldBlock, newBlock):

			# Compare different files according to their name - NOT full content
			def cmpFiles(x, y):
				return cmp(x[DataProvider.lfn], y[DataProvider.lfn])

			# Compare different blocks according to their content
			# Here just: #events, TODO: Checksums
			def changedFiles(filesAdded, filesMissing, filesChanged, oldFile, newFile):
				if oldFile[DataProvider.NEvents] != newFile[DataProvider.NEvents]:
					filesChanged.append(newFile)

			tmp = utils.DiffLists(oldBlock[DataProvider.FileList], newBlock[DataProvider.FileList], cmpFiles, changedFiles)
			(filesAdded, filesMissing, filesChanged) = tmp

			def copyWithoutFiles(oldblock, files):
				newblock = copy.copy(oldblock)
				newblock[DataProvider.FileList] = files
				newblock[DataProvider.NEvents] = sum(map(lambda x: x[DataProvider.NEvents], files))
				return newblock

			if filesAdded:
				blocksAdded.append(copyWithoutFiles(newBlock, filesAdded))
			if filesMissing:
				blocksMissing.append(copyWithoutFiles(newBlock, filesMissing))
			if filesChanged:
				blocksChanged.append(copyWithoutFiles(newBlock, filesChanged))

		return utils.DiffLists(oldBlocks, newBlocks, cmpBlock, changedBlock)
	resyncSources = staticmethod(resyncSources)

DataProvider.providers = {}
DataProvider.dynamicLoaderPath()
