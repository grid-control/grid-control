import os, gzip, cStringIO, copy, random
from grid_control import QM, utils, LoadableObject, AbstractError, ConfigError, noDefault, Config, DatasetError

class NickNameProducer(LoadableObject):
	def __init__(self, config):
		self.config = config
		# Ensure the same nickname is used consistently in all blocks of a dataset
		self._checkConsistency = config.getBool('nickname check consistency', True)
		self._checkConsistencyData = {}
		# Check if two different datasets have the same nickname
		self._checkCollision = config.getBool('nickname check collision', True)
		self._checkCollisionData = {}

	# Get nickname and check for collisions
	def process(self, block):
		blockDS = block[DataProvider.Dataset]
		oldNick = block.get(DataProvider.Nickname, '')
		newNick = self.getName(oldNick, blockDS, block)
		if not (self._checkConsistency or self._checkCollision):
			return newNick # Skip checking for collisions if disabled
		# Check if nickname is used consistenly in all blocks of a datasets
		if self._checkConsistency:
			if self._checkConsistencyData.setdefault(blockDS, newNick) != newNick:
				raise DatasetError('Different blocks of dataset "%s" have different nicknames: "%s" != "%s"' % (
					blockDS, self._checkConsistencyData[blockDS], newNick))
		if self._checkCollision:
			if self._checkCollisionData.setdefault(newNick, blockDS) != blockDS:
				raise DatasetError('Multiple datasets use the same nickname "%s": "%s" != "%s"' % (
					newNick, self._checkCollisionData[newNick], blockDS))
		return newNick

	# Overwritten by users / other implementations
	def getName(self, oldnick, dataset, block):
		raise AbstractError
NickNameProducer.registerObject()


class SimpleNickNameProducer(NickNameProducer):
	def getName(self, oldnick, dataset, block):
		if oldnick == '':
			return dataset.replace('/PRIVATE/', '').lstrip('/').split('/')[0].split('#')[0]
		return oldnick


class InlineNickNameProducer(NickNameProducer):
	def getName(self, oldnick, dataset, block):
		return eval(self.config.get('nickname expr', 'oldnick'))


class DataProvider(LoadableObject):
	# To uncover errors, the enums of DataProvider / DataSplitter do *NOT* match
	dataInfos = ['NEntries', 'BlockName', 'Dataset', 'Locations', 'URL', 'FileList',
		'Nickname', 'DatasetID', 'Metadata', 'Provider', 'ResyncInfo']
	for id, dataInfo in enumerate(dataInfos):
		locals()[dataInfo] = id

	def __init__(self, config, datasetExpr, datasetNick, datasetID):
		(self._datasetExpr, self._datasetNick, self._datasetID) = (datasetExpr, datasetNick, datasetID)
		self._cache = None
		self.ignoreURL = config.getList('ignore files', [])
		self.sitefilter = config.getList('sites', [])
		self.emptyBlock = config.getBool('remove empty blocks', True)
		self.emptyFiles = config.getBool('remove empty files', True)
		self.limitEvents = config.getInt('limit events', -1)
		self.limitFiles = config.getInt('limit files', -1)
		nickProducer = config.get('nickname source', 'SimpleNickNameProducer')
		self._nickProducer = NickNameProducer.open(nickProducer, config)


	# Parse dataset format [NICK : [PROVIDER : [(/)*]]] DATASET
	def parseDatasetExpr(config, expression, defaultProvider):
		(nickname, provider, dataset) = ('', defaultProvider, None)
		temp = map(str.strip, expression.split(':', 2))
		providerMap = dict(map(lambda (x, y): (y, x), DataProvider.providers.items()))

		if len(temp) == 3:
			(nickname, provider, dataset) = temp
			if dataset.startswith('/'):
				dataset = '/' + dataset.lstrip('/')
			provider = providerMap.get(provider.lower(), provider)
		elif len(temp) == 2:
			(nickname, dataset) = temp
		elif len(temp) == 1:
			dataset = temp[0]
		return (nickname, provider, dataset)
	parseDatasetExpr = staticmethod(parseDatasetExpr)


	# Create a new DataProvider instance
	def create(config, dataset, defaultProvider, dsId = 0):
		if '\n' in dataset:
			return DataProvider.open('DataMultiplexer', config, dataset, defaultProvider)
		else:
			(dsNick, dsProv, dsExpr) = DataProvider.parseDatasetExpr(config, dataset, defaultProvider)
			config = config.addSections(['dataset']).addNames([dsNick, str(dsId)])
			section = ['dataset %s' % dsNick, 'dataset %s' % dsId, 'dataset', 'dataset'] # TODO: remove
			return DataProvider.open(dsProv, config, dsExpr, dsNick, dsId)
	create = staticmethod(create)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 60 # 1 minute delay minimum


	# Check if splitter is valid
	def checkSplitter(self, splitter):
		return splitter


	# Cached access to list of block dicts, does also the validation checks
	def getBlocks(self):
		self.allEvents = 0
		def processBlocks():
			# Validation, Filtering & Naming:
			for block in self.getBlocksInternal():
				block.setdefault(DataProvider.BlockName, '0')
				block.setdefault(DataProvider.Provider, self.__class__.__name__)
				if self._datasetID:
					block[DataProvider.DatasetID] = self._datasetID
				if self._datasetNick:
					block[DataProvider.Nickname] = self._datasetNick
				else:
					block[DataProvider.Nickname] = self._nickProducer.process(block)

				# Filter file list
				events = sum(map(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
				if block.setdefault(DataProvider.NEntries, events) != events:
					utils.eprint('WARNING: Inconsistency in block %s#%s: Number of events doesn\'t match (b:%d != f:%d)'
						% (block[DataProvider.Dataset], block[DataProvider.BlockName], block[DataProvider.NEntries], events))

				# Filter ignored and empty files
				block[DataProvider.FileList] = filter(lambda x: x[DataProvider.URL] not in self.ignoreURL, block[DataProvider.FileList])
				if self.emptyFiles:
					block[DataProvider.FileList] = filter(lambda x: x[DataProvider.NEntries] != 0, block[DataProvider.FileList])

				# Filter dataset sites
				if block.setdefault(DataProvider.Locations, None) != None:
					sites = utils.doBlackWhiteList(block[DataProvider.Locations], self.sitefilter, onEmpty = [], preferWL = False)
					if len(sites) == 0 and len(block[DataProvider.FileList]) != 0:
						utils.eprint('WARNING: Block %s#%s is not available at any site!'
							% (block[DataProvider.Dataset], block[DataProvider.BlockName]))
					block[DataProvider.Locations] = sites

				# Filter by number of files
				block[DataProvider.FileList] = block[DataProvider.FileList][:QM(self.limitFiles < 0, None, self.limitFiles)]

				# Filter by event count
				class EventCounter:
					def __init__(self, start, limit):
						(self.counter, self.limit) = (start, limit)
					def accept(self, fi):
						if (self.limit < 0) or (self.counter + fi[DataProvider.NEntries] <= self.limit):
							self.counter += fi[DataProvider.NEntries]
							return True
						return False
				eventCounter = EventCounter(self.allEvents, self.limitEvents)
				block[DataProvider.FileList] = filter(eventCounter.accept, block[DataProvider.FileList])
				block[DataProvider.NEntries] = eventCounter.counter - self.allEvents
				self.allEvents = eventCounter.counter

				# Filter empty blocks
				if not (self.emptyBlock and block[DataProvider.NEntries] == 0):
					yield block

		if self._cache == None:
			if self._datasetExpr:
				log = utils.ActivityLog('Retrieving %s' % self._datasetExpr)
			self._cache = list(processBlocks())
			if self._datasetNick:
				utils.vprint('%s:' % self._datasetNick, newline = False)
			elif self.__class__.__name__ == 'DataMultiplexer':
				utils.vprint('Summary:', newline = False)
			units = QM(self.allEvents < 0, '%d files' % -self.allEvents, '%d events' % self.allEvents)
			utils.vprint('Running over %s split into %d blocks.' % (units, len(self._cache)))
		return self._cache


	# List of block dicts with format
	# { NEntries: 123, Dataset: '/path/to/data', Block: 'abcd-1234', Locations: ['site1','site2'],
	#   Filelist: [{URL: '/path/to/file1', NEntries: 100}, {URL: '/path/to/file2', NEntries: 23}]}
	def getBlocksInternal(self):
		raise AbstractError


	def clearCache(self):
		self._cache = None


	# Print information about datasets
	def printDataset(self, level = 2):
		utils.vprint('Provided datasets:', level)
		idList = [(DataProvider.DatasetID, 0), (DataProvider.Dataset, None), (DataProvider.Nickname, '""')]
		for block in self.getBlocks():
			utils.vprint('ID - Dataset - Nick : %s - %s - %s' % tuple(map(lambda (k, d): block.get(k, d), idList)), level)
			utils.vprint('BlockName : %s' % block[DataProvider.BlockName], level)
			utils.vprint('#Events   : %s' % block[DataProvider.NEntries], level)
			seList = QM(block[DataProvider.Locations] != None, block[DataProvider.Locations], ['Not specified'])
			utils.vprint('SE List   : %s' % str.join(', ',  seList), level)
			utils.vprint('Files     : ', level)
			for fi in block[DataProvider.FileList]:
				utils.vprint('%s (Events: %d)' % (fi[DataProvider.URL], fi[DataProvider.NEntries]), level)
			utils.vprint(level = level)


	# Save dataset information in 'ini'-style => 10x faster to r/w than cPickle
	def saveStateRaw(stream, dataBlocks, stripMetadata = False):
		writer = cStringIO.StringIO()
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
	def loadState(path, config = Config()):
		# None, None = Don't override NickName and ID
		return DataProvider.open('ListProvider', config.addSections(['dataset']), path, None, None)
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

DataProvider.providers = {}
DataProvider.registerObject()
