from __future__ import generators
import sys, os, gzip, cStringIO, copy
from grid_control import utils, AbstractObject, RuntimeError, ConfigError

class DataProvider(AbstractObject):
	dataInfos = ('Dataset', 'BlockName', 'NEvents', 'SEList', 'FileList', 'lfn', 'Nickname', 'DatasetID')
	for id, dataInfo in enumerate(dataInfos):
		locals()[dataInfo] = id

	def __init__(self, datasetExpr, datasetNick, datasetID):
		self._datasetExpr = datasetExpr
		self._datasetNick = datasetNick
		self._datasetID = datasetID
		self._cache = None


	# Parse dataset format [NICK : [PROVIDER : [(/)*]]] DATASET
	def parseDatasetExpr(expression, dbsProvider):
		temp = map(lambda x: x.strip(), expression.split(':'))
		nickname = ''
		provider = dbsProvider
		providerMap = { 'dbs': dbsProvider, 'file': 'FileProvider', 'list': 'ListProvider' }

		if len(temp) == 3:
			(nickname, provider, dataset) = temp
			if dataset[0] == '/':
				dataset = '/' + dataset.lstrip('/')
			provider = providerMap.get(provider.lower(), provider)
		elif len(temp) == 2:
			(nickname, dataset) = temp
		elif len(temp) == 1:
			(dataset) = temp[0]

		return (nickname, provider, dataset)
	parseDatasetExpr = staticmethod(parseDatasetExpr)


	# Create a new DataProvider instance
	def create(dataset, dbsapi):
		if "\n" in dataset:
			return DataProvider.open("DataMultiplexer", dataset, dbsapi)
		else:
			(nick, provider, datasetExpr) = DataProvider.parseDatasetExpr(dataset, dbsapi)
			return DataProvider.open(provider, datasetExpr, nick, 0)
	create = staticmethod(create)


	# Cached access to list of block dicts, does also the validation checks
	def getBlocks(self):
		if self._cache == None:
			if self._datasetExpr != None:
				log = utils.ActivityLog('Retrieving %s' % self._datasetExpr)
			self._cache = self.getBlocksInternal()
			# Validation:
#			log = utils.ActivityLog('Validating %s' % self._datasetExpr)
			for block in self._cache:
				if self._datasetNick:
					block[DataProvider.Nickname] = self._datasetNick
				if self._datasetID:
					block[DataProvider.DatasetID] = self._datasetID

				events = 0
				for file in block[DataProvider.FileList]:
					events += file[DataProvider.NEvents]
				if events != block[DataProvider.NEvents]:
					print('Inconsistency in block %s#%s: Number of events doesn\'t match (b:%d != f:%d)'
						% (block[DataProvider.Dataset], block[DataProvider.BlockName], block[DataProvider.NEvents], events))
		return self._cache


	# List of block dicts with format
	# { NEvents: 123, Dataset: '/path/to/data', Block: 'abcd-1234', SEList: ['site1','site2'],
	#   Filelist: [{lfn: '/path/to/file1', NEvents: 100}, {lfn: '/path/to/file2', NEvents: 23}]}
	def getBlocksInternal(self):
		raise AbstractError


	# Print information about datasets
	def printDataset(self):
		print "Matching blocks:"
		for block in self.getBlocks():
			print "ID / Dataset / Nick : ",
			print block[DataProvider.DatasetID], block[DataProvider.Dataset], block[DataProvider.Nickname]
			print "BlockName : ", block[DataProvider.BlockName]
			print "#Events   : ", block[DataProvider.NEvents]
			print "SE List   : ", block[DataProvider.SEList]
			print "Files     : "
			for fi in block[DataProvider.FileList]:
				print "%s (Events: %d)" % (fi[DataProvider.lfn], fi[DataProvider.NEvents])
			print


	# Save dataset information in "ini"-style => 10x faster to r/w than cPickle
	def saveState(self, path):
		writer = cStringIO.StringIO()
		for block in self.getBlocks():
			writer.write("[%s#%s]\n" % (block[DataProvider.Dataset], block[DataProvider.BlockName]))
			if block.has_key(DataProvider.Nickname):
				writer.write('nickname = %s\n' % block[DataProvider.Nickname])
			if block.has_key(DataProvider.DatasetID):
				writer.write('id = %d\n' % block[DataProvider.DatasetID])
			writer.write('events = %d\n' % block[DataProvider.NEvents])
			writer.write('se list = %s\n' % str.join(',', block[DataProvider.SEList]))
			for fi in block[DataProvider.FileList]:
				writer.write('%s = %d\n' % (fi[DataProvider.lfn], fi[DataProvider.NEvents]))
			writer.write('\n')
		open(os.path.join(path, 'datacache.dat'), 'wb').write(writer.getvalue())


	# Load dataset information using ListProvider
	def loadState(path):
		# None, None = Don't override NickName and ID
		return DataProvider.open('ListProvider', os.path.join(path, 'datacache.dat'), None, None)
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
