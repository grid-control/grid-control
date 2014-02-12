from grid_control import utils, DatasetError, GCError, logException
from provider_base import DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, config, datasetExpr, defaultProvider, datasetID = None):
		# ..., None, None) = Don't override NickName and ID
		DataProvider.__init__(self, config, None, None, None)
		mkProvider = lambda (id, entry): DataProvider.create(config, entry, defaultProvider, id)
		self.subprovider = map(mkProvider, enumerate(datasetExpr.splitlines()))


	def queryLimit(self):
		return max(map(lambda x: x.queryLimit(), self.subprovider))


	def checkSplitter(self, splitter):
		getProposal = lambda x: reduce(lambda prop, prov: prov.checkSplitter(prop), self.subprovider, x)
		if getProposal(splitter) != getProposal(getProposal(splitter)):
			raise DatasetError('Dataset providers could not agree on valid dataset splitter!')
		return getProposal(splitter)


	def getBlocksInternal(self):
		exceptions = ''
		for provider in self.subprovider:
			try:
				for block in provider.getBlocks():
					yield block
			except:
				exceptions += logException() + '\n'
			if utils.abort():
				raise DatasetError('Could not retrieve all datasets!')
		if exceptions:
			raise DatasetError('Could not retrieve all datasets!\n' + exceptions)
