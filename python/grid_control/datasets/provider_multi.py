from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DatasetError, GridError
from provider_base import DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, config, datasetExpr, defaultProvider, datasetID = None):
		# None, None = Don't override NickName and ID
		DataProvider.__init__(self, config, datasetExpr, None, None)
		self._datasetExpr = None
		self.subprovider = []

		# Allow provider shortcuts
		head = ["ID", "Nickname", "Dataset path"]
		for id, entry in enumerate(datasetExpr.split('\n')):
			(datasetNick, provider, datasetExpr) = DataProvider.parseDatasetExpr(entry, defaultProvider)
			source = DataProvider.open(provider, config, datasetExpr, datasetNick, id)
			dataUrl = "%s://%s" % (DataProvider.providers.get(provider, provider), datasetExpr)
			self.subprovider.append(dict(zip(["src"] + head, [source, id, datasetNick, dataUrl])))

		print('Using the following datasets:')
		print
		utils.printTabular(zip(head, head), self.subprovider, "rcl")
		print


	def checkSplitter(self, splitter):
		for provider in self.subprovider:
			splitter = provider.checkSplitter(splitter)
		proposal = splitter
		for provider in self.subprovider:
			splitter = provider.checkSplitter(splitter)
		if proposal != splitter:
			raise DatasetError('Dataset providers could not agree on valid dataset splitter!')
		return splitter


	def getBlocksInternal(self):
		result = []
		exceptions = []
		for provider in self.subprovider:
			try:
				result.extend(provider["src"].getBlocks())
			except GridError, e:
				exceptions.append(e)
			if self.config.opts.abort:
				raise DatasetError('Could not retrieve all datasets!')
		for e in exceptions:
			e.showMessage()
		if len(exceptions):
			raise DatasetError('Could not retrieve all datasets!')
		return result
