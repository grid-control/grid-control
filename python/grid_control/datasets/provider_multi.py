import sys
from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DatasetError, GCError
from provider_base import DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, config, section, datasetExpr, defaultProvider, datasetID = None):
		# None, None = Don't override NickName and ID
		DataProvider.__init__(self, config, section, datasetExpr, None, None)
		self._datasetExpr = None
		self.subprovider = []

		# Allow provider shortcuts
		head = ["ID", "Nickname", "Dataset path"]
		for id, entry in enumerate(datasetExpr.splitlines()):
			(datasetNick, provider, datasetExpr) = DataProvider.parseDatasetExpr(entry, defaultProvider)
			source = DataProvider.open(provider, config, section, datasetExpr, datasetNick, id)
			dataUrl = "%s://%s" % (DataProvider.providers.get(provider, provider), datasetExpr)
			self.subprovider.append(dict(zip(["src"] + head, [source, id, datasetNick, dataUrl])))

		print('Using the following datasets:')
		print
		utils.printTabular(zip(head, head), self.subprovider, "rcl")
		print


	def queryLimit(self):
		return max(map(lambda x: x['src'].queryLimit(), self.subprovider))


	def checkSplitter(self, splitter, first = None):
		def getProposal(x):
			for provider in self.subprovider:
				x = provider['src'].checkSplitter(x)
			return x
		if getProposal(splitter) != getProposal(getProposal(splitter)):
			raise DatasetError('Dataset providers could not agree on valid dataset splitter!')
		return getProposal(splitter)


	def getBlocksInternal(self):
		result = []
		exceptions = []
		for provider in self.subprovider:
			try:
				result.extend(provider["src"].getBlocks())
			except GCError:
				exceptions.append(GCError.message)
			if utils.abort():
				raise DatasetError('Could not retrieve all datasets!')
		if len(exceptions):
			sys.stderr.write(str.join("\n", exceptions))
			raise DatasetError('Could not retrieve all datasets!')
		return result
