from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DatasetError, GridError
from provider_base import DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, config, datasetExpr, dbsapi, datasetID = None):
		# None, None = Don't override NickName and ID
		DataProvider.__init__(self, config, datasetExpr, None, None)
		self._datasetExpr = None
		self.subprovider = []

		exprList = datasetExpr.split('\n')
		providerMap = { 'dbs': dbsapi, 'file': 'FileProvider', 'list': 'ListProvider' }
		reverseMap = dict(map(lambda (x,y): (y,x), providerMap.items()))
		head = ["ID", "Nickname", "Dataset path"]

		# Allow provider shortcuts
		for id, entry in enumerate(exprList):
			(datasetNick, provider, datasetExpr) = DataProvider.parseDatasetExpr(entry, dbsapi)
			source = DataProvider.open(provider, config, datasetExpr, datasetNick, id)
			dataUrl = "%s://%s" % (reverseMap.get(provider, provider), datasetExpr)
			self.subprovider.append(dict(zip(["src"] + head, [source, id, datasetNick, dataUrl])))

		print('Using the following datasets:')
		print
		utils.printTabular(zip(head, head), self.subprovider)
		print


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
