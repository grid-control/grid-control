from __future__ import generators
import sys, os, copy
from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DatasetError, GridError
from provider_base import DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, datasetExpr, dbsapi, datasetID = None):
		# None, None = Don't override NickName and ID
		DataProvider.__init__(self, datasetExpr, None, None)
		self.subprovider = []

		exprList = datasetExpr.split('\n')
		maxlen = reduce(max, map(lambda x: len(DataProvider.parseDatasetExpr(x, dbsapi)[2]), exprList))

		print('Using the following datasets:')
		print(' %6s | %15s | %s' % ('ID'.center(6), 'Nickname'.center(15), 'Dataset path'))
		print('=%6s=+=%15s=+=%s' % ('=' * 6, '=' * 15, '=' * (maxlen + 6)))

		providerMap = { 'dbs': dbsapi, 'file': 'FileProvider', 'list': 'ListProvider' }
		reverseMap = dict(map(lambda (x,y): (y,x), providerMap.items()))

		# Allow provider shortcuts
		for id, entry in enumerate(exprList):
			(datasetNick, provider, datasetExpr) = DataProvider.parseDatasetExpr(entry.strip(), dbsapi)
			source = DataProvider.open(provider, datasetExpr, datasetNick, id)
			self.subprovider.append(source)

			providerNick = reverseMap.get(provider, provider)
			print(' %6i | %s | %s://%s' % (id, datasetNick.center(15), providerNick, datasetExpr))
		print


	def getBlocksInternal(self):
		result = []
		exceptions = []
		for provider in self.subprovider:
			try:
				result.extend(provider.getBlocks())
			except GridError, e:
				exceptions.append(e)
		for e in exceptions:
			e.showMessage()
		if len(exceptions):
			raise DatasetError('Could not retrieve all datasets!')
		return result
