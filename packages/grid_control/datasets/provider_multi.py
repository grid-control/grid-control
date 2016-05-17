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

import logging
from grid_control import utils
from grid_control.datasets.provider_base import DataProvider, DatasetError
from hpfwk import ExceptionCollector
from python_compat import imap, reduce

class MultiDatasetProvider(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick, datasetID, providerList):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		self._providerList = providerList
		for provider in self._providerList:
			provider.setPassthrough()


	def queryLimit(self):
		return max(imap(lambda x: x.queryLimit(), self._providerList))


	def checkSplitter(self, splitter):
		def getProposal(x):
			return reduce(lambda prop, prov: prov.checkSplitter(prop), self._providerList, x)
		if getProposal(splitter) != getProposal(getProposal(splitter)):
			raise DatasetError('Dataset providers could not agree on valid dataset splitter!')
		return getProposal(splitter)


	def getDatasets(self):
		if self._cache_dataset is None:
			self._cache_dataset = []
			ec = ExceptionCollector()
			for provider in self._providerList:
				try:
					self._cache_dataset.extend(provider.getDatasets())
				except Exception:
					ec.collect()
				if utils.abort():
					raise DatasetError('Could not retrieve all datasets!')
			ec.raise_any(DatasetError('Could not retrieve all datasets!'))
		return self._cache_dataset


	def getBlocks(self, silent = True):
		if self._cache_block is None:
			ec = ExceptionCollector()
			def getAllBlocks():
				for provider in self._providerList:
					try:
						for block in provider.getBlocks(silent):
							yield block
					except Exception:
						ec.collect()
					if utils.abort():
						raise DatasetError('Could not retrieve all datasets!')
			self._cache_block = list(self._stats.process(self._datasetProcessor.process(getAllBlocks())))
			ec.raise_any(DatasetError('Could not retrieve all datasets!'))
			logging.getLogger('user').info('Summary: Running over %d block(s) containing %s', *self._stats.getStats())
		return self._cache_block
