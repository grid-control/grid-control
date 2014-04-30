#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

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
