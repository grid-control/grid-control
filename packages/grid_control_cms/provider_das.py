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

import time
from grid_control.datasets import DataProvider
from grid_control.gc_exceptions import UserError
from grid_control.utils.webservice import GridJSONRestClient
from grid_control_cms.provider_cms import CMSBaseProvider
from python_compat import lmap

class DASRetry(Exception):
	pass

class DASRestClient(GridJSONRestClient):
	def _process_json_result(self, value):
		if len(value) == 32:
			raise DASRetry
		return GridJSONRestClient._process_json_result(self, value)

# required format: <dataset path>[@<instance>][#<block>]
class DASProvider(CMSBaseProvider):
	alias = ['das']

	def __init__(self, config, datasetExpr, datasetNick = None):
		CMSBaseProvider.__init__(self, config, datasetExpr, datasetNick)
		self._url = config.get('das instance', 'https://cmsweb.cern.ch/das/cache', onChange = self._changeTrigger)
		if self._datasetInstance.startswith('http'):
			self._url = self._datasetInstance
			self._datasetInstance = ''
		self._gjrc = DASRestClient(self._url, 'VOMS proxy needed to query DAS!', UserError)


	def _queryDAS(self, query):
		if self._datasetInstance not in ('', 'prod/global'):
			query += ' instance=%s' % self._datasetInstance
		(start, sleep) = (time.time(), 0.4)
		while time.time() - start < 60:
			try:
				return self._gjrc.get(params = {'input': query})['data']
			except DASRetry:
				time.sleep(sleep)
				sleep += 0.4


	def _getCMSDatasets(self, datasetPath):
		for datasetInfo in self._queryDAS('dataset dataset=%s' % datasetPath):
			for serviceResult in datasetInfo['dataset']:
				yield serviceResult['name']


	def _getCMSBlocksImpl(self, datasetPath, getSites):
		for blockInfo in self._queryDAS('block dataset=%s' % datasetPath):
			replica_infos = None
			origin = []
			name = None
			for serviceResult in blockInfo['block']:
				name = serviceResult.get('name', name)
				if 'replica' in serviceResult:
					replica_infos = lmap(lambda r: (r['site'], r['se'], r['complete'] == 'y'), serviceResult['replica'])
				if 'origin_site_name' in serviceResult:
					origin = [(serviceResult['origin_site_name'], None, True)]
			if replica_infos is None:
				replica_infos = origin
			if name:
				yield (name, replica_infos)


	def _getCMSFilesImpl(self, blockPath, onlyValid, queryLumi):
		for fileInfo in self._queryDAS('file block=%s' % blockPath):
			for serviceResult in fileInfo['file']:
				if 'nevents' in serviceResult:
					yield ({DataProvider.URL: serviceResult['name'], DataProvider.NEntries: serviceResult['nevents']}, None)


	def _getBlocksInternal(self):
		return self._getGCBlocks(usePhedex = False)
