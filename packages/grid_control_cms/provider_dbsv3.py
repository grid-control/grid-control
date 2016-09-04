# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

from grid_control.datasets import DataProvider
from grid_control.gc_exceptions import UserError
from grid_control.utils.webservice import GridJSONRestClient
from grid_control_cms.provider_cms import CMSBaseProvider
from python_compat import lmap

# required format: <dataset path>[@<instance>][#<block>]
class DBS3Provider(CMSBaseProvider):
	alias = ['dbs3', 'dbs']

	def __init__(self, config, datasetExpr, datasetNick = None):
		CMSBaseProvider.__init__(self, config, datasetExpr, datasetNick)
		if self._datasetInstance.startswith('http'):
			self._url = self._datasetInstance
		else:
			self._url = 'https://cmsweb.cern.ch/dbs/%s/DBSReader' % self._datasetInstance
		self._usePhedex = (self._url == 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader') # Use DBS locality for private samples
		self._gjrc = GridJSONRestClient(self._url, 'VOMS proxy needed to query DBS3!', UserError)


	def _queryDBSv3(self, api, **kwargs):
		return self._gjrc.get(api = api, params = kwargs)


	def _getCMSDatasets(self, datasetPath):
		pd, sd, dt = (datasetPath.lstrip('/') + '/*/*/*').split('/')[:3]
		tmp = self._queryDBSv3('datasets', primary_ds_name = pd, processed_ds_name = sd, data_tier_name = dt)
		return lmap(lambda x: x['dataset'], tmp)


	def _getCMSBlocksImpl(self, datasetPath, getSites):
		def getNameSEList(blockinfo):
			if getSites:
				return (blockinfo['block_name'], [(blockinfo['origin_site_name'], None, True)])
			return (blockinfo['block_name'], None)
		return lmap(getNameSEList, self._queryDBSv3('blocks', dataset = datasetPath, detail = getSites))


	def _getCMSFilesImpl(self, blockPath, onlyValid, queryLumi):
		for fi in self._queryDBSv3('files', block_name = blockPath, detail = True):
			if (fi['is_file_valid'] == 1) or not onlyValid:
				yield ({DataProvider.URL: fi['logical_file_name'], DataProvider.NEntries: fi['event_count']}, None)


	def _getCMSLumisImpl(self, blockPath):
		result = {}
		for lumiInfo in self._queryDBSv3('filelumis', block_name = blockPath):
			tmp = (int(lumiInfo['run_num']), lmap(int, lumiInfo['lumi_section_num']))
			result.setdefault(lumiInfo['logical_file_name'], []).append(tmp)
		return result


	def _getBlocksInternal(self):
		return self._getGCBlocks(usePhedex = self._usePhedex)
