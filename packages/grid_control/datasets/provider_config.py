# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.parsing import parseJSON, parseList
from python_compat import md5_hex, sorted

# Provides dataset information from a config file
# required format: <config section>
class ConfigDataProvider(DataProvider):
	alias = ['config']

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		ds_config = config.changeView(viewClass = 'SimpleConfigView', setSections = ['datasource %s' % datasetExpr])
		self._block = self._readBlockFromConfig(ds_config, datasetExpr, datasetNick, datasetID)

		dataset_hash_new = md5_hex(repr(self._block))
		dataset_hash_old = ds_config.get('dataset hash', dataset_hash_new, persistent = True)
		self._request_resync = dataset_hash_new != dataset_hash_old
		if self._request_resync:
			self._log.critical('Dataset %r changed', datasetExpr)
			ds_config.setState(True, 'resync', detail = 'dataset')
			ds_config.setState(True, 'resync', detail = 'parameters')
			ds_config.set('dataset hash', dataset_hash_new)


	def _readFileFromConfig(self, ds_config, url, metadata_keys, common_metadata, common_prefix):
		info = ds_config.get(url, onChange = None)
		tmp = info.split(' ', 1)
		fi = {DataProvider.URL: common_prefix + url, DataProvider.NEntries: int(tmp[0])}
		if common_metadata:
			fi[DataProvider.Metadata] = common_metadata
		if len(tmp) == 2:
			file_metadata = parseJSON(tmp[1])
			if len(common_metadata) + len(file_metadata) > len(metadata_keys):
				raise DatasetError('Unable to set %d file metadata items with %d metadata keys (%d common metadata items)' %
					(len(file_metadata), len(metadata_keys), len(common_metadata)))
			fi[DataProvider.Metadata] = fi.get(DataProvider.Metadata, []) + file_metadata
		return fi


	def _readBlockFromConfig(self, ds_config, datasetExpr, datasetNick, datasetID):
		metadata_keys = parseJSON(ds_config.get('metadata', '[]', onChange = None))
		common_metadata = parseJSON(ds_config.get('metadata common', '[]', onChange = None))
		if len(common_metadata) > len(metadata_keys):
			raise DatasetError('Unable to set %d common metadata items with %d metadata keys' % (len(common_metadata), len(metadata_keys)))
		common_prefix = ds_config.get('prefix', '', onChange = None)
		file_list = []
		has_events = False
		has_se_list = False
		for url in ds_config.getOptions():
			if url == 'se list':
				has_se_list = True
			elif url == 'events':
				has_events = True
			elif url not in ['dataset hash', 'id', 'metadata', 'metadata common', 'nickname', 'prefix']:
				file_list.append(self._readFileFromConfig(ds_config, url, metadata_keys, common_metadata, common_prefix))
		if not file_list:
			raise DatasetError('There are no dataset files specified for dataset %r' % datasetExpr)

		result = {
			DataProvider.Nickname: ds_config.get('nickname', datasetNick, onChange = None),
			DataProvider.DatasetID: ds_config.getInt('id', datasetID, onChange = None),
			DataProvider.Dataset: datasetExpr,
			DataProvider.FileList: sorted(file_list, key = lambda fi: fi[DataProvider.URL]),
		}
		if metadata_keys:
			result[DataProvider.Metadata] = metadata_keys
		if has_events:
			result[DataProvider.NEntries] = ds_config.getInt('events', -1, onChange = None)
		if has_se_list:
			result[DataProvider.Locations] = parseList(ds_config.get('se list', '', onChange = None), ',')
		return result


	def getBlocksInternal(self):
		yield self._block
