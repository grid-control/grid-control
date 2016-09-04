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

import copy
from grid_control.config import triggerResync
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.parsing import parseJSON, parseList
from python_compat import sorted

# Provides dataset information from a config file
# required format: <config section>
class ConfigDataProvider(DataProvider):
	alias = ['config']

	def __init__(self, config, datasetExpr, datasetNick = None):
		DataProvider.__init__(self, config, datasetExpr, datasetNick)

		ds_config = config.changeView(viewClass = 'SimpleConfigView', setSections = ['datasource %s' % datasetExpr])
		self._block = self._readBlockFromConfig(ds_config, datasetExpr, datasetNick)

		def onChange(config, old_obj, cur_obj, cur_entry, obj2str):
			self._log.critical('Dataset %r changed', datasetExpr)
			return triggerResync(['datasets', 'parameters'])(config, old_obj, cur_obj, cur_entry, obj2str)
		ds_config.get('dataset hash', self.getHash(), persistent = True, onChange = onChange)


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


	def _createBlockInfo(self, ds_config, datasetExpr, datasetNick):
		datasetNameParts = datasetExpr.split('#', 1)
		if len(datasetNameParts) == 1:
			datasetNameParts.append('0')
		return {
			DataProvider.Nickname: ds_config.get('nickname', datasetNick or '', onChange = None),
			DataProvider.Dataset: datasetNameParts[0],
			DataProvider.BlockName: datasetNameParts[1],
		}


	def _readBlockFromConfig(self, ds_config, datasetExpr, datasetNick):
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
			elif url not in ['dataset hash', 'metadata', 'metadata common', 'nickname', 'prefix']:
				file_list.append(self._readFileFromConfig(ds_config, url, metadata_keys, common_metadata, common_prefix))
		if not file_list:
			raise DatasetError('There are no dataset files specified for dataset %r' % datasetExpr)

		result = self._createBlockInfo(ds_config, datasetExpr, datasetNick)
		result[DataProvider.FileList] = sorted(file_list, key = lambda fi: fi[DataProvider.URL])
		if metadata_keys:
			result[DataProvider.Metadata] = metadata_keys
		if has_events:
			result[DataProvider.NEntries] = ds_config.getInt('events', -1, onChange = None)
		if has_se_list:
			result[DataProvider.Locations] = parseList(ds_config.get('se list', '', onChange = None), ',')
		return result


	def _getBlocksInternal(self):
		yield copy.deepcopy(self._block) # dataset processors can modify metadata inplace
