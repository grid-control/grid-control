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
from grid_control.utils.parsing import parse_json, parse_list
from python_compat import sorted


# Provides dataset information from a config file
# required format: <config section>
class ConfigDataProvider(DataProvider):
	alias_list = ['config']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick = None, dataset_proc = None):
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)

		ds_config = config.changeView(view_class = 'SimpleConfigView', setSections = ['datasource %s' % dataset_expr])
		self._block = self._readBlockFromConfig(ds_config, dataset_expr, dataset_nick)

		def onChange(config, old_obj, cur_obj, cur_entry, obj2str):
			self._log.critical('Dataset %r changed', dataset_expr)
			return triggerResync(['datasets', 'parameters'])(config, old_obj, cur_obj, cur_entry, obj2str)
		ds_config.get('%s hash' % datasource_name, self.get_hash(), persistent = True, onChange = onChange)


	def _readFileFromConfig(self, ds_config, url, metadata_name_list, common_metadata, common_prefix):
		info = ds_config.get(url, onChange = None)
		tmp = info.split(' ', 1)
		fi = {DataProvider.URL: common_prefix + url, DataProvider.NEntries: int(tmp[0])}
		if common_metadata:
			fi[DataProvider.Metadata] = common_metadata
		if len(tmp) == 2:
			file_metadata = parse_json(tmp[1])
			if len(common_metadata) + len(file_metadata) > len(metadata_name_list):
				raise DatasetError('Unable to set %d file metadata items with %d metadata keys (%d common metadata items)' %
					(len(file_metadata), len(metadata_name_list), len(common_metadata)))
			fi[DataProvider.Metadata] = fi.get(DataProvider.Metadata, []) + file_metadata
		return fi


	def _createBlockInfo(self, ds_config, dataset_expr, dataset_nick):
		datasetNameParts = dataset_expr.split('#', 1)
		if len(datasetNameParts) == 1:
			datasetNameParts.append('0')
		return {
			DataProvider.Nickname: ds_config.get('nickname', dataset_nick or '', onChange = None),
			DataProvider.Dataset: datasetNameParts[0],
			DataProvider.BlockName: datasetNameParts[1],
		}


	def _readBlockFromConfig(self, ds_config, dataset_expr, dataset_nick):
		metadata_name_list = parse_json(ds_config.get('metadata', '[]', onChange = None))
		common_metadata = parse_json(ds_config.get('metadata common', '[]', onChange = None))
		if len(common_metadata) > len(metadata_name_list):
			raise DatasetError('Unable to set %d common metadata items with %d metadata keys' % (len(common_metadata), len(metadata_name_list)))
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
				file_list.append(self._readFileFromConfig(ds_config, url, metadata_name_list, common_metadata, common_prefix))
		if not file_list:
			raise DatasetError('There are no dataset files specified for dataset %r' % dataset_expr)

		result = self._createBlockInfo(ds_config, dataset_expr, dataset_nick)
		result[DataProvider.FileList] = sorted(file_list, key = lambda fi: fi[DataProvider.URL])
		if metadata_name_list:
			result[DataProvider.Metadata] = metadata_name_list
		if has_events:
			result[DataProvider.NEntries] = ds_config.getInt('events', -1, onChange = None)
		if has_se_list:
			result[DataProvider.Locations] = parse_list(ds_config.get('se list', '', onChange = None), ',')
		return result


	def _getBlocksInternal(self):
		yield copy.deepcopy(self._block) # dataset processors can modify metadata inplace
