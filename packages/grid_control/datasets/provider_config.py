# | Copyright 2016-2017 Karlsruhe Institute of Technology
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
from grid_control.config import TriggerResync
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.parsing import parse_json, parse_list
from python_compat import sorted


class ConfigDataProvider(DataProvider):
	# Provides dataset information from a config file
	# required format: <config section>
	alias_list = ['config']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)

		ds_config = config.change_view(view_class='SimpleConfigView',
			set_sections=['datasource %s' % dataset_expr])
		self._block = self._read_block(ds_config, dataset_expr, dataset_nick)

		def _on_change(config, old_obj, cur_obj, cur_entry, obj2str):
			self._log.critical('Dataset %r changed', dataset_expr)
			return TriggerResync(['datasets', 'parameters'])(config, old_obj, cur_obj, cur_entry, obj2str)
		ds_config.get('dataset hash', self._get_dataset_hash(), persistent=True, on_change=_on_change)

	def _iter_blocks_raw(self):
		yield copy.deepcopy(self._block)  # dataset processors might modify metadata inplace

	def _read_block(self, ds_config, dataset_expr, dataset_nick):
		metadata_name_list = parse_json(ds_config.get('metadata', '[]', on_change=None))
		common_metadata = parse_json(ds_config.get('metadata common', '[]', on_change=None))
		if len(common_metadata) > len(metadata_name_list):
			raise DatasetError('Unable to set %d common metadata items ' % len(common_metadata) +
				'with %d metadata keys' % len(metadata_name_list))
		common_prefix = ds_config.get('prefix', '', on_change=None)
		fn_list = []
		has_events = False
		has_se_list = False
		for url in ds_config.get_option_list():
			if url == 'se list':
				has_se_list = True
			elif url == 'events':
				has_events = True
			elif url not in ['dataset hash', 'metadata', 'metadata common', 'nickname', 'prefix']:
				fi = self._read_fi(ds_config, url, metadata_name_list, common_metadata, common_prefix)
				fn_list.append(fi)
		if not fn_list:
			raise DatasetError('There are no dataset files specified for dataset %r' % dataset_expr)

		result = {
			DataProvider.Nickname: ds_config.get('nickname', dataset_nick or '', on_change=None),
			DataProvider.FileList: sorted(fn_list, key=lambda fi: fi[DataProvider.URL])
		}
		result.update(DataProvider.parse_block_id(dataset_expr))
		if metadata_name_list:
			result[DataProvider.Metadata] = metadata_name_list
		if has_events:
			result[DataProvider.NEntries] = ds_config.get_int('events', -1, on_change=None)
		if has_se_list:
			result[DataProvider.Locations] = parse_list(ds_config.get('se list', '', on_change=None), ',')
		return result

	def _read_fi(self, ds_config, url, metadata_name_list, common_metadata, common_prefix):
		info = ds_config.get(url, on_change=None)
		tmp = info.split(' ', 1)
		fi = {DataProvider.URL: common_prefix + url, DataProvider.NEntries: int(tmp[0])}
		if common_metadata:
			fi[DataProvider.Metadata] = common_metadata
		if len(tmp) == 2:
			file_metadata = parse_json(tmp[1])
			if len(common_metadata) + len(file_metadata) > len(metadata_name_list):
				raise DatasetError('Unable to set %d file metadata items ' % len(file_metadata) +
					'with %d metadata keys ' % len(metadata_name_list) +
					'(%d common metadata items)' % len(common_metadata))
			fi[DataProvider.Metadata] = fi.get(DataProvider.Metadata, []) + file_metadata
		return fi
