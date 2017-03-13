# | Copyright 2015-2017 Karlsruhe Institute of Technology
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

import os
from grid_control.datasets.provider_base import DatasetError
from grid_control.datasets.provider_scan import GCProvider
from grid_control.utils import replace_with_dict
from grid_control.utils.parsing import str_guid
from hpfwk import clear_current_exception


class DBSInfoProvider(GCProvider):
	alias_list = ['dbsinfo']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None):
		tmp = ['OutputDirsFromConfig', 'MetadataFromTask']
		if os.path.isdir(dataset_expr):
			tmp = ['OutputDirsFromWork']
		tmp.extend(['JobInfoFromOutputDir', 'ObjectsFromCMSSW', 'FilesFromJobInfo', 'MetadataFromCMSSW',
			'ParentLookup', 'SEListFromPath', 'LFNFromPath', 'DetermineEvents', 'FilterEDMFiles'])
		config.set('scanner', str.join(' ', tmp))
		config.set('include config infos', 'True')
		config.set('parent keys', 'CMSSW_PARENT_LFN CMSSW_PARENT_PFN')
		config.set('events key', 'CMSSW_EVENTS_WRITE')
		GCProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick)
		self._discovery = config.get_bool('discovery', False)

	def _get_block_name(self, metadata_dict, hash_block):
		return str_guid(hash_block)

	def _get_dataset_name(self, metadata_dict, hash_dataset):
		if self._discovery:
			return GCProvider._get_dataset_name(self, metadata_dict, hash_dataset)
		if 'CMSSW_DATATIER' not in metadata_dict:
			raise DatasetError('Incompatible data tiers in dataset: %s' % repr(metadata_dict))

		def _get_path_components(path):
			if path:
				return path.strip('/').split('/')
			return []
		user_dataset_part_list = tuple(_get_path_components(self._dataset_pattern))

		(primary, processed, tier) = (None, None, None)
		# In case of a child dataset, use the parent infos to construct new path
		for parent in metadata_dict.get('PARENT_PATH', []):
			if len(user_dataset_part_list) == 3:
				(primary, processed, tier) = user_dataset_part_list
			else:
				try:
					(primary, processed, tier) = tuple(_get_path_components(parent))
				except Exception:
					clear_current_exception()
		if (primary is None) and (len(user_dataset_part_list) > 0):
			primary = user_dataset_part_list[0]
			user_dataset_part_list = user_dataset_part_list[1:]

		if len(user_dataset_part_list) == 2:
			(processed, tier) = user_dataset_part_list
		elif len(user_dataset_part_list) == 1:
			(processed, tier) = (user_dataset_part_list[0], metadata_dict['CMSSW_DATATIER'])
		elif len(user_dataset_part_list) == 0:
			(processed, tier) = ('Dataset_%s' % hash_dataset, metadata_dict['CMSSW_DATATIER'])

		raw_dataset_name = '/%s/%s/%s' % (primary, processed, tier)
		if None in (primary, processed, tier):
			raise DatasetError('Invalid dataset name supplied: %r\nresulting in %s' % (
				self._dataset_pattern, raw_dataset_name))
		return replace_with_dict(raw_dataset_name, metadata_dict)
