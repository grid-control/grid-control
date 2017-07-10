# | Copyright 2011-2017 Karlsruhe Institute of Technology
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

from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.splitter_basic import FileLevelSplitter
from grid_control.utils.algos import safe_index
from hpfwk import AbstractError
from python_compat import imap, lmap, sort_inplace


class FileClassSplitter(FileLevelSplitter):
	# Split dataset along block and class boundaries - using equivalence classes of file properties
	def divide_blocks(self, block_iter):
		for block in block_iter:
			fi_list = block[DataProvider.FileList]
			sort_inplace(fi_list, key=lambda fi: self._get_fi_class(fi, block))
			partition_fi_list = []
			if fi_list:
				fi_class_active = self._get_fi_class(fi_list[0], block)
			for fi in fi_list:
				fi_class_current = self._get_fi_class(fi, block)
				if fi_class_current != fi_class_active:
					yield self._create_sub_block(block, partition_fi_list)
					(partition_fi_list, fi_class_active) = ([], fi_class_current)
				partition_fi_list.append(fi)
			if partition_fi_list:
				yield self._create_sub_block(block, partition_fi_list)

	def _get_fi_class(self, fi, block):
		raise AbstractError


class UserMetadataSplitter(FileClassSplitter):
	alias_list = ['metadata']

	def __init__(self, config, datasource_name):
		FileClassSplitter.__init__(self, config, datasource_name)
		self._metadata_user_list = config.get_lookup('split metadata', {},
			parser=str.split, strfun=lambda x: str.join(' ', x))

	def _get_fi_class(self, fi, block):
		metadata_name_list = block.get(DataProvider.Metadata, [])
		metadata_name_list_selected = self._metadata_user_list.lookup(DataProvider.get_block_id(block))
		metadata_idx_list = lmap(lambda metadata_name: safe_index(metadata_name_list, metadata_name),
			metadata_name_list_selected)

		def _query_metadata(idx):
			if (idx is not None) and (idx < len(fi[DataProvider.Metadata])):
				return fi[DataProvider.Metadata][idx]
			return ''
		return tuple(imap(_query_metadata, metadata_idx_list))
