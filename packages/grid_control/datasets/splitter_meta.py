# | Copyright 2011-2016 Karlsruhe Institute of Technology
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
from hpfwk import AbstractError
from python_compat import imap, sort_inplace

# Split dataset along block and metadata boundaries - using equivalence classes of metadata
class MetadataSplitter(FileLevelSplitter):
	def _classify_fi(self, metadata_key_list, block, fi):
		raise AbstractError

	def _proto_partition_blocks(self, blocks):
		for block in blocks:
			files = block[DataProvider.FileList]
			sort_inplace(files, key = lambda fi: self._classify_fi(block.get(DataProvider.Metadata, []), block, fi))
			(fi_list, reprKey) = ([], None)
			for fi in files:
				if reprKey is None:
					reprKey = self._classify_fi(block[DataProvider.Metadata], block, fi)
				curKey = self._classify_fi(block[DataProvider.Metadata], block, fi)
				if curKey != reprKey:
					yield self._create_partition(block, fi_list)
					(fi_list, reprKey) = ([], curKey)
				fi_list.append(fi)
			yield self._create_partition(block, fi_list)


class UserMetadataSplitter(MetadataSplitter):
	alias = ['metadata']

	def _configure_splitter(self, config):
		self._metadata_user_list = self._query_config(config.getList, 'split metadata', [])

	def _classify_fi(self, metadata_key_list, block, fi):
		metadata_idx_list = []
		metadata_selected_list = self._setup(self._metadata_user_list, block)
		for metadata_selected in metadata_selected_list:
			if metadata_selected in metadata_key_list:
				metadata_idx_list.append(metadata_key_list.index(metadata_selected))
			else:
				metadata_idx_list.append(-1)

		def query_metadata(idx):
			if (idx >= 0) and (idx < len(fi[DataProvider.Metadata])):
				return fi[DataProvider.Metadata][idx]
			return ''
		return tuple(imap(query_metadata, metadata_idx_list))
