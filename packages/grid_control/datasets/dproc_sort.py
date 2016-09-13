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

from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider
from python_compat import itemgetter, sort_inplace, sorted

class SortingDataProcessor(DataProcessor):
	alias = ['sort']

	def __init__(self, config, datasource_name, on_change):
		DataProcessor.__init__(self, config, datasource_name, on_change)
		self._sort_ds = config.getBool('%s sort' % datasource_name, False, onChange = on_change)
		self._sort_block = config.getBool('%s block sort' % datasource_name, False, onChange = on_change)
		self._sort_files = config.getBool('%s files sort' % datasource_name, False, onChange = on_change)
		self._sort_location = config.getBool('%s location sort' % datasource_name, False, onChange = on_change)

	def enabled(self):
		return self._sort_ds or self._sort_block or self._sort_files or self._sort_location

	def process(self, block_iter):
		if self._sort_ds:
			map_dataset2block_list = {}
			for block in block_iter:
				map_dataset2block_list.setdefault(block[DataProvider.Dataset], []).append(block)
			def iter_blocks_by_dataset():
				for ds in sorted(map_dataset2block_list):
					if self._sort_block:
						sort_inplace(map_dataset2block_list[ds], key = itemgetter(DataProvider.BlockName))
					for block in map_dataset2block_list[ds]:
						yield block
			block_iter = iter_blocks_by_dataset()
		elif self._sort_block:
			block_iter = sorted(block_iter, key = itemgetter(DataProvider.BlockName))
		# Yield blocks
		for block in block_iter:
			if self._sort_files:
				sort_inplace(block[DataProvider.FileList], key = itemgetter(DataProvider.URL))
			if self._sort_location:
				sort_inplace(block[DataProvider.Locations])
			yield block
