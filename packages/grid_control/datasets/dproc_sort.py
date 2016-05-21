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

	def __init__(self, config):
		DataProcessor.__init__(self, config)
		self._sortDS = config.getBool('dataset sort', False, onChange = DataProcessor.triggerDataResync)
		self._sortBlock = config.getBool('dataset block sort', False, onChange = DataProcessor.triggerDataResync)
		self._sortFiles = config.getBool('dataset files sort', False, onChange = DataProcessor.triggerDataResync)
		self._sortLocation = config.getBool('dataset location sort', False, onChange = DataProcessor.triggerDataResync)

	def enabled(self):
		return self._sortDS or self._sortBlock or self._sortFiles or self._sortLocation

	def process(self, blockIter):
		if self._sortDS:
			dsCache = {}
			for block in blockIter:
				dsCache.setdefault(block[DataProvider.Dataset], []).append(block)
			def ds_generator():
				for ds in sorted(dsCache):
					if self._sortBlock:
						sort_inplace(dsCache[ds], key = itemgetter(DataProvider.BlockName))
					for block in dsCache[ds]:
						yield block
			blockIter = ds_generator()
		elif self._sortBlock:
			blockIter = sorted(blockIter, key = itemgetter(DataProvider.BlockName))
		# Yield blocks
		for block in blockIter:
			if self._sortFiles:
				sort_inplace(block[DataProvider.FileList], key = itemgetter(DataProvider.URL))
			if self._sortLocation:
				sort_inplace(block[DataProvider.Locations])
			yield block
