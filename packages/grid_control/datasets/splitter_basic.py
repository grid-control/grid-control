# | Copyright 2010-2016 Karlsruhe Institute of Technology
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
from grid_control.datasets.splitter_base import DataSplitter
from hpfwk import AbstractError
from python_compat import imap, reduce, itemgetter

class FileLevelSplitter(DataSplitter):
	# Base class for (stackable) splitters with file level granularity
	def divide_blocks(self, block_iter):
		raise AbstractError

	def partition_blocks_raw(self, block_iter, event_first = 0):
		for sub_block in self.divide_blocks(block_iter):
			yield self._finish_partition(sub_block, dict(), sub_block[DataProvider.FileList])

	def _create_sub_block(self, block_template, fi_list):
		partition = dict(block_template)
		partition[DataProvider.FileList] = fi_list
		partition[DataProvider.NEntries] = sum(imap(itemgetter(DataProvider.NEntries), fi_list))
		return partition


class BlockBoundarySplitter(FileLevelSplitter):
	# Split only along block boundaries
	alias = ['blocks']

	def divide_blocks(self, block_iter):
		return block_iter


class FileBoundarySplitter(FileLevelSplitter):
	# Split dataset along block boundaries into jobs with 'files per job' files
	alias = ['files']

	def divide_blocks(self, block_iter):
		for block in block_iter:
			fi_idx_start = 0
			files_per_job = self._setup(self._files_per_job, block)
			while fi_idx_start < len(block[DataProvider.FileList]):
				fi_list = block[DataProvider.FileList][fi_idx_start : fi_idx_start + files_per_job]
				fi_idx_start += files_per_job
				yield self._create_sub_block(block, fi_list)

	def _configure_splitter(self, config):
		self._files_per_job = self._query_config(config.getInt, 'files per job')


class FLSplitStacker(FileLevelSplitter):
	alias = ['pipeline']

	def partition_blocks_raw(self, block_iter, event_first = 0):
		for block in block_iter:
			splitter_name_list = self._setup(self._splitter_name_list, block)
			splitter_iter = imap(lambda x: FileLevelSplitter.createInstance(x, self._config, self._datasource_name), splitter_name_list[:-1])
			splitter_final = DataSplitter.createInstance(splitter_name_list[-1], self._config, self._datasource_name)
			for sub_block in reduce(lambda x, y: y.divide_blocks(x), splitter_iter, [block]):
				for partition in splitter_final.partition_blocks_raw([sub_block]):
					yield partition

	def _configure_splitter(self, config):
		self._config = config
		self._splitter_name_list = self._query_config(config.getList, 'splitter stack', ['BlockBoundarySplitter'])


class HybridSplitter(FileLevelSplitter):
	# Split dataset along block and file boundaries into jobs with (mostly <=) 'events per job' events
	# In case of file with #events > 'events per job', use just the single file (=> job has more events!)
	alias = ['hybrid']

	def divide_blocks(self, block_iter):
		for block in block_iter:
			(events, fi_list) = (0, [])
			events_per_job = self._setup(self._events_per_job, block)
			for fileInfo in block[DataProvider.FileList]:
				if (len(fi_list) > 0) and (events + fileInfo[DataProvider.NEntries] > events_per_job):
					yield self._create_sub_block(block, fi_list)
					(events, fi_list) = (0, [])
				fi_list.append(fileInfo)
				events += fileInfo[DataProvider.NEntries]
			yield self._create_sub_block(block, fi_list)

	def _configure_splitter(self, config):
		self._events_per_job = self._query_config(config.getInt, 'events per job')
