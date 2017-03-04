# | Copyright 2010-2017 Karlsruhe Institute of Technology
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
from grid_control.datasets.splitter_base import DataSplitter, PartitionError
from hpfwk import AbstractError
from python_compat import imap, itemgetter, reduce


class FileLevelSplitter(DataSplitter):
	# Base class for (stackable) splitters with file level granularity
	def divide_blocks(self, block_iter):
		raise AbstractError

	def split_partitions(self, block_iter, entry_first=0):
		for sub_block in self.divide_blocks(block_iter):
			if sub_block[DataProvider.FileList]:
				yield self._finish_partition(sub_block, dict(), sub_block[DataProvider.FileList])

	def _create_sub_block(self, block_template, fi_list):
		partition = dict(block_template)
		partition[DataProvider.FileList] = fi_list
		partition[DataProvider.NEntries] = sum(imap(itemgetter(DataProvider.NEntries), fi_list))
		return partition


class BlockBoundarySplitter(FileLevelSplitter):
	# Split only along block boundaries
	alias_list = ['blocks']

	def divide_blocks(self, block_iter):
		return block_iter


class FLSplitStacker(FileLevelSplitter):
	alias_list = ['pipeline']

	def __init__(self, config, datasource_name):
		FileLevelSplitter.__init__(self, config, datasource_name)
		part_name_list = config.get_list(self._get_part_opt('splitter stack'), ['BlockBoundarySplitter'])
		self._part_list = []
		for part_name in part_name_list[:-1]:
			self._part_list.append(FileLevelSplitter.create_instance(part_name, config, datasource_name))
		self._part_final = DataSplitter.create_instance(part_name_list[-1], config, datasource_name)

	def split_partitions(self, block_iter, entry_first=0):
		for block in block_iter:
			for sub_block in reduce(lambda x, y: y.divide_blocks(x), self._part_list, [block]):
				for partition in self._part_final.split_partitions([sub_block]):
					yield partition


class FileBoundarySplitter(FileLevelSplitter):
	# Split dataset along block boundaries into jobs with 'files per job' files
	alias_list = ['files']

	def __init__(self, config, datasource_name):
		FileLevelSplitter.__init__(self, config, datasource_name)
		self._files_per_job = config.get_lookup(self._get_part_opt('files per job'),
			parser=int, strfun=int.__str__)

	def divide_blocks(self, block_iter):
		for block in block_iter:
			fi_idx_start = 0
			files_per_job = self._files_per_job.lookup(DataProvider.get_block_id(block))
			if files_per_job <= 0:
				raise PartitionError('Invalid number of files per job: %d' % files_per_job)
			while fi_idx_start < len(block[DataProvider.FileList]):
				fi_list = block[DataProvider.FileList][fi_idx_start:fi_idx_start + files_per_job]
				fi_idx_start += files_per_job
				if fi_list:
					yield self._create_sub_block(block, fi_list)


class HybridSplitter(FileLevelSplitter):
	# Split dataset along block and file boundaries into jobs with (mostly<=) 'events per job' events
	# If file has #events > 'events per job', use just the single file (=> job has more events!)
	alias_list = ['hybrid']

	def __init__(self, config, datasource_name):
		FileLevelSplitter.__init__(self, config, datasource_name)
		self._entries_per_job = config.get_lookup(
			self._get_part_opt(['events per job', 'entries per job']), parser=int, strfun=int.__str__)

	def divide_blocks(self, block_iter):
		for block in block_iter:
			(entries, fi_list) = (0, [])
			entries_per_job = self._entries_per_job.lookup(DataProvider.get_block_id(block))
			if entries_per_job <= 0:
				raise PartitionError('Invalid number of entries per job: %d' % entries_per_job)
			for fi in block[DataProvider.FileList]:
				if fi_list and (entries + fi[DataProvider.NEntries] > entries_per_job):
					yield self._create_sub_block(block, fi_list)
					(entries, fi_list) = (0, [])
				fi_list.append(fi)
				entries += fi[DataProvider.NEntries]
			if fi_list:
				yield self._create_sub_block(block, fi_list)
