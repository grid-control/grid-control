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
from python_compat import imap, reduce

# Base class for (stackable) splitters with file level granularity
class FileLevelSplitter(DataSplitter):
	def _proto_partition_blocks(self, blocks):
		raise AbstractError

	def _create_partition(self, old, filelist):
		new = dict(old)
		new[DataProvider.FileList] = filelist
		new[DataProvider.NEntries] = sum(imap(lambda x: x[DataProvider.NEntries], filelist))
		return new

	def _partition_blocks(self, blocks, firstEvent = 0):
		for proto_partition in self._proto_partition_blocks(blocks):
			yield self._finish_partition(proto_partition, dict(), proto_partition[DataProvider.FileList])


class FLSplitStacker(FileLevelSplitter):
	alias = ['pipeline']

	def _configure_splitter(self, config):
		self._config = config
		self._splitstack = self._query_config(config.getList, 'splitter stack', ['BlockBoundarySplitter'])

	def _partition_blocks(self, blocks, firstEvent = 0):
		for block in blocks:
			splitterList = self._setup(self._splitstack, block)
			subSplitter = imap(lambda x: FileLevelSplitter.createInstance(x, self._config, self._datasource_name), splitterList[:-1])
			endSplitter = DataSplitter.createInstance(splitterList[-1], self._config, self._datasource_name)
			for subBlock in reduce(lambda x, y: y._proto_partition_blocks(x), subSplitter, [block]):
				for splitting in endSplitter._partition_blocks([subBlock]):
					yield splitting


# Split only along block boundaries
class BlockBoundarySplitter(FileLevelSplitter):
	alias = ['blocks']

	def _proto_partition_blocks(self, blocks):
		return blocks


# Split dataset along block boundaries into jobs with 'files per job' files
class FileBoundarySplitter(FileLevelSplitter):
	alias = ['files']

	def _configure_splitter(self, config):
		self._files_per_job = self._query_config(config.getInt, 'files per job')

	def _proto_partition_blocks(self, blocks):
		for block in blocks:
			start = 0
			filesPerJob = self._setup(self._files_per_job, block)
			while start < len(block[DataProvider.FileList]):
				files = block[DataProvider.FileList][start : start + filesPerJob]
				start += filesPerJob
				yield self._create_partition(block, files)


# Split dataset along block and file boundaries into jobs with (mostly <=) 'events per job' events
# In case of file with #events > 'events per job', use just the single file (=> job has more events!)
class HybridSplitter(FileLevelSplitter):
	alias = ['hybrid']

	def _configure_splitter(self, config):
		self._events_per_job = self._query_config(config.getInt, 'events per job')

	def _proto_partition_blocks(self, blocks):
		for block in blocks:
			(events, fileStack) = (0, [])
			eventsPerJob = self._setup(self._events_per_job, block)
			for fileInfo in block[DataProvider.FileList]:
				if (len(fileStack) > 0) and (events + fileInfo[DataProvider.NEntries] > eventsPerJob):
					yield self._create_partition(block, fileStack)
					(events, fileStack) = (0, [])
				fileStack.append(fileInfo)
				events += fileInfo[DataProvider.NEntries]
			yield self._create_partition(block, fileStack)
