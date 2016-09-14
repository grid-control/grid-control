# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.datasets.splitter_base import DataSplitter
from python_compat import next

class EventBoundarySplitter(DataSplitter):
	alias = ['events']

	def get_needed_enums(cls):
		return [DataSplitter.FileList, DataSplitter.Skipped, DataSplitter.NEntries]
	get_needed_enums = classmethod(get_needed_enums)


	def _partition_block(self, fi_list, events_per_job, event_first):
		nextEvent = event_first
		succEvent = nextEvent + events_per_job
		curEvent = 0
		lastEvent = 0
		curSkip = 0
		fi_listIter = iter(fi_list)
		proto_partition = {DataSplitter.Skipped: 0, DataSplitter.NEntries: 0, DataSplitter.FileList: []}
		while True:
			if curEvent >= lastEvent:
				fileObj = next(fi_listIter, None)
				if fileObj is None:
					if proto_partition[DataSplitter.FileList]:
						yield proto_partition
					break

				nEvents = fileObj[DataProvider.NEntries]
				if nEvents < 0:
					raise DatasetError('EventBoundarySplitter does not support files with a negative number of events!')
				curEvent = lastEvent
				lastEvent = curEvent + nEvents
				curSkip = 0

			if nextEvent >= lastEvent:
				curEvent = lastEvent
				continue

			curSkip += nextEvent - curEvent
			curEvent = nextEvent

			available = lastEvent - curEvent
			if succEvent - nextEvent < available:
				available = succEvent - nextEvent

			if not proto_partition[DataSplitter.FileList]:
				proto_partition[DataSplitter.Skipped] = curSkip

			proto_partition[DataSplitter.NEntries] += available
			nextEvent += available

			proto_partition[DataSplitter.FileList].append(fileObj[DataProvider.URL])
			if DataProvider.Metadata in fileObj:
				proto_partition.setdefault(DataSplitter.Metadata, []).append(fileObj[DataProvider.Metadata])

			if nextEvent >= succEvent:
				succEvent += events_per_job
				yield proto_partition
				proto_partition = {DataSplitter.Skipped: 0, DataSplitter.NEntries: 0, DataSplitter.FileList: []}


	def _configure_splitter(self, config):
		self._events_per_job = self._query_config(config.getInt, 'events per job')


	def _partition_blocks(self, block_iter, event_first = 0):
		for block in block_iter:
			for proto_partition in self._partition_block(block[DataProvider.FileList], self._setup(self._events_per_job, block), event_first):
				event_first = 0
				yield self._finish_partition(block, proto_partition)
