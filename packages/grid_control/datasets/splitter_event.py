# | Copyright 2009-2017 Karlsruhe Institute of Technology
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
	alias_list = ['events']

	def __init__(self, config, datasource_name):
		DataSplitter.__init__(self, config, datasource_name)
		self._entries_per_job = config.get_lookup(
			self._get_part_opt(['events per job', 'entries per job']), parser=int, strfun=int.__str__)

	def get_needed_enums(cls):
		return [DataSplitter.FileList, DataSplitter.Skipped, DataSplitter.NEntries]
	get_needed_enums = classmethod(get_needed_enums)

	def split_partitions(self, block_iter, entry_first=0):
		for block in block_iter:
			entries_per_job = self._entries_per_job.lookup(DataProvider.get_block_id(block))
			for proto_partition in self._partition_block(block[DataProvider.FileList],
					entries_per_job, entry_first):
				entry_first = 0
				yield self._finish_partition(block, proto_partition)

	def _partition_block(self, fi_list, events_per_job, entry_first):
		event_next = entry_first
		event_succ = event_next + events_per_job
		event_current = 0
		event_prev = 0
		skip_current = 0
		fi_iter = iter(fi_list)
		proto_partition = {DataSplitter.Skipped: 0, DataSplitter.NEntries: 0, DataSplitter.FileList: []}
		while True:
			if event_current >= event_prev:
				fi = next(fi_iter, None)
				if fi is None:
					if proto_partition[DataSplitter.FileList]:
						yield proto_partition
					break

				event_count = fi[DataProvider.NEntries]
				if event_count < 0:
					raise DatasetError('%s does not support files with a negative number of events!' %
						self.__class__.__name__)
				event_current = event_prev
				event_prev = event_current + event_count
				skip_current = 0

			if event_next >= event_prev:
				event_current = event_prev
				continue

			skip_current += event_next - event_current
			event_current = event_next

			available = event_prev - event_current
			if event_succ - event_next < available:
				available = event_succ - event_next

			if not proto_partition[DataSplitter.FileList]:
				proto_partition[DataSplitter.Skipped] = skip_current

			proto_partition[DataSplitter.NEntries] += available
			event_next += available

			proto_partition[DataSplitter.FileList].append(fi[DataProvider.URL])
			if DataProvider.Metadata in fi:
				proto_partition.setdefault(DataSplitter.Metadata, []).append(fi[DataProvider.Metadata])

			if event_next >= event_succ:
				event_succ += events_per_job
				yield proto_partition
				proto_partition = {DataSplitter.Skipped: 0, DataSplitter.NEntries: 0, DataSplitter.FileList: []}
