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

from grid_control.config import join_config_locations
from grid_control.datasets.provider_base import DataProvider
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.data_structures import make_enum
from grid_control.utils.thread_tools import GCLock, with_lock
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import imap, irange, itemgetter, lmap


class PartitionError(NestedException):
	pass


class DataSplitter(ConfigurablePlugin):
	def __init__(self, config, datasource_name, resync_handler_name='DefaultPartitionResyncHandler'):
		ConfigurablePlugin.__init__(self, config)
		self._datasource_name = datasource_name
		self._partition_source = None
		self._resync_handler = PartitionResyncHandler.create_instance(resync_handler_name, config)

		self._dp_ds_prop_list = []
		for prop in ['Dataset', 'BlockName', 'Nickname', 'Locations']:
			self._dp_ds_prop_list.append((getattr(DataProvider, prop), getattr(DataSplitter, prop)))

	def get_needed_enums(cls):
		return [DataSplitter.FileList]
	get_needed_enums = classmethod(get_needed_enums)

	def get_resync_handler(self):
		return self._resync_handler

	def load_partitions(cls, path, reader_name='auto'):
		return PartitionReader.create_instance(reader_name, path)
	load_partitions = classmethod(load_partitions)

	def save_partitions(cls, path, partition_iter, progress=None, writer_name='auto'):
		writer = PartitionWriter.create_instance(writer_name)
		return writer.save_partitions(path, partition_iter, progress)
	save_partitions = classmethod(save_partitions)

	def split_partitions(self, block_iter, entry_first=0):
		raise AbstractError

	def _finish_partition(self, block, partition, fi_list=None):
		# Copy infos from block
		for (dp_prop, ds_prop) in self._dp_ds_prop_list:
			if dp_prop in block:
				partition[ds_prop] = block[dp_prop]
		if DataProvider.Metadata in block:
			partition[DataSplitter.MetadataHeader] = block[DataProvider.Metadata]
		# Helper for very simple splitter
		if fi_list:
			partition[DataSplitter.FileList] = lmap(itemgetter(DataProvider.URL), fi_list)
			partition[DataSplitter.NEntries] = sum(imap(itemgetter(DataProvider.NEntries), fi_list))
			if DataProvider.Metadata in block:
				partition[DataSplitter.Metadata] = lmap(itemgetter(DataProvider.Metadata), fi_list)
		return partition

	def _get_part_opt(self, *args):
		return join_config_locations(['', self._datasource_name], *args)

make_enum(['Dataset', 'Locations', 'NEntries', 'Skipped',
	'FileList', 'Nickname', 'DatasetID',  # DatasetID is legacy
	'CommonPrefix', 'Invalid', 'BlockName', 'MetadataHeader',
	'Metadata', 'Comment'], DataSplitter, use_hash=False)


class PartitionResyncHandler(ConfigurablePlugin):
	def resync(self, splitter, reader, block_list_old, block_list_new):
		raise AbstractError


class PartitionReader(Plugin):
	def __init__(self, partition_len):
		self._lock = GCLock()
		self._partition_len = partition_len

	def get_partition_checked(self, partition_num):
		if partition_num >= self._partition_len:
			raise PartitionError('%s is out of range for available partitions' % repr(partition_num))
		return with_lock(self._lock, self.get_partition_unchecked, partition_num)

	def get_partition_len(self):
		return self._partition_len

	def get_partition_unchecked(self, partition_num):
		raise AbstractError

	def iter_partitions(self):
		for partition_num in irange(self._partition_len):
			yield self.get_partition_checked(partition_num)


class PartitionWriter(Plugin):
	def save_partitions(self, path, partition_iter, progress=None):
		raise AbstractError
