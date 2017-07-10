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

import os, copy, logging
from grid_control.config import TriggerResync, create_config
from grid_control.datasets.dproc_base import DataProcessor, NullDataProcessor
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import abort, ensure_dir_exists
from grid_control.utils.activity import Activity
from grid_control.utils.algos import get_list_difference, split_list
from grid_control.utils.data_structures import make_enum
from grid_control.utils.file_tools import SafeFile, erase_content, with_file_iter
from hpfwk import AbstractError, InstanceFactory, NestedException
from python_compat import StringBuffer, identity, ifilter, imap, irange, itemgetter, json, lmap, lrange, md5_hex, set, sort_inplace  # pylint:disable=line-too-long


class DatasetError(NestedException):
	pass


class DatasetRetrievalError(DatasetError):
	pass


class DataProvider(ConfigurablePlugin):
	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('%s.provider' % datasource_name)
		(self._datasource_name, self._dataset_expr) = (datasource_name, dataset_expr)
		self._dataset_nick_override = dataset_nick
		(self._cache_block, self._cache_dataset) = (None, None)
		self._dataset_query_interval = config.get_time(
			'%s default query interval' % datasource_name, 60, on_change=None)

		self._stats = dataset_proc or DataProcessor.create_instance('SimpleStatsDataProcessor',
			config, datasource_name, self._log,
			' * Dataset %s:\n\tcontains ' % repr(dataset_nick or dataset_expr))

		dataset_config = config.change_view(default_on_change=TriggerResync(['datasets', 'parameters']))
		self._nick_producer = dataset_config.get_plugin(
			['nickname source', '%s nickname source' % datasource_name], 'SimpleNickNameProducer',
			cls=DataProcessor, pargs=(datasource_name,))
		self._dataset_processor = dataset_proc or dataset_config.get_composited_plugin(
			'%s processor' % datasource_name,
			'NickNameConsistencyProcessor EntriesConsistencyDataProcessor URLDataProcessor ' +
			'URLCountDataProcessor EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor ' +
			'LocationDataProcessor', 'MultiDataProcessor', cls=DataProcessor, pargs=(datasource_name,))

	def bind(cls, value, **kwargs):
		instance_arg_list = list(cls.parse_bind_args(value, **kwargs))
		for (instance_idx, instance_arg) in enumerate(instance_arg_list):
			if len(instance_arg_list) > 1:
				(bind_value, provider, config, datasource_name, dataset_expr, nickname) = instance_arg
				yield InstanceFactory(bind_value, provider, config,
					datasource_name + ' provider %d' % (instance_idx + 1), dataset_expr, nickname)
			else:
				yield InstanceFactory(*instance_arg)
	bind = classmethod(bind)

	def check_splitter(self, splitter):
		# Check if splitter is valid
		return splitter

	def clear_cache(self):
		self._cache_block = None
		self._cache_dataset = None

	def disable_stream_singletons(self):
		self._dataset_processor.disable_stream_singletons()
		if not self._dataset_processor.enabled():
			self._dataset_processor = NullDataProcessor()

	def get_block_id(cls, block):
		if block.get(DataProvider.BlockName, '') in ['', '0']:
			return block[DataProvider.Dataset]
		return block[DataProvider.Dataset] + '#' + block[DataProvider.BlockName]
	get_block_id = classmethod(get_block_id)

	def get_block_list_cached(self, show_stats):
		return self._create_block_cache(show_stats, self.iter_blocks_normed)

	def get_dataset_name_list(self):
		# Default implementation via get_block_list_cached
		if self._cache_dataset is None:
			self._cache_dataset = set()
			for block in self.get_block_list_cached(show_stats=True):
				self._cache_dataset.add(block[DataProvider.Dataset])
				if abort():
					raise DatasetError('Received abort request during dataset name retrieval!')
		return list(self._cache_dataset)

	def get_query_interval(self):
		# Define how often the dataprovider can be queried automatically
		return self._dataset_query_interval

	def iter_blocks_from_expr(cls, config, dataset_expr, dataset_proc=None):
		for dp_factory in DataProvider.bind(dataset_expr, config=config):
			dproc = dp_factory.create_instance_bound(dataset_proc=dataset_proc)
			for block in dproc.iter_blocks_normed():
				yield block
	iter_blocks_from_expr = classmethod(iter_blocks_from_expr)

	def iter_blocks_normed(self):
		activity = Activity('Retrieving %s' % self._dataset_expr)
		try:
			# Validation, Naming:
			for block in self._iter_blocks_raw():
				if not block.get(DataProvider.Dataset):
					raise DatasetError('Block does not contain the dataset name!')
				block.setdefault(DataProvider.BlockName, '0')
				block.setdefault(DataProvider.Provider, self.__class__.__name__)
				block.setdefault(DataProvider.Query, self._dataset_expr)
				block.setdefault(DataProvider.Locations, None)
				events = sum(imap(itemgetter(DataProvider.NEntries), block[DataProvider.FileList]))
				block.setdefault(DataProvider.NEntries, events)
				if self._dataset_nick_override:
					block[DataProvider.Nickname] = self._dataset_nick_override
				elif self._nick_producer:
					block = self._nick_producer.process_block(block)
					if not block:
						raise DatasetError('Nickname producer failed!')
				yield block
		except Exception:
			raise DatasetRetrievalError('Unable to retrieve dataset %s' % repr(self._dataset_expr))
		activity.finish()

	def load_from_file(path):
		# Load dataset information using ListProvider
		return DataProvider.create_instance('ListProvider', create_config(load_old_config=False,
			config_dict={'dataset': {'dataset processor': 'NullDataProcessor'}}), 'dataset', path)
	load_from_file = staticmethod(load_from_file)

	def need_init_query(self):
		return self._dataset_processor.must_complete_for_partition()

	def parse_bind_args(cls, value, **kwargs):
		config = kwargs.pop('config')
		datasource_name = kwargs.pop('datasource_name', 'dataset')
		provider_name_default = kwargs.pop('provider_name_default', 'ListProvider')

		for entry in ifilter(str.strip, value.splitlines()):
			(nickname, provider_name, dataset_expr) = ('', provider_name_default, None)
			tmp = lmap(str.strip, entry.split(':', 2))
			if len(tmp) == 3:  # use tmp[...] to avoid false positives for unpacking checker ...
				(nickname, provider_name, dataset_expr) = (tmp[0], tmp[1], tmp[2])
				if dataset_expr.startswith('/'):
					dataset_expr = '/' + dataset_expr.lstrip('/')
			elif len(tmp) == 2:
				(nickname, dataset_expr) = (tmp[0], tmp[1])
			elif len(tmp) == 1:
				dataset_expr = tmp[0]

			provider = cls.get_class(provider_name)
			bind_value = str.join(':', [nickname, provider.get_bind_class_name(provider_name), dataset_expr])
			yield (bind_value, provider, config, datasource_name, dataset_expr, nickname)
	parse_bind_args = classmethod(parse_bind_args)

	def parse_block_id(cls, block_id_str):
		block_id_parts = block_id_str.split('#', 1)
		if len(block_id_parts) == 2:
			return {DataProvider.Dataset: block_id_parts[0], DataProvider.BlockName: block_id_parts[1]}
		elif len(block_id_parts) == 1:
			return {DataProvider.Dataset: block_id_parts[0]}
		raise DatasetError('Invalid block ID: %r' % block_id_str)
	parse_block_id = classmethod(parse_block_id)

	def resync_blocks(block_list_old, block_list_new):
		# Returns changes between two sets of blocks in terms of added, missing and changed blocks
		# Only the affected files are returned in the block file list
		def _get_block_key(block):  # Compare different blocks according to their name - NOT full content
			return (block[DataProvider.Dataset], block[DataProvider.BlockName])
		sort_inplace(block_list_old, key=_get_block_key)
		sort_inplace(block_list_new, key=_get_block_key)

		def _handle_matching_block(block_list_added, block_list_missing, block_list_matching,
				block_old, block_new):
			# Compare different files according to their name - NOT full content
			get_file_key = itemgetter(DataProvider.URL)
			sort_inplace(block_old[DataProvider.FileList], key=get_file_key)
			sort_inplace(block_new[DataProvider.FileList], key=get_file_key)

			def _handle_matching_fi(fi_list_added, fi_list_missing, fi_list_matched, fi_old, fi_new):
				fi_list_matched.append((fi_old, fi_new))

			(fi_list_added, fi_list_missing, fi_list_matched) = get_list_difference(
				block_old[DataProvider.FileList], block_new[DataProvider.FileList],
				get_file_key, _handle_matching_fi, is_sorted=True)
			if fi_list_added:  # Create new block for added files in an existing block
				block_added = copy.copy(block_new)
				block_added[DataProvider.FileList] = fi_list_added
				block_added[DataProvider.NEntries] = sum(imap(itemgetter(DataProvider.NEntries), fi_list_added))
				block_list_added.append(block_added)
			block_list_matching.append((block_old, block_new, fi_list_missing, fi_list_matched))

		return get_list_difference(block_list_old, block_list_new,
			_get_block_key, _handle_matching_block, is_sorted=True)
	resync_blocks = staticmethod(resync_blocks)

	def save_to_file(path, block_iter, strip_metadata=False):
		for _ in DataProvider.save_to_file_iter(path, block_iter, strip_metadata):
			pass
	save_to_file = staticmethod(save_to_file)

	def save_to_file_iter(path, block_iter, strip_metadata=False):
		# Save dataset information in 'ini'-style => 10x faster to r/w than cPickle
		if os.path.dirname(path):
			ensure_dir_exists(os.path.dirname(path), 'dataset cache directory')
		return with_file_iter(SafeFile(path, 'w'),
			lambda fp: DataProvider.save_to_stream(fp, block_iter, strip_metadata))
	save_to_file_iter = staticmethod(save_to_file_iter)

	def save_to_stream(stream, block_iter, strip_metadata=False):
		writer = StringBuffer()
		write_separator = False
		for block in block_iter:
			if write_separator:
				writer.write('\n')
			writer.write('[%s]\n' % DataProvider.get_block_id(block))
			if DataProvider.Nickname in block:
				writer.write('nickname = %s\n' % block[DataProvider.Nickname])
			if DataProvider.NEntries in block:
				writer.write('events = %d\n' % block[DataProvider.NEntries])
			if block.get(DataProvider.Locations) is not None:
				writer.write('se list = %s\n' % str.join(',', block[DataProvider.Locations]))
			common_prefix = os.path.commonprefix(lmap(itemgetter(DataProvider.URL),
				block[DataProvider.FileList]))
			common_prefix = str.join('/', common_prefix.split('/')[:-1])
			if len(common_prefix) > 6:
				def _formatter(value):
					return value.replace(common_prefix + '/', '')
				writer.write('prefix = %s\n' % common_prefix)
			else:
				_formatter = identity

			do_write_metadata = (DataProvider.Metadata in block) and not strip_metadata
			if do_write_metadata:
				def _get_metadata_str(fi, idx_list):
					idx_list = ifilter(lambda idx: idx < len(fi[DataProvider.Metadata]), idx_list)
					return json.dumps(lmap(lambda idx: fi[DataProvider.Metadata][idx], idx_list))
				(metadata_idx_list_block, metadata_idx_list_file) = _split_metadata_idx_list(block)
				metadata_header_str = json.dumps(lmap(lambda idx: block[DataProvider.Metadata][idx],
					metadata_idx_list_block + metadata_idx_list_file))
				writer.write('metadata = %s\n' % metadata_header_str)
				if metadata_idx_list_block:
					metadata_str = _get_metadata_str(block[DataProvider.FileList][0], metadata_idx_list_block)
					writer.write('metadata common = %s\n' % metadata_str)
			for fi in block[DataProvider.FileList]:
				writer.write('%s = %d' % (_formatter(fi[DataProvider.URL]), fi[DataProvider.NEntries]))
				if do_write_metadata and metadata_idx_list_file:
					writer.write(' %s' % _get_metadata_str(fi, metadata_idx_list_file))
				writer.write('\n')
			stream.write(writer.getvalue())
			erase_content(writer)
			write_separator = True
			yield block
		writer.close()
	save_to_stream = staticmethod(save_to_stream)

	def _create_block_cache(self, show_stats, iter_fun):
		def _iter_blocks():
			for block in iter_fun():
				yield block
				self._raise_on_abort()
		# Cached access to list of block dicts, does also the validation checks
		if self._cache_block is None:
			try:
				block_iter_processed = self._dataset_processor.process(_iter_blocks())
				if show_stats:
					block_iter_processed = self._stats.process(block_iter_processed)
				self._cache_block = list(block_iter_processed)
			except DatasetRetrievalError:  # skip dataset processing pipeline error message
				raise
			except Exception:
				raise DatasetError('Unable to run dataset %s ' % repr(self._dataset_expr) +
					'through processing pipeline!')
		self._raise_on_abort()
		return self._cache_block

	def _get_dataset_hash(self):
		buffer = StringBuffer()
		for _ in DataProvider.save_to_stream(buffer, self.iter_blocks_normed()):
			pass
		value = buffer.getvalue()
		buffer.close()
		return md5_hex(value)

	def _iter_blocks_raw(self):
		# List of (partial or complete) block dicts with format
		# { NEntries: 123, Dataset: '/path/to/data', Block: 'abcd-1234', Locations: ['site1','site2'],
		#   Filelist: [{URL: '/path/to/file1', NEntries: 100}, {URL: '/path/to/file2', NEntries: 23}]}
		raise AbstractError

	def _raise_on_abort(self):
		if abort():
			raise DatasetError('Received abort request during dataset retrieval')

make_enum(  # To uncover errors, the enums of DataProvider / DataSplitter do *NOT* match type wise
	['NEntries', 'BlockName', 'Dataset', 'Locations', 'URL', 'FileList',
	'Nickname', 'Metadata', 'Provider', 'Query'], DataProvider)


def _split_metadata_idx_list(block):
	def _get_metadata_hash(fi, idx):
		if idx < len(fi[DataProvider.Metadata]):
			return md5_hex(repr(fi[DataProvider.Metadata][idx]))
	fi_list = block[DataProvider.FileList]
	common_metadata_idx_list = lrange(len(block[DataProvider.Metadata]))
	if fi_list:
		common_metadata_hash_list = lmap(lambda idx: _get_metadata_hash(fi_list[0], idx),
			common_metadata_idx_list)
	for fi in fi_list:  # Identify common metadata
		for idx in common_metadata_idx_list:
			if _get_metadata_hash(fi, idx) != common_metadata_hash_list[idx]:
				common_metadata_idx_list.remove(idx)
	return split_list(irange(len(block[DataProvider.Metadata])),
		fun=common_metadata_idx_list.__contains__,
		sort_key=lambda idx: block[DataProvider.Metadata][idx])
