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

import os, time
from grid_control.config import TriggerResync
from grid_control.datasets import DataProvider, DataSplitter, DatasetError, PartitionProcessor
from grid_control.gc_exceptions import UserError
from grid_control.parameters.psource_base import LimitedResyncParameterSource, NullParameterSource
from grid_control.utils import ensure_dir_exists, rename_file
from grid_control.utils.activity import Activity, ProgressActivity
from grid_control.utils.parsing import str_time_long
from python_compat import md5_hex, set


class BaseDataParameterSource(LimitedResyncParameterSource):
	def __init__(self, config, datasource_name, repository, reader=None):
		LimitedResyncParameterSource.__init__(self)
		# needed for backwards compatible file names: datacache/datamap
		self._name = datasource_name.replace('dataset', 'data')
		(self._reader, self._len) = (None, None)
		self._set_reader(reader)
		self._part_proc = config.get_composited_plugin(
			['partition processor', '%s partition processor' % datasource_name],
			'TFCPartitionProcessor LocationPartitionProcessor ' +
			'MetaPartitionProcessor BasicPartitionProcessor',
			'MultiPartitionProcessor', cls=PartitionProcessor, on_change=TriggerResync(['parameters']),
			pargs=(datasource_name,))
		self._log.debug('%s: Using partition processor %s', datasource_name, repr(self._part_proc))
		repository['dataset:%s' % self._name] = self

	def __repr__(self):
		if self._name == 'data':
			return 'data()'
		return 'data(%s)' % self._name

	def create_psrc(cls, pconfig, repository, src='data'):  # pylint:disable=arguments-differ
		src_key = 'dataset:%s' % src
		if src_key not in repository:
			raise UserError('Dataset parameter source "%s" not setup!' % src)
		return repository[src_key]
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		partition = self._reader.get_partition_checked(pnum)
		self._part_proc.process(pnum, partition, result)

	def fill_parameter_metadata(self, result):
		result.extend(self._part_proc.get_partition_metadata() or [])

	def get_datasource_name(self):
		return self._name

	def get_parameter_len(self):
		return self._len

	def get_psrc_hash(self):
		return md5_hex(repr([self._name, self._len]))

	def show_psrc(self):
		return ['%s: src = %s' % (self.__class__.__name__, self._name)]

	def _set_reader(self, reader):
		(self._reader, self._len) = (reader, None)
		if reader is not None:
			self._len = reader.get_partition_len()


class DataParameterSource(BaseDataParameterSource):
	alias_list = ['data']

	def __new__(cls, config, datasource_name, repository, keep_old=True):
		provider_name_default = config.get(
			['default provider', '%s provider' % datasource_name], 'ListProvider')
		provider = config.get_composited_plugin(datasource_name, '', ':ThreadedMultiDatasetProvider:',
			cls=DataProvider, require_plugin=False, on_change=TriggerResync(['datasets', 'parameters']),
			bind_kwargs={'datasource_name': datasource_name, 'provider_name_default': provider_name_default})
		if not provider:
			return NullParameterSource()
		instance = BaseDataParameterSource.__new__(cls)
		instance.provider = provider
		return instance

	def __init__(self, config, datasource_name, repository, keep_old=True):
		BaseDataParameterSource.__init__(self, config, datasource_name, repository)

		# hide provider property set by __new__
		self._provider = self.provider
		del self.provider

		if self._provider.need_init_query():
			self._provider.get_block_list_cached(show_stats=False)

		data_src_text = 'Dataset source %r' % datasource_name
		# Select dataset refresh rate
		data_refresh = config.get_time('%s refresh' % datasource_name, -1, on_change=None)
		if data_refresh >= 0:
			data_refresh = max(data_refresh, self._provider.get_query_interval())
			self._log.info('%s will be queried every %s', data_src_text, str_time_long(data_refresh))
		self.setup_resync(interval=data_refresh, force=config.get_state('resync', detail='datasets'))

		splitter_name = config.get('%s splitter' % datasource_name, 'FileBoundarySplitter')
		splitter_cls = self._provider.check_splitter(DataSplitter.get_class(splitter_name))
		self._splitter = splitter_cls(config, datasource_name)

		# Settings:
		(self._dn, self._keep_old) = (config.get_work_path(), keep_old)
		ensure_dir_exists(self._dn, 'partition map directory', DatasetError)
		self._set_reader(self._init_reader())

		if not self.get_parameter_len():
			if data_refresh < 0:
				raise UserError('%s does not provide jobs to process' % data_src_text)
			self._log.warning('%s does not provide jobs to process', data_src_text)

	def can_finish(self):
		return self._resync_interval < 0

	def get_needed_dataset_keys(self):
		return self._part_proc.get_needed_vn_list(self._splitter) or []

	def _exists_data_path(self, postfix):
		return os.path.exists(os.path.join(self._dn, self.get_datasource_name() + postfix))

	def _get_data_path(self, postfix):
		return os.path.join(self._dn, self.get_datasource_name() + postfix)

	def _init_reader(self):
		# look for aborted inits / resyncs - and try to restore old state if possible
		if self._exists_data_path('map.tar.resync') and self._exists_data_path('cache.dat.resync'):
			rename_file(self._get_data_path('cache.dat.resync'), self._get_data_path('cache.dat'))
			rename_file(self._get_data_path('map.tar.resync'), self._get_data_path('map.tar'))
		elif self._exists_data_path('map.tar.resync') or self._exists_data_path('cache.dat.resync'):
			raise DatasetError('Found broken dataset partition resync state in work directory')

		if self._exists_data_path('map.tar') and not self._exists_data_path('cache.dat'):
			raise DatasetError('Found broken dataset partition in work directory')
		elif not self._exists_data_path('map.tar'):
			# create initial partition map file
			if not self._exists_data_path('cache.dat'):
				provider = self._provider
			else:
				provider = DataProvider.load_from_file(self._get_data_path('cache.dat'))
			block_iter = DataProvider.save_to_file_iter(self._get_data_path('cache.dat.init'),
				provider.get_block_list_cached(show_stats=True))
			partition_iter = self._splitter.split_partitions(block_iter)
			DataSplitter.save_partitions(self._get_data_path('map.tar.init'), partition_iter)
			rename_file(self._get_data_path('cache.dat.init'), self._get_data_path('cache.dat'))
			rename_file(self._get_data_path('map.tar.init'), self._get_data_path('map.tar'))
		return DataSplitter.load_partitions(self._get_data_path('map.tar'))

	def _resync_partitions(self, path, block_list_old, block_list_new):
		partition_resync_handler = self._splitter.get_resync_handler()
		progress = ProgressActivity(progress_max=self.get_parameter_len(),
			msg='Writing resyncronized dataset partitions (progress is estimated)')
		path_tmp = path + '.tmp'
		try:
			resync_result = partition_resync_handler.resync(self._splitter,
				self._reader, block_list_old, block_list_new)
			DataSplitter.save_partitions(path_tmp, resync_result.partition_iter, progress)
		except Exception:
			raise DatasetError('Unable to resync %r' % self.get_datasource_name())
		os.rename(path_tmp, path)
		return (resync_result.pnum_list_redo, resync_result.pnum_list_disable)

	def _resync_psrc(self):
		activity = Activity('Performing resync of datasource %r' % self.get_datasource_name())
		# Get old and new dataset information
		provider_old = DataProvider.load_from_file(self._get_data_path('cache.dat'))
		block_list_old = provider_old.get_block_list_cached(show_stats=False)
		self._provider.clear_cache()
		block_list_new = self._provider.get_block_list_cached(show_stats=False)
		self._provider.save_to_file(self._get_data_path('cache-new.dat'), block_list_new)

		# Use old splitting information to synchronize with new dataset infos
		partition_len_old = self.get_parameter_len()
		partition_changes = self._resync_partitions(
			self._get_data_path('map-new.tar'), block_list_old, block_list_new)
		activity.finish()
		if partition_changes is not None:
			# Move current splitting to backup and use the new splitting from now on
			def _rename_with_backup(new, cur, old):
				if self._keep_old:
					os.rename(self._get_data_path(cur), self._get_data_path(old))
				os.rename(self._get_data_path(new), self._get_data_path(cur))
			_rename_with_backup('map-new.tar', 'map.tar', 'map-old-%d.tar' % time.time())
			_rename_with_backup('cache-new.dat', 'cache.dat', 'cache-old-%d.dat' % time.time())
			self._set_reader(DataSplitter.load_partitions(self._get_data_path('map.tar')))
			self._log.debug('Dataset resync finished: %d -> %d partitions', partition_len_old, self._len)
			(pnum_list_redo, pnum_list_disable) = partition_changes
			return (set(pnum_list_redo), set(pnum_list_disable), partition_len_old != self._len)
